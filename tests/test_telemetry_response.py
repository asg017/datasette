"""
Tests for spans and metrics covering the response-building half of a request:
template rendering, facet calculation and CSV streaming.

Before these, a trace of a slow page whose queries were all fast showed a
handful of quick `db.query` spans and then an unexplained gap.
"""

import pytest

pytest.importorskip("opentelemetry.sdk")

from datasette.app import Datasette


def spans_named(otel_spans, name):
    return [span for span in otel_spans.get_finished_spans() if span.name == name]


@pytest.mark.asyncio
async def test_render_template_span(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = spans_named(otel_spans, "datasette.render_template")
    assert spans, "an HTML page must produce a render_template span"
    span = spans[-1]
    assert span.attributes["datasette.template"].endswith(".html")
    assert span.attributes["datasette.view_name"] == "table"


@pytest.mark.asyncio
async def test_render_template_span_covers_the_hooks_it_awaits(otel_spans):
    """
    The span must start before the template context is built, not at
    render_async(). extra_body_script is awaited during context building, so
    its hook span has to fall *inside* the render_template span.
    """
    from datasette import hookimpl
    from datasette.plugins import pm

    class SlowScriptPlugin:
        __name__ = "slow-script-plugin"

        @hookimpl
        def extra_body_script(self):
            async def inner():
                import asyncio

                await asyncio.sleep(0.05)
                return ""

            return inner()

    pm.register(SlowScriptPlugin(), name="slow-script-plugin")
    try:
        ds = Datasette(memory=True)
        await ds.invoke_startup()
        otel_spans.clear()
        response = await ds.client.get("/_memory")
        assert response.status_code == 200

        render = spans_named(otel_spans, "datasette.render_template")[-1]
        hook_spans = [
            span
            for span in otel_spans.get_finished_spans()
            if span.name == "datasette.hook.extra_body_script"
        ]
        assert hook_spans, "expected the slow plugin's hook span"
        hook = hook_spans[-1]
        assert hook.start_time >= render.start_time
        assert hook.end_time <= render.end_time
        # And the render span must actually include that 50ms of awaited work.
        assert (render.end_time - render.start_time) >= 50_000_000
        ds.close()
    finally:
        pm.unregister(name="slow-script-plugin")


@pytest.mark.asyncio
async def test_template_render_duration_metric(ds_client, otel_metrics):
    response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200
    otel_metrics.collect()
    points = otel_metrics.points("datasette.template.render.duration")
    assert points
    templates = {dict(point.attributes)["datasette.template"] for point in points}
    assert any(name.endswith(".html") for name in templates)
    assert "unknown" not in templates


@pytest.mark.asyncio
async def test_facet_results_span(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/facetable.json?_facet=state")
    assert response.status_code == 200

    spans = spans_named(otel_spans, "datasette.facet_results")
    assert spans, "a faceted request must produce facet_results spans"
    types = {span.attributes["datasette.facet_type"] for span in spans}
    assert "column" in types


@pytest.mark.asyncio
async def test_facet_suggest_span(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/facetable.json?_extra=suggested_facets")
    assert response.status_code == 200

    spans = spans_named(otel_spans, "datasette.facet_suggest")
    assert spans, "facet suggestion must produce its own spans"
    types = {span.attributes["datasette.facet_type"] for span in spans}
    assert "column" in types


@pytest.mark.asyncio
async def test_facet_span_covers_its_queries(ds_client, otel_spans):
    "The facet's SQL must nest inside the facet span, not float beside it."
    response = await ds_client.get("/fixtures/facetable.json?_facet=state")
    assert response.status_code == 200

    facet_spans = spans_named(otel_spans, "datasette.facet_results")
    facet_ids = {span.context.span_id for span in facet_spans}
    queries_under_facets = [
        span
        for span in otel_spans.get_finished_spans()
        if span.name == "db.query"
        and span.parent is not None
        and span.parent.span_id in facet_ids
    ]
    assert queries_under_facets, "facet SQL should be a child of the facet span"


@pytest.mark.asyncio
async def test_facet_duration_metric(ds_client, otel_metrics):
    response = await ds_client.get("/fixtures/facetable.json?_facet=state")
    assert response.status_code == 200
    otel_metrics.collect()
    points = otel_metrics.points(
        "datasette.facet.duration",
        {"datasette.facet_type": "column", "datasette.facet_phase": "results"},
    )
    assert points
    assert points[0].count >= 1


@pytest.mark.asyncio
async def test_render_span_for_json(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/facetable.json")
    assert response.status_code == 200

    spans = spans_named(otel_spans, "datasette.render")
    assert spans, "a JSON response must produce a render span"
    span = spans[-1]
    assert span.attributes["datasette.format"] == "json"
    assert span.attributes["datasette.rows_returned"] > 0


@pytest.mark.asyncio
async def test_render_span_for_row_json(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/facetable/1.json")
    assert response.status_code == 200
    spans = spans_named(otel_spans, "datasette.render")
    assert spans
    assert spans[-1].attributes["datasette.format"] == "json"


@pytest.mark.asyncio
async def test_render_span_for_query_json(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/-/query.json?sql=select+1+as+n")
    assert response.status_code == 200
    spans = spans_named(otel_spans, "datasette.render")
    assert spans
    assert spans[-1].attributes["datasette.format"] == "json"


@pytest.mark.asyncio
async def test_csv_stream_span(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/facetable.csv")
    assert response.status_code == 200

    spans = spans_named(otel_spans, "datasette.csv_stream")
    assert spans, "a CSV response must produce a csv_stream span"
    span = spans[-1]
    assert span.attributes["datasette.rows_written"] > 0
    assert span.attributes["datasette.pages_fetched"] == 1
    assert span.attributes["datasette.stream"] is False
    assert span.attributes["db.namespace"] == "fixtures"
    assert span.attributes["datasette.table"] == "facetable"


@pytest.mark.asyncio
async def test_csv_stream_span_counts_every_page(otel_spans):
    """
    ?_stream=1 pages through the whole table. The span must report the real
    row and page counts - the point of the span is that this loop is where a
    streaming export spends its time, and it runs after the view returned.
    """
    import sqlite_utils

    ds = Datasette(memory=True)
    db = ds.add_memory_database("csvstream")
    await db.execute_write_fn(
        lambda conn: sqlite_utils.Database(conn)["rows"].insert_all(
            [{"id": i, "v": f"value-{i}"} for i in range(250)], pk="id"
        )
    )
    await ds.invoke_startup()
    otel_spans.clear()

    response = await ds.client.get("/csvstream/rows.csv?_stream=1&_size=100")
    assert response.status_code == 200
    assert response.text.count("\n") >= 250

    span = spans_named(otel_spans, "datasette.csv_stream")[-1]
    assert span.attributes["datasette.stream"] is True
    assert span.attributes["datasette.rows_written"] == 250
    assert span.attributes["datasette.pages_fetched"] > 1
    ds.close()


@pytest.mark.asyncio
async def test_csv_rows_streamed_metric(ds_client, otel_metrics):
    response = await ds_client.get("/fixtures/facetable.csv")
    assert response.status_code == 200
    otel_metrics.collect()
    points = otel_metrics.points("datasette.csv.rows_streamed")
    assert points
    assert points[0].value > 0


@pytest.mark.asyncio
async def test_no_response_spans_without_a_request(otel_spans):
    "Constructing and starting a Datasette must not emit response-side spans."
    ds = Datasette(memory=True)
    await ds.invoke_startup()
    otel_spans.clear()
    await ds.get_database("_memory").execute("select 1")
    names = {span.name for span in otel_spans.get_finished_spans()}
    assert "datasette.render_template" not in names
    assert "datasette.csv_stream" not in names
    assert "datasette.render" not in names
    ds.close()
