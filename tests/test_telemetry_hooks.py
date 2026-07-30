"""
Tests for per-plugin hook dispatch spans (`datasette.hook.<name>`).

The load-bearing test here is `test_async_hookimpl_span_covers_the_await`:
pluggy's multicall is synchronous, so an `async def` hookimpl returns a
coroutine in ~0ms and only does its real work later, when Datasette awaits it
through `await_me_maybe`. Any instrumentation that times the multicall instead
of the await reports ~0ms for exactly the plugins worth measuring.
"""

import asyncio
from contextlib import contextmanager

import pytest

pytest.importorskip("opentelemetry.sdk")

from datasette import hookimpl
from datasette.plugins import pm

SLEEP_SECONDS = 0.05


@contextmanager
def register(plugin, name):
    pm.register(plugin, name=name)
    try:
        yield
    finally:
        pm.unregister(plugin, name=name)


def hook_spans(otel_spans, hook_name, plugin=None):
    spans = [
        span
        for span in otel_spans.get_finished_spans()
        if span.name == "datasette.hook." + hook_name
    ]
    if plugin is not None:
        spans = [
            span for span in spans if span.attributes["datasette.plugin"] == plugin
        ]
    return spans


class SyncPlugin:
    __name__ = "SyncPlugin"

    def __init__(self):
        self.seen = []

    @hookimpl
    def extra_body_script(self, template, datasette):
        self.seen.append((template, datasette))
        return "/* sync */"


class AsyncPlugin:
    __name__ = "AsyncPlugin"

    @hookimpl
    def extra_body_script(self):
        async def inner():
            await asyncio.sleep(SLEEP_SECONDS)
            return "/* async */"

        return inner()


class OtherSyncPlugin:
    __name__ = "OtherSyncPlugin"

    @hookimpl
    def extra_body_script(self):
        return "/* other */"


class NullRenderCellPlugin:
    __name__ = "NullRenderCellPlugin"

    @hookimpl
    def render_cell(self, value, column):
        # Always None so the per-cell loop never short circuits and this impl
        # is dispatched once for every cell on the page
        return None


@pytest.mark.asyncio
async def test_sync_hookimpl_span(ds_client, otel_spans):
    plugin = SyncPlugin()
    with register(plugin, "sync-plugin"):
        response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = hook_spans(otel_spans, "extra_body_script", plugin="sync-plugin")
    assert len(spans) == 1
    assert spans[0].attributes["code.function"] == "extra_body_script"

    # Wrapping must not disturb pluggy's argument binding: pluggy introspects
    # the impl's signature to decide which of the hookspec's arguments to pass
    assert len(plugin.seen) == 1
    template, datasette = plugin.seen[0]
    assert template.endswith(".html")
    assert hasattr(datasette, "databases")


@pytest.mark.asyncio
async def test_async_hookimpl_span_covers_the_await(ds_client, otel_spans):
    with register(AsyncPlugin(), "async-plugin"):
        response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = hook_spans(otel_spans, "extra_body_script", plugin="async-plugin")
    assert len(spans) == 1
    duration_seconds = (spans[0].end_time - spans[0].start_time) / 1e9
    # The dispatch itself is ~0ms - only a span that covers the await sees this
    assert duration_seconds >= SLEEP_SECONDS * 0.9, (
        f"span lasted {duration_seconds:.4f}s, expected to cover a "
        f"{SLEEP_SECONDS}s sleep inside the hookimpl"
    )


@pytest.mark.asyncio
async def test_two_plugins_same_hook_two_spans(ds_client, otel_spans):
    with (
        register(SyncPlugin(), "sync-plugin"),
        register(OtherSyncPlugin(), "other-sync-plugin"),
    ):
        response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = hook_spans(otel_spans, "extra_body_script")
    by_plugin = {span.attributes["datasette.plugin"] for span in spans}
    assert {"sync-plugin", "other-sync-plugin"}.issubset(by_plugin)
    assert (
        len([s for s in spans if s.attributes["datasette.plugin"] == "sync-plugin"])
        == 1
    )
    assert (
        len(
            [
                s
                for s in spans
                if s.attributes["datasette.plugin"] == "other-sync-plugin"
            ]
        )
        == 1
    )


@pytest.mark.asyncio
async def test_render_cell_aggregated_into_one_span(ds_client, otel_spans):
    with register(NullRenderCellPlugin(), "null-render-cell"):
        response = await ds_client.get("/fixtures/compound_three_primary_keys")
    assert response.status_code == 200

    spans = hook_spans(otel_spans, "render_cell", plugin="null-render-cell")
    assert len(spans) == 1, f"expected 1 aggregate span, got {len(spans)}"
    span = spans[0]
    assert span.attributes["datasette.hook.aggregated"] is True
    # 50 rows (default_page_size in this fixture) x 4 columns, minus skipped pks
    assert span.attributes["datasette.hook.call_count"] > 100
    assert span.attributes["datasette.hook.total_duration_ms"] >= 0
    assert span.attributes["code.function"] == "render_cell"


@pytest.mark.asyncio
async def test_hook_spans_are_parented_to_the_surrounding_span(ds_client, otel_spans):
    "A hookimpl that runs a query should have the db.query span nested under it."

    class QueryingPlugin:
        __name__ = "QueryingPlugin"

        @hookimpl
        def extra_body_script(self, datasette):
            async def inner():
                db = datasette.get_database("fixtures")
                await db.execute("select 1 as querying_plugin_marker")
                return ""

            return inner()

    with register(QueryingPlugin(), "querying-plugin"):
        response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    finished = otel_spans.get_finished_spans()
    hook = [
        span
        for span in finished
        if span.name == "datasette.hook.extra_body_script"
        and span.attributes["datasette.plugin"] == "querying-plugin"
    ]
    assert len(hook) == 1
    query = [
        span
        for span in finished
        if span.name == "db.query"
        and "querying_plugin_marker" in (span.attributes.get("db.query.text") or "")
    ]
    assert len(query) == 1
    assert query[0].parent.span_id == hook[0].context.span_id


@pytest.mark.asyncio
async def test_permission_resources_sql_aggregated_into_one_span_per_plugin(
    ds_client, otel_spans
):
    """
    The hook that dominated the trace before aggregation.

    It is dispatched once per unique action per implementing plugin, which on a
    table page meant 96 spans - 45% of the whole trace - for a hook that does no
    I/O at all: it returns SQL *fragments* that are concatenated into one query
    per batch. Aggregating collapses those to one span per plugin.
    """
    response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = hook_spans(otel_spans, "permission_resources_sql")
    assert spans, "expected at least one permission_resources_sql span"

    # One span per (plugin, function), not one per dispatch.
    for span in spans:
        assert span.attributes["datasette.hook.aggregated"] is True
        assert span.attributes["datasette.hook.call_count"] >= 1
        assert span.attributes["datasette.hook.total_duration_ms"] >= 0

    # The aggregation has to be doing real work, or this test would pass just as
    # well with the hook removed from AGGREGATED_HOOKS on a page that happens to
    # dispatch it once.
    assert max(s.attributes["datasette.hook.call_count"] for s in spans) > 1

    # One span per (plugin, function). Not per function alone: different
    # plugins may name their implementation the same thing, and the test
    # fixtures in this suite do exactly that.
    keys = [
        (span.attributes["datasette.plugin"], span.attributes["code.function"])
        for span in spans
    ]
    assert len(keys) == len(
        set(keys)
    ), f"expected one span per implementation, got duplicates: {sorted(keys)}"


@pytest.mark.asyncio
async def test_aggregated_hooks_still_emit_spans_outside_a_request(otel_spans):
    """
    Aggregation only applies inside a request's window.

    Permission checks also run from CLI paths and plugin code with no request,
    where there is no window to accumulate into. Those must still produce a span
    each rather than silently vanishing.
    """
    from datasette.app import Datasette

    ds = Datasette(memory=True)
    ds.add_memory_database("outside_request")
    await ds.invoke_startup()
    otel_spans.clear()

    await ds.allowed_resources("view-table", actor=None)

    spans = hook_spans(otel_spans, "permission_resources_sql")
    assert spans, "permission hooks outside a request must still be traced"
    for span in spans:
        assert "datasette.hook.aggregated" not in (span.attributes or {})
    ds.close()


@pytest.mark.asyncio
async def test_aggregate_span_duration_is_work_not_window(ds_client, otel_spans):
    """
    An aggregate span's duration must be the time spent in the hook, not the
    span of wall clock between its first and last dispatch.

    This was a real bug: permission_resources_sql fires at the start of a
    request and again during template rendering, so a first-dispatch-to-last
    span read 43ms in Jaeger for a hook that consumed 0.04ms - putting it at
    the top of the "slowest spans" list, which is the first place anyone looks
    when a page is slow.
    """
    response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = hook_spans(otel_spans, "permission_resources_sql")
    assert spans

    for span in spans:
        duration_ms = (span.end_time - span.start_time) / 1e6
        total = span.attributes["datasette.hook.total_duration_ms"]
        window = span.attributes["datasette.hook.window_ms"]
        assert duration_ms == pytest.approx(total, abs=0.001), (
            f"span duration {duration_ms:.4f}ms should equal total_duration_ms "
            f"{total:.4f}ms, not the window"
        )
        # The window really is much wider, which is exactly why this matters.
        assert window >= total

    widest = max(s.attributes["datasette.hook.window_ms"] for s in spans)
    worked = max(s.attributes["datasette.hook.total_duration_ms"] for s in spans)
    assert widest > worked * 5, (
        "expected the dispatches to be genuinely spread across the request - "
        "if they are not, this test is no longer proving anything"
    )
