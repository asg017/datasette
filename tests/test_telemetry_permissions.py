"""
Tests for the `datasette.permission_check` span wrapped around
Datasette.allowed_many() - the single choke point behind allowed(),
ensure_permission() and check_visibility() - plus the
`datasette.permission_resources_sql` and `datasette.allowed_resources` spans
around the resource-listing path (Datasette.allowed_resources_sql() and
Datasette.allowed_resources()).
"""

from contextlib import contextmanager

import pytest

pytest.importorskip("opentelemetry.sdk")

from datasette import hookimpl
from datasette.plugins import pm
from datasette.resources import TableResource

# A distinctive, obviously-not-real actor id used to prove no actor
# identifier ever leaks onto a span attribute.
SENTINEL_ACTOR_ID = "OTEL_SENTINEL_ACTOR_ID_4f8c9b2e7a11"


@contextmanager
def register(plugin, name):
    pm.register(plugin, name=name)
    try:
        yield
    finally:
        pm.unregister(plugin, name=name)


def _permission_check_spans(otel_spans):
    return [
        span
        for span in otel_spans.get_finished_spans()
        if span.name == "datasette.permission_check"
    ]


def _all_attribute_values(otel_spans):
    "Every attribute value across every finished span (and span events)."
    values = []
    for span in otel_spans.get_finished_spans():
        values.extend((span.attributes or {}).values())
        for event in span.events:
            values.extend((event.attributes or {}).values())
    return values


@pytest.mark.asyncio
async def test_permission_check_span_basic_attributes(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = _permission_check_spans(otel_spans)
    assert spans, "expected at least one datasette.permission_check span"

    plausible_actions = {"view-instance", "view-database", "view-table"}
    matching = [
        span
        for span in spans
        if span.attributes.get("datasette.action") in plausible_actions
    ]
    assert matching, [dict(span.attributes) for span in spans]

    span = matching[0]
    assert isinstance(span.attributes["datasette.result"], bool)
    assert isinstance(span.attributes["datasette.actor_present"], bool)
    assert isinstance(span.attributes["datasette.permission.cached"], bool)


@pytest.mark.asyncio
async def test_db_query_span_is_parented_to_permission_check_span(
    ds_client, otel_spans
):
    # Permission checks run against the *internal* database. A fresh
    # request guarantees at least one cache miss, so
    # check_permissions_for_actions() must issue a real db.query.
    response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    finished = otel_spans.get_finished_spans()
    permission_spans = [s for s in finished if s.name == "datasette.permission_check"]
    query_spans = [s for s in finished if s.name == "db.query"]
    assert permission_spans, "expected at least one datasette.permission_check span"
    assert query_spans, "expected at least one db.query span"

    permission_span_ids = {s.context.span_id for s in permission_spans}
    parented = [
        q
        for q in query_spans
        if q.parent is not None and q.parent.span_id in permission_span_ids
    ]
    assert parented, (
        "expected at least one db.query span parented directly to a "
        "datasette.permission_check span"
    )
    # And the query really did run against the internal database, not
    # some incidental fixtures query that happened to be in flight.
    from datasette.app import INTERNAL_DB_NAME

    assert any(q.attributes.get("db.namespace") == INTERNAL_DB_NAME for q in parented)


@pytest.mark.asyncio
async def test_resource_attribute_present_for_table_absent_for_global(
    ds_client, otel_spans
):
    ds = ds_client.ds
    await ds.allowed(
        action="view-table",
        resource=TableResource(database="fixtures", table="facetable"),
        actor=None,
    )
    await ds.allowed(action="view-instance", actor=None)

    spans = _permission_check_spans(otel_spans)
    table_spans = [
        s for s in spans if s.attributes.get("datasette.action") == "view-table"
    ]
    instance_spans = [
        s for s in spans if s.attributes.get("datasette.action") == "view-instance"
    ]
    assert table_spans, [dict(s.attributes) for s in spans]
    assert instance_spans, [dict(s.attributes) for s in spans]

    assert table_spans[-1].attributes["datasette.resource"] == "fixtures/facetable"
    assert "datasette.resource" not in instance_spans[-1].attributes


@pytest.mark.asyncio
async def test_multi_action_call_sets_actions_plural_not_action(ds_client, otel_spans):
    ds = ds_client.ds
    results = await ds.allowed_many(
        actions=["view-table", "insert-row"],
        resource=TableResource(database="fixtures", table="facetable"),
        actor=None,
    )
    assert set(results.keys()) == {"view-table", "insert-row"}

    spans = _permission_check_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert span.attributes["datasette.actions"] == ("view-table", "insert-row")
    assert "datasette.action" not in span.attributes
    # datasette.result is only recorded for single-action calls
    assert "datasette.result" not in span.attributes


@pytest.mark.asyncio
async def test_no_span_attribute_ever_contains_an_actor_id(ds_client, otel_spans):
    cookies = {"ds_actor": ds_client.actor_cookie({"id": SENTINEL_ACTOR_ID})}
    response = await ds_client.get("/fixtures/facetable", cookies=cookies)
    assert response.status_code == 200

    # Sanity check the sentinel actor really was in play, so the
    # assertions below can't pass vacuously.
    permission_spans = _permission_check_spans(otel_spans)
    assert permission_spans
    assert any(
        span.attributes.get("datasette.actor_present") is True
        for span in permission_spans
    )

    for value in _all_attribute_values(otel_spans):
        if isinstance(value, str):
            assert SENTINEL_ACTOR_ID not in value
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, str):
                    assert SENTINEL_ACTOR_ID not in item


def _named_spans(otel_spans, name):
    return [span for span in otel_spans.get_finished_spans() if span.name == name]


@pytest.mark.asyncio
async def test_allowed_resources_span_basic_attributes(ds_client, otel_spans):
    # The database page calls datasette.allowed_resources("view-table", ...)
    # to bulk-fetch every table the actor can see.
    response = await ds_client.get("/fixtures")
    assert response.status_code == 200

    spans = _named_spans(otel_spans, "datasette.allowed_resources")
    assert spans, "expected at least one datasette.allowed_resources span"

    matching = [
        span
        for span in spans
        if span.attributes.get("datasette.action") == "view-table"
    ]
    assert matching, [dict(span.attributes) for span in spans]
    span = matching[0]
    assert isinstance(span.attributes["datasette.resources_returned"], int)


@pytest.mark.asyncio
async def test_permission_resources_sql_and_db_query_nested_under_allowed_resources(
    ds_client, otel_spans
):
    response = await ds_client.get("/fixtures")
    assert response.status_code == 200

    allowed_resources_spans = _named_spans(otel_spans, "datasette.allowed_resources")
    sql_spans = _named_spans(otel_spans, "datasette.permission_resources_sql")
    query_spans = _named_spans(otel_spans, "db.query")
    assert allowed_resources_spans
    assert sql_spans
    assert query_spans

    allowed_resources_span_ids = {s.context.span_id for s in allowed_resources_spans}

    sql_parented = [
        s
        for s in sql_spans
        if s.parent is not None and s.parent.span_id in allowed_resources_span_ids
    ]
    assert sql_parented, (
        "expected a datasette.permission_resources_sql span parented directly "
        "to a datasette.allowed_resources span"
    )

    # The same allowed_resources span must also parent the db.query that
    # executes the built SQL against the internal database.
    sql_parent_ids = {s.parent.span_id for s in sql_parented}
    query_parented = [
        q
        for q in query_spans
        if q.parent is not None and q.parent.span_id in sql_parent_ids
    ]
    assert query_parented, (
        "expected a db.query span parented to the same datasette.allowed_resources "
        "span as a datasette.permission_resources_sql span"
    )


@pytest.mark.asyncio
async def test_allowed_resources_resource_attribute_present_only_with_parent(
    ds_client, otel_spans
):
    ds = ds_client.ds
    await ds.allowed_resources("view-table", None, parent="fixtures")
    await ds.allowed_resources("view-database", None)

    spans = _named_spans(otel_spans, "datasette.allowed_resources")
    table_spans = [
        s for s in spans if s.attributes.get("datasette.action") == "view-table"
    ]
    database_spans = [
        s for s in spans if s.attributes.get("datasette.action") == "view-database"
    ]
    assert table_spans, [dict(s.attributes) for s in spans]
    assert database_spans, [dict(s.attributes) for s in spans]

    assert table_spans[-1].attributes["datasette.resource"] == "fixtures"
    assert "datasette.resource" not in database_spans[-1].attributes


@pytest.mark.asyncio
async def test_no_actor_id_leak_via_allowed_resources_span(ds_client, otel_spans):
    cookies = {"ds_actor": ds_client.actor_cookie({"id": SENTINEL_ACTOR_ID})}
    response = await ds_client.get("/fixtures", cookies=cookies)
    assert response.status_code == 200

    allowed_resources_spans = _named_spans(otel_spans, "datasette.allowed_resources")
    assert allowed_resources_spans
    assert any(
        span.attributes.get("datasette.actor_present") is True
        for span in allowed_resources_spans
    )

    for value in _all_attribute_values(otel_spans):
        if isinstance(value, str):
            assert SENTINEL_ACTOR_ID not in value
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, str):
                    assert SENTINEL_ACTOR_ID not in item


@pytest.mark.asyncio
async def test_second_allowed_call_in_same_request_is_served_from_cache(
    ds_client, otel_spans
):
    "The request-scoped cache (installed by DatasetteRouter.__call__) turns a repeated allowed() check into a cache hit."

    class DoubleCheckPlugin:
        __name__ = "DoubleCheckPlugin"

        @hookimpl
        def extra_body_script(self, datasette, request):
            async def inner():
                resource = TableResource(database="fixtures", table="facetable")
                await datasette.allowed(
                    action="view-table", resource=resource, actor=request.actor
                )
                await datasette.allowed(
                    action="view-table", resource=resource, actor=request.actor
                )
                return ""

            return inner()

    with register(DoubleCheckPlugin(), "double-check-plugin"):
        response = await ds_client.get("/fixtures/facetable")
    assert response.status_code == 200

    spans = [
        span
        for span in _permission_check_spans(otel_spans)
        if span.attributes.get("datasette.action") == "view-table"
        and span.attributes.get("datasette.resource") == "fixtures/facetable"
    ]
    # At least two checks for the same action+resource+actor happened
    # inside one request: the first is a cache miss, the second (and any
    # further ones triggered by page rendering) must be cache hits.
    assert len(spans) >= 2
    cached_flags = [span.attributes["datasette.permission.cached"] for span in spans]
    assert False in cached_flags, "expected at least one cache-miss span"
    assert True in cached_flags, "expected at least one cache-hit span"
