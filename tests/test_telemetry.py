import json
import sqlite3

import pytest
import sqlite_utils

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.trace import StatusCode

from datasette.database import Database
from datasette.telemetry import MAX_SQL_LENGTH, sql_attribute, tracer

SECRET_PARAM_VALUE = "SUPER_SECRET_PARAM_VALUE_XYZ_123"


def _db_query_spans(otel_spans):
    return [span for span in otel_spans.get_finished_spans() if span.name == "db.query"]


def _all_attribute_values(otel_spans):
    "Every attribute value across every finished span, for the 'no leaked param values' test."
    values = []
    for span in otel_spans.get_finished_spans():
        values.extend((span.attributes or {}).values())
        for event in span.events:
            values.extend((event.attributes or {}).values())
    return values


@pytest.mark.asyncio
async def test_db_query_span_basic_attributes(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/-/query.json?sql=select+1")
    assert response.status_code == 200

    spans = _db_query_spans(otel_spans)
    assert spans, "expected at least one db.query span"
    span = spans[-1]

    assert span.attributes["db.system"] == "sqlite"
    assert span.attributes["db.namespace"] == "fixtures"
    assert span.attributes["db.query.text"] == "select 1"
    assert span.attributes["datasette.rows_returned"] == 1
    assert span.attributes["datasette.truncated"] is False
    assert isinstance(span.attributes["datasette.time_limit_ms"], int)
    assert span.status.status_code == StatusCode.UNSET


def test_sql_attribute_truncates_at_2048():
    short_sql = "select 1"
    assert sql_attribute(short_sql) == "select 1"

    long_sql = "select 1 -- " + ("x" * 3000)
    truncated = sql_attribute(long_sql)
    assert len(truncated) == MAX_SQL_LENGTH + len("…[truncated]")
    assert truncated.startswith("select 1 -- ")
    assert truncated.endswith("…[truncated]")


@pytest.mark.asyncio
async def test_db_query_text_is_truncated_in_real_span(ds_client, otel_spans):
    # A long trailing SQL comment keeps the query valid and executable while
    # pushing db.query.text well past the 2048 char cap.
    long_sql = "select 1 -- " + ("x" * 3000)
    response = await ds_client.get("/fixtures/-/query.json", params={"sql": long_sql})
    assert response.status_code == 200

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    recorded = span.attributes["db.query.text"]
    assert len(recorded) == MAX_SQL_LENGTH + len("…[truncated]")
    assert recorded.endswith("…[truncated]")


@pytest.mark.asyncio
async def test_no_span_attribute_ever_contains_a_parameter_value(ds_client, otel_spans):
    response = await ds_client.get(
        "/fixtures/-/query.json",
        params={"sql": "select :secret", "secret": SECRET_PARAM_VALUE},
    )
    assert response.status_code == 200
    # Sanity check the value really did flow through as a bound parameter,
    # not inlined into the SQL text, otherwise this test would be vacuous.
    assert SECRET_PARAM_VALUE in json.dumps(response.json())

    for value in _all_attribute_values(otel_spans):
        if isinstance(value, str):
            assert SECRET_PARAM_VALUE not in value
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, str):
                    assert SECRET_PARAM_VALUE not in item

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert "select :secret" in span.attributes["db.query.text"]
    assert span.attributes.get("datasette.param_count") == 1


@pytest.mark.asyncio
async def test_query_interrupted_sets_error_status(ds_client, otel_spans):
    response = await ds_client.get(
        "/fixtures/-/query.json",
        params={"sql": "select sleep(0.05)", "_timelimit": 5},
    )
    assert response.status_code == 400

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["datasette.interrupted"] is True
    assert span.events
    assert all(event.name == "exception" for event in span.events)


@pytest.mark.asyncio
async def test_sql_error_sets_error_status_by_default(ds_client, otel_spans):
    db = ds_client.ds.get_database("fixtures")
    with pytest.raises(sqlite3.OperationalError):
        await db.execute("select this_is_not_valid_sql from nowhere")

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert span.status.status_code == StatusCode.ERROR
    assert any(event.name == "exception" for event in span.events)


@pytest.mark.asyncio
async def test_suppressed_sql_error_is_not_a_span_error(ds_client, otel_spans):
    """
    log_sql_errors=False means the caller is probing and expects failures.

    Facet suggestion runs `json_type(column)` against every column precisely
    to discover which ones raise, so marking those spans as errors would put
    two red spans per text column on every table page - burying real failures
    and tripping any alerting keyed on span status.
    """
    db = ds_client.ds.get_database("fixtures")
    with pytest.raises(sqlite3.OperationalError):
        await db.execute(
            "select json_type(content) from simple_primary_key where content != ''",
            log_sql_errors=False,
        )

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert span.status.status_code == StatusCode.UNSET
    assert span.attributes["datasette.sql_error_suppressed"] is True
    assert not [event for event in span.events if event.name == "exception"]


# --- Ticket 03: context propagation across thread boundaries ---------------
#
# Every assertion below checks *parentage* (child.parent.span_id ==
# expected_parent.context.span_id), not merely that spans exist. Spans can
# exist and still be wrongly parented (or unparented root spans) if a
# thread boundary drops the otel context - which is exactly the failure
# mode this ticket exists to prevent.


@pytest.mark.asyncio
async def test_read_query_execute_span_parents_to_db_query_across_worker_thread(
    ds_client, otel_spans
):
    # execute_fn()'s executor.submit() is thread boundary #1. The
    # db.query.execute span is created inside that worker thread; if the
    # copy_context() propagation were missing it would come back as an
    # unparented root span instead of a child of db.query.
    response = await ds_client.get("/fixtures/-/query.json?sql=select+1")
    assert response.status_code == 200

    spans = otel_spans.get_finished_spans()
    query_spans = [s for s in spans if s.name == "db.query"]
    execute_spans = [s for s in spans if s.name == "db.query.execute"]
    assert query_spans, "expected a db.query span"
    assert execute_spans, "expected a db.query.execute span"

    query_span = query_spans[-1]
    execute_span = execute_spans[-1]
    assert execute_span.parent is not None
    assert execute_span.parent.span_id == query_span.context.span_id


@pytest.mark.asyncio
async def test_db_write_produces_queue_wait_and_execute_spans_as_children_of_db_query(
    ds_client, otel_spans
):
    # Thread boundary #2 (WriteTask -> queue.Queue -> write thread).
    # db.write.queue_wait and db.write.execute are both direct children of
    # the db.query span that was current on the event loop at enqueue time
    # - siblings of each other, not nested inside one another.
    db = ds_client.ds.get_database("fixtures")
    await db.execute_write(
        "create table if not exists otel_ticket3_write_test (id integer primary key)"
    )

    spans = otel_spans.get_finished_spans()
    query_spans = [s for s in spans if s.name == "db.query"]
    queue_wait_spans = [s for s in spans if s.name == "db.write.queue_wait"]
    execute_spans = [s for s in spans if s.name == "db.write.execute"]
    assert query_spans, "expected a db.query span"
    assert queue_wait_spans, "expected a db.write.queue_wait span"
    assert execute_spans, "expected a db.write.execute span"

    query_span = query_spans[-1]
    queue_wait_span = queue_wait_spans[-1]
    execute_span = execute_spans[-1]

    assert queue_wait_span.parent is not None
    assert queue_wait_span.parent.span_id == query_span.context.span_id
    assert execute_span.parent is not None
    assert execute_span.parent.span_id == query_span.context.span_id

    # Explicit timestamps rather than the span's own creation/end time.
    assert queue_wait_span.start_time <= queue_wait_span.end_time

    assert execute_span.attributes["datasette.isolated_connection"] is False
    assert execute_span.attributes["datasette.transaction"] is True


@pytest.mark.asyncio
async def test_execute_isolated_fn_immutable_db_context_propagates(
    tmp_path, app_client, otel_spans
):
    # Thread boundary #3 - "a fifth thread boundary, easy to miss":
    # immutable databases route execute_isolated_fn() through
    # loop.run_in_executor() directly rather than through the write
    # thread. A span created inside that worker must still parent to
    # whatever was the current span when execute_isolated_fn() was
    # awaited, or this path silently produces unparented root spans on
    # every immutable-database operation.
    db_path = tmp_path / "otel_ticket3_immutable.db"
    sqlite_utils.Database(str(db_path))["t"].insert({"id": 1}, pk="id")

    db = Database(app_client.ds, path=str(db_path), is_mutable=False)
    app_client.ds.add_database(db, name="otel_ticket3_immutable")

    def fn(conn):
        with tracer.start_as_current_span("child-in-isolated-immutable-db"):
            pass

    try:
        with tracer.start_as_current_span("test-parent-for-isolated") as parent_span:
            parent_span_id = parent_span.get_span_context().span_id
            await db.execute_isolated_fn(fn)
    finally:
        app_client.ds.remove_database("otel_ticket3_immutable")

    spans = otel_spans.get_finished_spans()
    children = [s for s in spans if s.name == "child-in-isolated-immutable-db"]
    assert children, "expected a span created inside execute_isolated_fn's worker"
    assert children[-1].parent is not None
    assert children[-1].parent.span_id == parent_span_id


@pytest.mark.asyncio
async def test_block_false_write_spans_can_end_after_parent_closes(
    ds_client, otel_spans
):
    # Known, documented (not fixed) limitation: a block=False write returns
    # immediately, so the enclosing db.query span can finish and export
    # before db.write.queue_wait/db.write.execute exist at all. This test
    # pins that behaviour down rather than "fixing" it - the parent/child
    # relationship is still correct once the child spans do appear, the
    # waterfall view is just incomplete for block=False.
    db = ds_client.ds.get_database("fixtures")

    def slow_write(conn):
        import time as _time

        _time.sleep(0.05)

    with tracer.start_as_current_span("test-block-false-parent") as parent_span:
        parent_span_id = parent_span.get_span_context().span_id
        # Calling the private _send_to_write_thread() directly (rather than
        # execute_write_fn(..., block=False), which only returns task_id)
        # so the test can await the reply_future itself.
        _, reply_future = await db._send_to_write_thread(slow_write, block=False)

    # The parent span above has already ended (the `with` block exited)
    # before the write thread has necessarily even started the task.
    await reply_future

    spans = otel_spans.get_finished_spans()
    execute_spans = [
        s
        for s in spans
        if s.name == "db.write.execute"
        and s.parent
        and s.parent.span_id == parent_span_id
    ]
    assert execute_spans, "expected a db.write.execute span parented to the closed span"
    execute_span = execute_spans[-1]
    # The child ended strictly after its already-closed parent.
    assert execute_span.end_time > parent_span.end_time


@pytest.mark.asyncio
async def test_suppressed_sql_error_is_not_an_error_on_the_execute_span(
    ds_client, otel_spans
):
    """
    The inner db.query.execute span must honour log_sql_errors too.

    It is created inside the worker thread, so it would otherwise mark every
    facet-suggestion probe as failed even though the outer db.query span
    correctly reports the failure as suppressed.
    """
    db = ds_client.ds.get_database("fixtures")
    with pytest.raises(sqlite3.OperationalError):
        await db.execute(
            "select json_type(content) from simple_primary_key where content != ''",
            log_sql_errors=False,
        )

    execute_spans = [
        span
        for span in otel_spans.get_finished_spans()
        if span.name == "db.query.execute"
    ]
    assert execute_spans
    span = execute_spans[-1]
    assert span.status.status_code == StatusCode.UNSET
    assert not [event for event in span.events if event.name == "exception"]
