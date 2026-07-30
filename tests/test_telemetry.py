import json
import sqlite3

import pytest
import sqlite_utils

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.trace import SpanKind, StatusCode

from datasette.database import Database
from datasette.telemetry import (
    MAX_SQL_LENGTH,
    SCHEMA_URL,
    sql_attribute,
    sql_operation_name,
    tracer,
)
from datasette.version import __version__

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


@pytest.mark.asyncio
async def test_db_query_is_client_kind_and_its_execute_child_is_internal(
    ds_client, otel_spans
):
    """
    db.query is a real database call, so semantic conventions - and trace
    UIs, which key their database styling off this - expect SpanKind.CLIENT.

    db.query.execute stays INTERNAL: it is Datasette's own decomposition of
    that one logical query (the in-thread execution), not a second database
    call, so marking it CLIENT too would make one query look like two to any
    UI that counts spans by kind.

    Both sides are asserted so this pins the decision, not just the change -
    a regression that makes every span CLIENT (or every span INTERNAL) would
    slip past a test that checked only one of the two.
    """
    response = await ds_client.get("/fixtures/-/query.json?sql=select+1")
    assert response.status_code == 200

    spans = otel_spans.get_finished_spans()
    query_spans = [s for s in spans if s.name == "db.query"]
    execute_spans = [s for s in spans if s.name == "db.query.execute"]
    assert query_spans, "expected a db.query span"
    assert execute_spans, "expected a db.query.execute span"

    assert query_spans[-1].kind == SpanKind.CLIENT
    assert execute_spans[-1].kind == SpanKind.INTERNAL


@pytest.mark.asyncio
async def test_instrumentation_scope_has_version_and_schema_url(
    ds_client, otel_spans, otel_metrics
):
    """
    Every span and metric names the Datasette version that produced it and
    the semconv schema its attribute names follow.

    Before `get_tracer()` and `get_meter()` were given a version and a schema
    URL, every scope exported was `name='datasette' version='' schema_url=''`,
    so nothing downstream could tell which Datasette a span came from.

    One request produces both a `db.query` span and a
    `db.client.operation.duration` point, so both scopes come from the same
    request.
    """
    response = await ds_client.get("/fixtures/-/query.json?sql=select+1")
    assert response.status_code == 200

    spans = _db_query_spans(otel_spans)
    assert spans, "expected at least one db.query span"
    span_scope = spans[-1].instrumentation_scope
    assert span_scope.name == "datasette"
    assert span_scope.version == __version__
    assert span_scope.schema_url == SCHEMA_URL

    # MetricsCollector.collect() keeps only data points and flattens the scope
    # away, so the reader is walked directly here.
    data = otel_metrics.reader.get_metrics_data()
    metric_scopes = {
        scope_metrics.scope.name: scope_metrics.scope
        for resource_metrics in data.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
    }
    assert "datasette" in metric_scopes, f"no datasette scope in {list(metric_scopes)}"
    metric_scope = metric_scopes["datasette"]
    assert metric_scope.version == __version__
    assert metric_scope.schema_url == SCHEMA_URL

    # The version and the URL are compared against the same constants the
    # instrumentation is built from, so those comparisons cannot catch a wrong
    # *value* - only a dropped argument, which is what they are for. These two
    # check the values themselves are the shape they claim to be.
    assert __version__, "the scope version must not be empty"
    assert SCHEMA_URL.startswith("https://opentelemetry.io/schemas/")


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
async def test_invoke_startup_produces_one_trace_not_dozens_of_orphans(otel_spans):
    """
    invoke_startup() runs before any request exists: register_events,
    register_actions, the internal catalog's db.query and db.write.execute
    spans, and the prepare_connection warm-up each of those triggers the
    first time a database is touched. None of it has an ambient span to nest
    under, so each piece used to become its own single- or two-span root
    trace - around twenty of them per instance, cluttering a trace UI's
    search results around every real request.

    invoke_startup() now wraps its whole body in one `datasette.startup`
    span, so all of that - including the prepare_connection spans - lands in
    a single trace.
    """
    from datasette.app import Datasette

    ds = Datasette(memory=True)
    ds.add_memory_database("otel_startup_orphans_test")
    otel_spans.clear()

    # No `with tracer.start_as_current_span(...)` around this call - that is
    # the point. In production invoke_startup() runs from
    # AsgiRunOnFirstRequest during the ASGI "lifespan" scope, which
    # opentelemetry-instrumentation-asgi does not wrap in a span, so there is
    # no ambient span here either.
    await ds.invoke_startup()

    spans = otel_spans.get_finished_spans()
    assert spans, "expected invoke_startup() to produce spans"

    trace_ids = {span.context.trace_id for span in spans}
    assert len(trace_ids) == 1, (
        f"expected every span from one invoke_startup() call to share a "
        f"single trace, got {len(trace_ids)} distinct trace_ids across: "
        f"{sorted(s.name for s in spans)}"
    )

    startup_spans = [s for s in spans if s.name == "datasette.startup"]
    assert len(startup_spans) == 1
    assert startup_spans[0].parent is None, "datasette.startup must be the root"

    prepare_connection_spans = [
        s for s in spans if s.name == "datasette.hook.prepare_connection"
    ]
    assert prepare_connection_spans, (
        "expected a prepare_connection span from warming up the new "
        "database's connection during startup"
    )
    for span in prepare_connection_spans:
        assert span.parent is not None, (
            "prepare_connection must nest under datasette.startup, not "
            "become its own orphan root"
        )
        assert span.attributes["datasette.plugin"]
        assert span.attributes["code.function"] == "prepare_connection"

    ds.close()


@pytest.mark.asyncio
async def test_block_false_write_spans_are_linked_root_spans_not_children(
    ds_client, otel_spans
):
    """
    A block=False write returns immediately without awaiting the reply
    future, so the enclosing span can finish (and export) before
    db.write.queue_wait/db.write.execute even exist. Parenting those spans to
    it used to be a documented-but-not-fixed wart: a child bar outliving its
    already-closed parent is legal OTel but renders badly in a trace UI.

    The fix: for block=False the enqueueing request *caused* the write
    without *containing* it, so its spans are built as their own roots (no
    parent) carrying a single Link back to the enqueueing span's context
    instead. This test awaits the reply future as its synchronisation point,
    since block=False means the spans may not have been exported yet, and
    pins both halves of the distinction: block=False gets a root plus a link,
    block=True still parents exactly as before.
    """
    db = ds_client.ds.get_database("fixtures")

    def slow_write(conn):
        import time as _time

        _time.sleep(0.05)

    # --- block=False: root span, single link back to the enqueuer -----
    with tracer.start_as_current_span("test-block-false-parent") as parent_span:
        parent_ctx = parent_span.get_span_context()
        # Calling the private _send_to_write_thread() directly (rather than
        # execute_write_fn(..., block=False), which only returns task_id)
        # so the test can await the reply_future itself.
        _, reply_future = await db._send_to_write_thread(slow_write, block=False)

    # The parent span above has already ended (the `with` block exited)
    # before the write thread has necessarily even started the task.
    await reply_future

    spans = otel_spans.get_finished_spans()
    execute_spans = [s for s in spans if s.name == "db.write.execute"]
    assert execute_spans, "expected a db.write.execute span"
    execute_span = execute_spans[-1]

    # No longer a child of the (already-closed) enqueueing span...
    assert execute_span.parent is None, "block=False write span must be a root"
    # ...even though it genuinely does end after that span closed, which is
    # exactly why parenting it was the wrong relationship.
    assert execute_span.end_time > parent_span.end_time
    # ...and carries exactly one link back to the enqueueing span instead.
    assert len(execute_span.links) == 1
    link = execute_span.links[0]
    assert link.context.trace_id == parent_ctx.trace_id
    assert link.context.span_id == parent_ctx.span_id

    # --- block=True: unchanged - still a real, awaited child -----------
    otel_spans.clear()
    with tracer.start_as_current_span("test-block-true-parent") as parent_span2:
        parent_span_id = parent_span2.get_span_context().span_id
        await db._send_to_write_thread(slow_write, block=True)

    spans = otel_spans.get_finished_spans()
    execute_spans = [s for s in spans if s.name == "db.write.execute"]
    assert execute_spans, "expected a db.write.execute span"
    execute_span = execute_spans[-1]
    assert execute_span.parent is not None
    assert execute_span.parent.span_id == parent_span_id
    assert (
        not execute_span.links
    ), "block=True write span has a real parent, so it should carry no link"


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


# --- db.operation.name and db.collection.name -------------------------------
#
# Both are `optional=True` in the registry and set only where Datasette
# already knows the answer with confidence - never guessed from parsing.
# Omission is the expected, correct outcome for anything the allowlist or the
# call site does not recognise, so several tests below assert an attribute's
# *absence*, not just its presence.


def test_sql_operation_name_recognises_allowlisted_keywords():
    "The unit-level function, independent of any span machinery."
    assert sql_operation_name("select 1") == "SELECT"
    assert sql_operation_name("  \n  INSERT into t values (1)") == "INSERT"
    assert sql_operation_name("Create Table t (id integer)") == "CREATE"
    assert sql_operation_name("with c as (select 1) select * from c") == "WITH"


def test_sql_operation_name_omits_rather_than_guesses():
    """
    Anything not on the fixed allowlist comes back None, never a value made
    up from whatever word happens to come first - including a nonsense
    statement, a statement that leads with punctuation such as a
    parenthesised SELECT, and a recognisable SQL keyword that is simply not
    on the (deliberately short) allowlist.
    """
    assert sql_operation_name("this is not sql at all") is None
    assert sql_operation_name("(select 1) union (select 2)") is None
    assert sql_operation_name("") is None
    assert sql_operation_name("   ") is None
    # REINDEX is a real SQLite keyword, just not one of the ones the ticket
    # put on the allowlist - proves this is a fixed list, not "any keyword".
    assert sql_operation_name("reindex t") is None


@pytest.mark.asyncio
async def test_db_operation_name_set_for_select(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/-/query.json?sql=select+1")
    assert response.status_code == 200

    spans = _db_query_spans(otel_spans)
    assert spans
    assert spans[-1].attributes["db.operation.name"] == "SELECT"


@pytest.mark.asyncio
async def test_db_operation_name_omitted_for_unrecognised_statement(
    ds_client, otel_spans
):
    "A nonsense statement must not produce a garbage db.operation.name value."
    db = ds_client.ds.get_database("fixtures")
    with pytest.raises(sqlite3.OperationalError):
        await db.execute("this is not valid sql")

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert span.attributes["db.query.text"] == "this is not valid sql"
    assert "db.operation.name" not in span.attributes


@pytest.mark.asyncio
async def test_db_operation_name_set_for_write_and_executemany(ds_client, otel_spans):
    db = Database(ds_client.ds, is_memory=True)
    ds_client.ds.add_database(db, name="db_operation_name_write")
    try:
        await db.execute_write("create table t (id integer primary key, v text)")
        otel_spans.clear()

        await db.execute_write("insert into t (id, v) values (1, 'a')")
        await db.execute_write_many(
            "insert into t (id, v) values (?, ?)", [(2, "b"), (3, "c")]
        )

        spans = _db_query_spans(otel_spans)
        assert len(spans) == 2
        assert spans[0].attributes["db.operation.name"] == "INSERT"
        assert spans[1].attributes["db.operation.name"] == "INSERT"
        assert spans[1].attributes["datasette.executemany"] is True
    finally:
        ds_client.ds.remove_database("db_operation_name_write")


@pytest.mark.asyncio
async def test_db_operation_name_omitted_for_execute_write_script(
    ds_client, otel_spans
):
    """
    execute_write_script() runs multiple semicolon-separated statements, so
    per semantic conventions' guidance for db.operation.name - it should not
    be extracted from query text that can contain more than one operation -
    the attribute is left off entirely rather than reporting only the first
    statement's keyword.
    """
    db = Database(ds_client.ds, is_memory=True)
    ds_client.ds.add_database(db, name="db_operation_name_script")
    try:
        otel_spans.clear()

        await db.execute_write_script(
            "create table t (id integer); create table t2 (id integer);"
        )

        spans = _db_query_spans(otel_spans)
        assert spans
        span = spans[-1]
        assert span.attributes["datasette.executescript"] is True
        assert "db.operation.name" not in span.attributes
    finally:
        ds_client.ds.remove_database("db_operation_name_script")


@pytest.mark.asyncio
async def test_db_collection_name_set_on_table_page(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/simple_primary_key.json")
    assert response.status_code == 200

    spans_with_table = [
        span
        for span in _db_query_spans(otel_spans)
        if "db.collection.name" in span.attributes
    ]
    assert (
        spans_with_table
    ), "expected the main table-page query to carry db.collection.name"
    for span in spans_with_table:
        assert span.attributes["db.collection.name"] == "simple_primary_key"


@pytest.mark.asyncio
async def test_db_collection_name_set_on_row_page(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/simple_primary_key/1.json")
    assert response.status_code == 200

    spans_with_table = [
        span
        for span in _db_query_spans(otel_spans)
        if "db.collection.name" in span.attributes
    ]
    assert spans_with_table, "expected the row-page query to carry db.collection.name"
    for span in spans_with_table:
        assert span.attributes["db.collection.name"] == "simple_primary_key"


@pytest.mark.asyncio
async def test_db_collection_name_omitted_for_arbitrary_sql(ds_client, otel_spans):
    """
    An arbitrary ?sql= query is not run through the table or row view, so
    nothing there knows the table - even though the table name is sitting
    right there in the SQL text. Determining it would mean parsing, which is
    exactly what this attribute must not do; the analyser that could do it
    accurately (`analyze_sql_tables()`) is too expensive to run per query and
    is deliberately not called from the telemetry path.
    """
    response = await ds_client.get(
        "/fixtures/-/query.json?sql=select+*+from+simple_primary_key"
    )
    assert response.status_code == 200

    spans = _db_query_spans(otel_spans)
    assert spans
    assert "db.collection.name" not in spans[-1].attributes
