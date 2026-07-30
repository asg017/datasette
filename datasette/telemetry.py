"""
OpenTelemetry integration for Datasette core.

Core depends on `opentelemetry-api` only. It never creates a
`TracerProvider` or a `MeterProvider`, never configures an exporter, and
never touches sampling - that is the responsibility of whoever is running
Datasette (an `opentelemetry-instrument` agent, a future plugin, or a test
harness). With no provider installed every span produced here is a
`NonRecordingSpan` and every instrument is a no-op, and both cost
approximately nothing.
"""

import functools
import inspect
import threading
import time
import weakref
from contextlib import contextmanager
from contextvars import ContextVar

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace as otel_trace
from opentelemetry.trace import Status, StatusCode

tracer = otel_trace.get_tracer("datasette")
meter = otel_metrics.get_meter("datasette")

MAX_SQL_LENGTH = 2048


def sql_attribute(sql: str) -> str:
    "Truncate SQL text so it is safe to attach to a span as an attribute."
    sql = sql.strip()
    if len(sql) <= MAX_SQL_LENGTH:
        return sql
    return sql[:MAX_SQL_LENGTH] + "…[truncated]"


# --- SQL parameter values -------------------------------------------------
#
# Recording parameter values is OFF by default and stays that way unless an
# operator opts in with the trace_sql_parameters setting. Two things make this
# genuinely dangerous rather than merely verbose:
#
# 1. Permission SQL binds the actor. utils/permissions.py binds
#    json.dumps(actor) as :actor and the actor id as :actor_id on every
#    permission check, so recording parameters on the internal database
#    exports actor identity - which is exactly what the permission spans go
#    out of their way not to do. The "user" mode exists to structurally
#    prevent that, rather than relying on a denylist of parameter names.
#
# 2. Canned queries can bind cookies and headers. The _cookie_* and _header_*
#    magic parameters resolve to request cookies and headers, so a canned
#    query can bind a session cookie or an Authorization header as an
#    ordinary SQL parameter. No mode protects against that, because those
#    queries run against user databases - it is documented instead.

TRACE_SQL_PARAMETER_MODES = ("off", "user", "all")

# Per-value cap. Parameters are usually short, but a single IN (...) clause or
# a pasted blob should not be able to dominate a span.
MAX_PARAM_LENGTH = 256

# Cap on how many parameters are recorded for one query.
MAX_PARAMS = 32


def should_record_parameters(mode: str, is_internal_database: bool) -> bool:
    "Whether parameter values may be recorded, given the setting and the database."
    if mode == "all":
        return True
    if mode == "user":
        return not is_internal_database
    return False


def _parameter_value(value) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Never the content: a blob is both useless as a span attribute and
        # the most likely thing to contain something private.
        return f"<bytes[{len(value)}]>"
    text = str(value)
    if len(text) <= MAX_PARAM_LENGTH:
        return text
    return text[:MAX_PARAM_LENGTH] + "…[truncated]"


def parameter_attributes(params):
    """
    Yield (attribute_name, value) pairs for a query's parameters.

    Named parameters keep their names; positional parameters are numbered.
    Both are namespaced under OTel's db.query.parameter.<key> convention.
    """
    if isinstance(params, dict):
        items = list(params.items())
    else:
        items = list(enumerate(params or []))
    for key, value in items[:MAX_PARAMS]:
        yield f"db.query.parameter.{key}", _parameter_value(value)


# Hooks that Datasette dispatches from inside per-row/per-cell loops. A single
# 100 row x 15 column table page dispatches render_cell ~1,500 times; one span
# per dispatch would blow straight through BatchSpanProcessor's default
# max_queue_size of 2048 and silently drop every other span in the request.
# These are counted and timed into a single aggregate span per request instead.
AGGREGATED_HOOKS = frozenset({"render_cell"})

# Per-request accumulator for AGGREGATED_HOOKS. None outside a request.
_hook_aggregate = ContextVar("datasette_hook_aggregate", default=None)


@contextmanager
def aggregate_hook_spans():
    """
    Open an aggregation window - one per HTTP request. Dispatches of the
    hooks in AGGREGATED_HOOKS that happen inside the window are counted and
    timed rather than producing a span each, and one aggregate span per
    (hook, plugin) pair is emitted when the window closes.
    """
    bucket = {}
    token = _hook_aggregate.set(bucket)
    try:
        yield bucket
    finally:
        _hook_aggregate.reset(token)
        if bucket:
            _flush_hook_aggregate(bucket)


def _flush_hook_aggregate(bucket):
    for (hook_name, plugin_name, function_name), stats in bucket.items():
        span = tracer.start_span(
            "datasette.hook." + hook_name,
            start_time=stats["first_start"],
            attributes={
                "datasette.plugin": plugin_name,
                "code.function": function_name,
                "datasette.hook.call_count": stats["count"],
                # Wall time actually spent inside the hook, summed across every
                # dispatch. The span's own duration spans first dispatch to last
                # and so also covers the work in between them.
                "datasette.hook.total_duration_ms": stats["total_ns"] / 1e6,
                "datasette.hook.aggregated": True,
            },
        )
        span.end(end_time=stats["last_end"])


def _record_aggregate(bucket, key, started, ended):
    stats = bucket.get(key)
    if stats is None:
        bucket[key] = {
            "count": 1,
            "total_ns": ended - started,
            "first_start": started,
            "last_end": ended,
        }
    else:
        stats["count"] += 1
        stats["total_ns"] += ended - started
        stats["last_end"] = ended


def _fail_span(span, exception):
    span.record_exception(exception)
    span.set_status(Status(StatusCode.ERROR, str(exception)))


async def _await_in_span(span, awaitable):
    "Await a hookimpl's coroutine with `span` current, then end the span."
    try:
        with otel_trace.use_span(
            span,
            end_on_exit=False,
            record_exception=False,
            set_status_on_exception=False,
        ):
            return await awaitable
    except BaseException as exception:
        _fail_span(span, exception)
        raise
    finally:
        span.end()


async def _await_in_aggregate(bucket, key, started, awaitable):
    try:
        return await awaitable
    finally:
        _record_aggregate(bucket, key, started, time.time_ns())


def instrument_hookimpl(hook_name, plugin_name, function):
    """
    Wrap a single pluggy hookimpl so its execution is recorded as a
    `datasette.hook.<hook_name>` span.

    pluggy's multicall is synchronous: for an `async def` hookimpl,
    `pm.hook.foo(...)` returns immediately with a list of coroutines and the
    real work happens later, when Datasette awaits them through
    `await_me_maybe`. So for an awaitable result the coroutine itself is
    wrapped and the span covers the `await`, not the dispatch - otherwise
    every plugin doing I/O, exactly the ones worth measuring, would report 0ms.
    """
    function_name = getattr(function, "__name__", repr(function))
    span_name = "datasette.hook." + hook_name
    attributes = {
        "datasette.plugin": plugin_name,
        "code.function": function_name,
    }
    aggregate_key = (
        (hook_name, plugin_name, function_name)
        if hook_name in AGGREGATED_HOOKS
        else None
    )

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        if aggregate_key is not None:
            bucket = _hook_aggregate.get()
            if bucket is not None:
                started = time.time_ns()
                result = function(*args, **kwargs)
                if inspect.isawaitable(result):
                    return _await_in_aggregate(bucket, aggregate_key, started, result)
                _record_aggregate(bucket, aggregate_key, started, time.time_ns())
                return result

        span = tracer.start_span(span_name, attributes=attributes)
        if not span.get_span_context().is_valid:
            # No provider installed, so this is the shared INVALID_SPAN: there
            # is nothing to export and nothing worth making current. Skip the
            # context attach/detach entirely - this is the default path for
            # anyone running Datasette without OTel configured.
            return function(*args, **kwargs)
        try:
            with otel_trace.use_span(
                span,
                end_on_exit=False,
                record_exception=False,
                set_status_on_exception=False,
            ):
                result = function(*args, **kwargs)
        except BaseException as exception:
            _fail_span(span, exception)
            span.end()
            raise
        if inspect.isawaitable(result):
            return _await_in_span(span, result)
        span.end()
        return result

    wrapper.__datasette_hookimpl_wrapped__ = function
    return wrapper


def instrument_plugin_hookimpls(pm, plugin, plugin_name):
    """
    Wrap every hookimpl a just-registered plugin contributes.

    This runs *after* `PluginManager.register`, which is deliberate: pluggy
    introspects each implementation's signature to decide which arguments to
    pass, and freezes the result on the `HookImpl` as `argnames`. Replacing
    `hookimpl.function` afterwards leaves that binding untouched, so a plain
    `*args, **kwargs` wrapper is safe here in a way that wrapping before
    registration would not be.
    """
    for hook_caller in pm.get_hookcallers(plugin) or []:
        for hookimpl in hook_caller.get_hookimpls():
            if hookimpl.plugin is not plugin:
                continue
            # Generator-based hookwrappers have their own protocol; leave them be.
            if getattr(hookimpl, "hookwrapper", False) or getattr(
                hookimpl, "wrapper", False
            ):
                continue
            if hasattr(hookimpl.function, "__datasette_hookimpl_wrapped__"):
                continue
            hookimpl.function = instrument_hookimpl(
                hook_caller.name, plugin_name, hookimpl.function
            )


# --- Metrics --------------------------------------------------------------
#
# Spans answer "what happened during this request". They cannot answer "am I
# saturating my 3 SQL threads right now", because that is a gauge: a level
# sampled at collection time, not an event with a duration. It is also the
# single most useful operational question about a Datasette deployment, since
# num_sql_threads defaults to 3 and every read query in the process competes
# for those threads.
#
# Two shapes are used here:
#
#   Observable gauges - a callback the SDK invokes on its own collection
#   cycle. Nothing is computed unless something is collecting, so the default
#   no-provider install pays literally nothing for them.
#
#   Synchronous histograms/counters - recorded inline on the query path. These
#   survive trace sampling, which spans do not: an operator sampling 1% of
#   traces still gets 100% of the latency distribution and the interrupted
#   count.
#
# Note a real difference from tracing: `_ProxyMeter` and its instruments
# forward to a provider installed *after* they were created, whereas
# `ProxyTracer` permanently caches the concrete tracer it first resolves. So
# module-level instruments here are safe, and tests do not need a provider
# installed before this module is imported.


def _duration_attributes(database_name, operation):
    return {
        "db.system": "sqlite",
        "db.namespace": database_name,
        "datasette.operation": operation,
    }


sql_operation_duration = meter.create_histogram(
    "db.client.operation.duration",
    unit="s",
    description="Duration of a SQL operation issued by Datasette",
)

write_queue_wait = meter.create_histogram(
    "datasette.write.queue_wait",
    unit="s",
    description=(
        "Time a write spent queued behind the single write thread for its database"
    ),
)

queries_interrupted = meter.create_counter(
    "datasette.sql.queries.interrupted",
    unit="{query}",
    description=(
        "Queries cancelled for exceeding sql_time_limit_ms. Not derivable from "
        "spans under sampling, and the signal that a time limit is too tight"
    ),
)


@contextmanager
def record_operation_duration(database_name, operation):
    """
    Record `db.client.operation.duration` for one SQL operation.

    `error.type` is set from the exception class on failure, per semconv, so a
    latency distribution can be split by success and failure. For a
    `block=False` write this measures the enqueue, not the write - the same
    caveat that applies to the surrounding span.
    """
    attributes = _duration_attributes(database_name, operation)
    started = time.perf_counter()
    try:
        yield
    except BaseException as exception:
        attributes["error.type"] = type(exception).__qualname__
        raise
    finally:
        sql_operation_duration.record(time.perf_counter() - started, attributes)


def record_write_queue_wait(database_name, waited_ns):
    write_queue_wait.record(waited_ns / 1e9, {"db.namespace": database_name})


def record_query_interrupted(database_name):
    queries_interrupted.add(1, {"db.namespace": database_name})


# Live Datasette instances, weakly held so that instrumenting an instance
# never keeps it alive. Guarded by a lock because the gauge callbacks run on
# the SDK's collection thread while the event loop may be building or closing
# a Datasette.
#
# Known limitation: the pool gauges below carry no attribute identifying which
# Datasette produced them, so if a single process runs more than one instance
# their observations collide and last-one-wins. Production runs one instance
# per process; adding an instance id to make the test suite's hundreds of
# instances distinguishable would mean unbounded attribute cardinality in
# exchange for fixing a case that does not occur in production.
_live_datasettes = weakref.WeakSet()
_live_datasettes_lock = threading.Lock()


def register_datasette(ds):
    "Start reporting pool/queue gauges for this Datasette instance."
    with _live_datasettes_lock:
        _live_datasettes.add(ds)


def unregister_datasette(ds):
    "Stop reporting gauges for an instance that has been closed."
    with _live_datasettes_lock:
        _live_datasettes.discard(ds)


def _live_instances():
    with _live_datasettes_lock:
        return list(_live_datasettes)


def _databases_of(ds):
    """
    Every Database attached to an instance, including the internal database.

    The internal database is deliberately included: permission checks run SQL
    against it on essentially every request, so its queue depth and connection
    count are as operationally interesting as any user database's.
    """
    databases = list(ds.databases.values())
    internal = getattr(ds, "_internal_database", None)
    if internal is not None:
        databases.append(internal)
    return databases


# Each callback is a plain generator function so it can be unit-tested
# directly, without standing up an SDK provider and a metric reader.


def observe_sql_thread_limit(options=None):
    "Size of the shared read-query thread pool (the num_sql_threads setting)."
    for ds in _live_instances():
        if ds.executor is None:
            # num_sql_threads=0 - queries run on the event loop, no pool.
            continue
        yield otel_metrics.Observation(ds.setting("num_sql_threads"), {})


def observe_sql_thread_queue_depth(options=None):
    """
    Read queries waiting for a free thread in the shared pool.

    This is the saturation signal: sustained above zero means requests are
    queueing on num_sql_threads. `_work_queue` is a private attribute of
    ThreadPoolExecutor, so its absence is tolerated rather than fatal - a
    missing gauge is much better than a crashed collection cycle.
    """
    for ds in _live_instances():
        if ds.executor is None:
            continue
        work_queue = getattr(ds.executor, "_work_queue", None)
        if work_queue is None:
            continue
        yield otel_metrics.Observation(work_queue.qsize(), {})


def observe_pending_queries(options=None):
    """
    Read queries submitted to the pool and not yet finished, per database.

    Summed across databases and compared against the thread limit, this is the
    utilisation half of the saturation picture. `len()` is deliberately taken
    without `_pending_execute_futures_lock`: it is atomic, and taking a lock
    held on the request path from the collection thread would let telemetry
    add latency to queries.
    """
    for ds in _live_instances():
        for db in _databases_of(ds):
            yield otel_metrics.Observation(
                len(db._pending_execute_futures), {"db.namespace": db.name}
            )


def observe_write_queue_depth(options=None):
    """
    Writes queued behind the single write thread, per database.

    Every database serialises its writes through one thread, so this is
    unbounded backpressure that no amount of num_sql_threads will relieve.
    """
    for ds in _live_instances():
        for db in _databases_of(ds):
            write_queue = db._write_queue
            if write_queue is None:
                # No write has ever been queued for this database.
                continue
            yield otel_metrics.Observation(
                write_queue.qsize(), {"db.namespace": db.name}
            )


def observe_open_connections(options=None):
    "Open SQLite file connections tracked for closing, per database."
    for ds in _live_instances():
        for db in _databases_of(ds):
            yield otel_metrics.Observation(
                len(db._all_file_connections), {"db.namespace": db.name}
            )


sql_thread_limit_gauge = meter.create_observable_gauge(
    "datasette.sql.threads.limit",
    callbacks=[observe_sql_thread_limit],
    unit="{thread}",
    description="Maximum concurrent read queries (the num_sql_threads setting)",
)

sql_thread_queue_depth_gauge = meter.create_observable_gauge(
    "datasette.sql.threads.queue_depth",
    callbacks=[observe_sql_thread_queue_depth],
    unit="{query}",
    description="Read queries waiting for a free thread in the shared SQL pool",
)

pending_queries_gauge = meter.create_observable_gauge(
    "datasette.sql.queries.pending",
    callbacks=[observe_pending_queries],
    unit="{query}",
    description="Read queries submitted to the pool and not yet complete",
)

write_queue_depth_gauge = meter.create_observable_gauge(
    "datasette.write.queue_depth",
    callbacks=[observe_write_queue_depth],
    unit="{write}",
    description="Writes queued behind a database's single write thread",
)

open_connections_gauge = meter.create_observable_gauge(
    "datasette.connections.open",
    callbacks=[observe_open_connections],
    unit="{connection}",
    description="Open SQLite file connections tracked for closing",
)


# --- Response building ----------------------------------------------------
#
# Between "the SQL finished" and "the bytes went out" sits template rendering,
# facet calculation and CSV serialisation. Without spans for these, a slow
# page whose queries are all fast shows a trace full of quick db.query spans
# and a large unexplained gap - which is the single least useful shape a
# trace can have, because it tells you where the time is *not*.

template_render_duration = meter.create_histogram(
    "datasette.template.render.duration",
    unit="s",
    description="Time spent rendering a Jinja template into a response",
)

facet_duration = meter.create_histogram(
    "datasette.facet.duration",
    unit="s",
    description="Time spent calculating or suggesting one facet",
)

facets_timed_out = meter.create_counter(
    "datasette.facets.timed_out",
    unit="{facet}",
    description="Facet calculations abandoned for exceeding facet_time_limit_ms",
)

csv_rows_streamed = meter.create_counter(
    "datasette.csv.rows_streamed",
    unit="{row}",
    description="Rows written to a streaming CSV response",
)


def record_facets_timed_out(facet_type, count):
    if count:
        facets_timed_out.add(count, {"datasette.facet_type": facet_type})


def record_csv_rows(rows):
    if rows:
        csv_rows_streamed.add(rows)


async def traced_facet(span_name, facet_type, phase, awaitable):
    """
    Wrap one facet calculation in a span and time it.

    The coroutine is wrapped rather than the dispatch, for the same reason as
    plugin hookimpls: the facets are built as a list of coroutines and awaited
    later by `run_sequential`, so timing the construction would measure
    nothing.
    """
    attributes = {"datasette.facet_type": facet_type}
    started = time.perf_counter()
    try:
        with tracer.start_as_current_span(span_name, attributes=attributes):
            return await awaitable
    finally:
        facet_duration.record(
            time.perf_counter() - started,
            {**attributes, "datasette.facet_phase": phase},
        )
