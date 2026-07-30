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
import re
import threading
import time
import weakref
from contextlib import contextmanager
from contextvars import ContextVar

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace as otel_trace
from opentelemetry.trace import Status, StatusCode

from .telemetry_registry import (
    CODE_FUNCTION,
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_QUERY_PARAMETER,
    DB_SYSTEM,
    ERROR_TYPE,
    FACET_PHASE,
    FACET_TYPE,
    HOOK,
    HOOK_AGGREGATED,
    HOOK_CALL_COUNT,
    HOOK_TOTAL_DURATION_MS,
    HOOK_WINDOW_MS,
    M_CONNECTIONS_OPEN,
    M_CSV_ROWS_STREAMED,
    M_FACET_DURATION,
    M_FACETS_TIMED_OUT,
    M_OPERATION_DURATION,
    M_QUERIES_INTERRUPTED,
    M_QUERIES_PENDING,
    M_TEMPLATE_RENDER_DURATION,
    M_THREADS_LIMIT,
    M_THREADS_QUEUE_DEPTH,
    M_WRITE_QUEUE_DEPTH,
    M_WRITE_QUEUE_WAIT,
    OPERATION,
    PLUGIN,
)
from .version import __version__

# A schema URL is a machine-readable claim about which semantic-convention
# version the names on this scope's spans and metrics match. A consumer
# doing schema translation uses it to replay the renames between that
# version and whatever version it wants, so the claim has to hold exactly -
# a wrong one makes translation wrong rather than merely uninformative.
#
# Every OTel-governed name emitted below is the current spelling as of
# semconv 1.29.0: db.system, db.namespace, db.query.text,
# db.query.parameter.<key>, db.client.operation.duration, error.type,
# code.function. Two of those were renamed in semconv 1.30.0 - db.system to
# db.system.name, and code.function to code.function.name - and this module
# still emits the pre-1.30.0 spelling of both. Declaring the latest schema
# would therefore be a false claim about names we do not emit, and would
# stop a consumer from translating db.system and code.function forward to
# what they are now called; declaring 1.29.0 keeps the claim true for every
# name actually on the wire. Everything under datasette.* is Datasette's own
# and outside semconv, so it is unaffected by this URL either way.
#
# Bump this deliberately, together with the attribute renames it implies -
# it is a claim about the names, not decoration.
SCHEMA_URL = "https://opentelemetry.io/schemas/1.29.0"

tracer = otel_trace.get_tracer("datasette", __version__, schema_url=SCHEMA_URL)
meter = otel_metrics.get_meter("datasette", __version__, schema_url=SCHEMA_URL)

MAX_SQL_LENGTH = 2048


def sql_attribute(sql: str) -> str:
    "Truncate SQL text so it is safe to attach to a span as an attribute."
    sql = sql.strip()
    if len(sql) <= MAX_SQL_LENGTH:
        return sql
    return sql[:MAX_SQL_LENGTH] + "…[truncated]"


# db.operation.name is a fixed allowlist rather than "whatever token comes
# first", on purpose. This runs against arbitrary user-supplied SQL (the
# `?sql=` query string, canned queries, values typed into the query editor),
# and this attribute is also a candidate dimension on the db.client.operation
# .duration metric - see M_OPERATION_DURATION's registry entry. A metric
# series is keyed by its attribute values, so echoing an arbitrary first word
# back as an attribute would let one visitor's typo or made-up statement
# mint a new, permanent metric series. The allowlist bounds that at a fixed,
# small set of names regardless of what anyone sends.
#
# It is deliberately *not* a SQL parser: no comment stripping, no handling of
# a leading "(" before a UNIONed SELECT, no compound names like "CREATE
# TABLE". Every one of those is a place a hand-rolled matcher would start
# accreting special cases and eventually get one wrong - the ticket this
# implements is explicit that parsing is where this goes wrong, and that
# omitting a name beats guessing at one. A statement this cannot recognise
# gets no attribute at all instead of a wrong one.
DB_OPERATION_ALLOWLIST = frozenset(
    {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "PRAGMA",
        "VACUUM",
        "WITH",
        "EXPLAIN",
        "BEGIN",
        "COMMIT",
    }
)

_LEADING_KEYWORD = re.compile(r"^\s*([A-Za-z]+)")


def sql_operation_name(sql: str) -> str | None:
    """
    The statement's leading keyword, if it is one of a small recognised set.

    Returns None - never a guess - for anything not on the allowlist,
    including a statement that starts with whitespace-only content, a
    comment, or punctuation such as the "(" of a parenthesised SELECT. Only
    safe to call with a single statement: `execute_write_script()` runs
    several separated by semicolons, so per semantic conventions' guidance on
    `db.operation.name` ("SHOULD NOT be extracted from db.query.text, when
    the database system supports query text with multiple operations in
    non-batch operations") that call site does not use this at all rather
    than reporting only the first statement's operation.
    """
    match = _LEADING_KEYWORD.match(sql)
    if not match:
        return None
    keyword = match.group(1).upper()
    if keyword in DB_OPERATION_ALLOWLIST:
        return keyword
    return None


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
        yield DB_QUERY_PARAMETER.replace("<name>", str(key)), _parameter_value(value)


# Hooks dispatched so often that one span per dispatch is worse than useless.
# These are counted and timed into a single aggregate span per (hook, plugin)
# pair per request instead, carrying datasette.hook.call_count and
# datasette.hook.total_duration_ms.
#
# render_cell is dispatched from inside per-row/per-cell loops. A single
# 100 row x 15 column table page dispatches it ~1,500 times; one span per
# dispatch would blow straight through BatchSpanProcessor's default
# max_queue_size of 2048 and silently drop every other span in the request.
#
# permission_resources_sql is dispatched once per unique action per
# implementing plugin. Measured on a table page: 16 gathers x 6 core hookimpls
# = 96 spans, 45% of the whole trace. Two things make aggregating it right
# rather than merely convenient:
#
#   - The hook does no I/O. It returns PermissionSQL *fragments*, which are
#     concatenated into one CTE per action; those 96 dispatches produce 3
#     actual SQL executions, and 42 of them produce none at all because every
#     implementation returned None and the caller short-circuits to
#     default-deny. There is no latency hiding in an individual dispatch.
#   - The span costs more than the work it measures. Measured per dispatch:
#     0.029us raw, 8.09us wrapped with a span, 0.425us aggregated. Across 96
#     dispatches that is ~0.74ms of span overhead for ~0.03ms of hook time.
#     Note this does *not* show up end to end: 0.74ms is ~2% of a 32ms table
#     page, well inside the +/-3ms run-to-run spread, and a before/after
#     benchmark could not resolve it. The honest claim is trace quality, not
#     speed.
#
# What is lost is the parent link to the individual permission_check - the
# aggregate hangs off the request instead. For a hook that does no I/O that is
# not worth 90 spans. Per-plugin attribution survives, so a third-party
# permission plugin that *does* do I/O here still shows up, in
# total_duration_ms; if one ever does, un-aggregate it.
AGGREGATED_HOOKS = frozenset({"render_cell", "permission_resources_sql"})

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
    """
    Emit one span per (hook, plugin) for the dispatches accumulated this
    request.

    **The span's duration is the summed time spent inside the hook, not the
    window from first dispatch to last.** Those are wildly different numbers
    for a hook dispatched at spread-out points, and using the window makes a
    trace actively lie: permission_resources_sql fires at the start of a
    request and again during template rendering, so its window is ~43ms while
    the hook itself consumes ~0.04ms. A span drawn 1000x wider than its work
    lands at the top of every "slowest spans" list, which is the first thing
    anyone looks at when a page is slow.

    An earlier version used first_start -> last_end and recorded the real
    figure only in an attribute. That was wrong: nobody reads an attribute to
    sanity-check a bar that is already sorted to the top. The window is still
    available as datasette.hook.window_ms for anyone who wants it.

    The start time is kept at the first dispatch, so the span sits where the
    work began. It is one contiguous bar standing in for N scattered ones, so
    the position is honest and only the contiguity is a simplification.
    """
    for (hook_name, plugin_name, function_name), stats in bucket.items():
        span = tracer.start_span(
            HOOK + hook_name,
            start_time=stats["first_start"],
            attributes={
                PLUGIN: plugin_name,
                CODE_FUNCTION: function_name,
                HOOK_CALL_COUNT: stats["count"],
                HOOK_TOTAL_DURATION_MS: stats["total_ns"] / 1e6,
                # First dispatch to last, including everything Datasette did
                # in between. Recorded because "these calls were spread across
                # the whole request" is real information - it just must not be
                # the span's duration.
                HOOK_WINDOW_MS: (stats["last_end"] - stats["first_start"]) / 1e6,
                HOOK_AGGREGATED: True,
            },
        )
        span.end(end_time=stats["first_start"] + stats["total_ns"])


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


async def _await_in_span(span_name, attributes, awaitable):
    """
    Await a hookimpl's coroutine, starting its span only once this coroutine
    itself actually runs.

    The span is *not* started by the caller before this coroutine is handed
    back - see the comment in `instrument_hookimpl`. Starting it here instead
    means a coroutine that is constructed but never awaited (because the
    dispatch loop that requested it short-circuited on an earlier plugin)
    never starts a span at all, rather than starting one that then never
    gets ended.
    """
    span = tracer.start_span(span_name, attributes=attributes)
    if not span.get_span_context().is_valid:
        # No provider installed, so this is the shared INVALID_SPAN: nothing
        # to attach, nothing to end.
        return await awaitable
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

    That wrapping coroutine does not start its span until it is actually
    awaited - see `_await_in_span`. Some dispatch loops stop awaiting once a
    plugin returns a non-`None` result: `render_cell` breaks out of its loop
    in `views/table.py`, `views/database.py` and `views/table_extras.py`, so
    a coroutine returned from here can be discarded rather than awaited.
    Starting the span eagerly, before that is known, would leave it started
    and never ended - and an unended span is never exported, silently.
    Deferring the start means a discarded coroutine starts no span at all.
    """
    function_name = getattr(function, "__name__", repr(function))
    span_name = HOOK + hook_name
    attributes = {
        PLUGIN: plugin_name,
        CODE_FUNCTION: function_name,
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

        # Whether the result is awaitable is not knowable until the function
        # has been called - the documented Datasette idiom is a *sync*
        # hookimpl returning an `async def inner()` coroutine, whose body has
        # not run yet. So the call is timed rather than spanned, and the span
        # is created from those timestamps once the answer is known:
        #
        #   - a plain value or an exception: both timestamps are already in
        #     hand, so the span is started and ended in the same breath and
        #     there is no window in which it could be abandoned;
        #   - an awaitable: span creation is handed to `_await_in_span`,
        #     which runs only if something actually awaits the coroutine.
        #
        # The cost of that is nesting for a *fully synchronous* hookimpl:
        # a span it creates during its own execution now attaches to the
        # surrounding span rather than to its dispatch span. No hookimpl in
        # this codebase does that - the async ones, which are the ones doing
        # work worth nesting, still run inside their span, in
        # `_await_in_span`.
        started = time.time_ns()
        try:
            result = function(*args, **kwargs)
        except BaseException as exception:
            span = tracer.start_span(
                span_name, start_time=started, attributes=attributes
            )
            # Every call here is a no-op on the INVALID_SPAN handed out when
            # no provider is installed, so that case needs no branch.
            _fail_span(span, exception)
            span.end(end_time=time.time_ns())
            raise
        if inspect.isawaitable(result):
            return _await_in_span(span_name, attributes, result)
        span = tracer.start_span(span_name, start_time=started, attributes=attributes)
        span.end(end_time=time.time_ns())
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


def _duration_attributes(database_name, operation, operation_name=None):
    attributes = {
        DB_SYSTEM: "sqlite",
        DB_NAMESPACE: database_name,
        OPERATION: operation,
    }
    if operation_name is not None:
        # Bounded to DB_OPERATION_ALLOWLIST - see the cardinality note on
        # M_OPERATION_DURATION in telemetry_registry.py. None (the caller
        # either has no recognised keyword or, for execute_write_script(),
        # never asked) leaves the key off entirely rather than recording it
        # as an empty or sentinel value.
        attributes[DB_OPERATION_NAME] = operation_name
    return attributes


sql_operation_duration = meter.create_histogram(
    M_OPERATION_DURATION,
    unit=M_OPERATION_DURATION.unit,
    description="Duration of a SQL operation issued by Datasette",
    explicit_bucket_boundaries_advisory=M_OPERATION_DURATION.buckets,
)

write_queue_wait = meter.create_histogram(
    M_WRITE_QUEUE_WAIT,
    unit=M_WRITE_QUEUE_WAIT.unit,
    description=(
        "Time a write spent queued behind the single write thread for its database"
    ),
    explicit_bucket_boundaries_advisory=M_WRITE_QUEUE_WAIT.buckets,
)

queries_interrupted = meter.create_counter(
    M_QUERIES_INTERRUPTED,
    unit=M_QUERIES_INTERRUPTED.unit,
    description=(
        "Queries cancelled for exceeding sql_time_limit_ms. Not derivable from "
        "spans under sampling, and the signal that a time limit is too tight"
    ),
)


@contextmanager
def record_operation_duration(database_name, operation, operation_name=None):
    """
    Record `db.client.operation.duration` for one SQL operation.

    `error.type` is set from the exception class on failure, per semconv, so a
    latency distribution can be split by success and failure. For a
    `block=False` write this measures the enqueue, not the write - the same
    caveat that applies to the surrounding span. `operation_name`, when given,
    is the same allowlisted `db.operation.name` value set on the span - see
    `sql_operation_name()` above and the cardinality note on
    `M_OPERATION_DURATION` in telemetry_registry.py for why it is safe here
    but `db.collection.name` is not.
    """
    attributes = _duration_attributes(database_name, operation, operation_name)
    started = time.perf_counter()
    try:
        yield
    except BaseException as exception:
        attributes[ERROR_TYPE] = type(exception).__qualname__
        raise
    finally:
        sql_operation_duration.record(time.perf_counter() - started, attributes)


def record_write_queue_wait(database_name, waited_ns):
    write_queue_wait.record(waited_ns / 1e9, {DB_NAMESPACE: database_name})


def record_query_interrupted(database_name):
    queries_interrupted.add(1, {DB_NAMESPACE: database_name})


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
                len(db._pending_execute_futures), {DB_NAMESPACE: db.name}
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
            yield otel_metrics.Observation(write_queue.qsize(), {DB_NAMESPACE: db.name})


def observe_open_connections(options=None):
    "Open SQLite file connections tracked for closing, per database."
    for ds in _live_instances():
        for db in _databases_of(ds):
            yield otel_metrics.Observation(
                len(db._all_file_connections), {DB_NAMESPACE: db.name}
            )


sql_thread_limit_gauge = meter.create_observable_gauge(
    M_THREADS_LIMIT,
    callbacks=[observe_sql_thread_limit],
    unit=M_THREADS_LIMIT.unit,
    description="Maximum concurrent read queries (the num_sql_threads setting)",
)

sql_thread_queue_depth_gauge = meter.create_observable_gauge(
    M_THREADS_QUEUE_DEPTH,
    callbacks=[observe_sql_thread_queue_depth],
    unit=M_THREADS_QUEUE_DEPTH.unit,
    description="Read queries waiting for a free thread in the shared SQL pool",
)

pending_queries_gauge = meter.create_observable_gauge(
    M_QUERIES_PENDING,
    callbacks=[observe_pending_queries],
    unit=M_QUERIES_PENDING.unit,
    description="Read queries submitted to the pool and not yet complete",
)

write_queue_depth_gauge = meter.create_observable_gauge(
    M_WRITE_QUEUE_DEPTH,
    callbacks=[observe_write_queue_depth],
    unit=M_WRITE_QUEUE_DEPTH.unit,
    description="Writes queued behind a database's single write thread",
)

open_connections_gauge = meter.create_observable_gauge(
    M_CONNECTIONS_OPEN,
    callbacks=[observe_open_connections],
    unit=M_CONNECTIONS_OPEN.unit,
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
    M_TEMPLATE_RENDER_DURATION,
    unit=M_TEMPLATE_RENDER_DURATION.unit,
    description="Time spent rendering a Jinja template into a response",
    explicit_bucket_boundaries_advisory=M_TEMPLATE_RENDER_DURATION.buckets,
)

facet_duration = meter.create_histogram(
    M_FACET_DURATION,
    unit=M_FACET_DURATION.unit,
    description="Time spent calculating or suggesting one facet",
    explicit_bucket_boundaries_advisory=M_FACET_DURATION.buckets,
)

facets_timed_out = meter.create_counter(
    M_FACETS_TIMED_OUT,
    unit=M_FACETS_TIMED_OUT.unit,
    description="Facet calculations abandoned for exceeding facet_time_limit_ms",
)

csv_rows_streamed = meter.create_counter(
    M_CSV_ROWS_STREAMED,
    unit=M_CSV_ROWS_STREAMED.unit,
    description="Rows written to a streaming CSV response",
)


def record_facets_timed_out(facet_type, count):
    if count:
        facets_timed_out.add(count, {FACET_TYPE: facet_type})


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
    attributes = {FACET_TYPE: facet_type}
    started = time.perf_counter()
    try:
        with tracer.start_as_current_span(span_name, attributes=attributes):
            return await awaitable
    finally:
        facet_duration.record(
            time.perf_counter() - started,
            {**attributes, FACET_PHASE: phase},
        )
