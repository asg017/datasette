"""
OpenTelemetry integration for Datasette core.

Core depends on `opentelemetry-api` only. It never creates a
`TracerProvider`, never configures an exporter, and never touches
sampling - that is the responsibility of whoever is running Datasette
(an `opentelemetry-instrument` agent, a future plugin, or a test
harness). With no provider installed every span produced here is a
`NonRecordingSpan` and costs approximately nothing.
"""

import functools
import inspect
import time
from contextlib import contextmanager
from contextvars import ContextVar

from opentelemetry import trace as otel_trace
from opentelemetry.trace import Status, StatusCode

tracer = otel_trace.get_tracer("datasette")

MAX_SQL_LENGTH = 2048


def sql_attribute(sql: str) -> str:
    "Truncate SQL text so it is safe to attach to a span as an attribute."
    sql = sql.strip()
    if len(sql) <= MAX_SQL_LENGTH:
        return sql
    return sql[:MAX_SQL_LENGTH] + "…[truncated]"


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
