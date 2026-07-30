"""
The single source of truth for every span, span attribute and metric that
Datasette core emits.

Three things read this module, which is the point of it existing:

1. **The instrumentation itself.** `Attribute` and `SpanName` subclass `str`,
   so a registry entry *is* the string OpenTelemetry wants. Call sites pass
   `DB_NAMESPACE` where they used to pass `"db.namespace"` - no wrapper API
   over the OTel calls, no parallel structure to keep in step, and a typo is
   now an `ImportError` instead of a silently misnamed attribute.

2. **The documentation.** `docs/telemetry_doc.py` renders the span and metric
   reference in `docs/internals.rst` from these definitions using cog, and
   `cog --check` runs in CI - so the docs cannot drift from the code.

3. **A conformance test.** `tests/test_telemetry_registry.py` makes real
   requests, collects every span and attribute actually emitted, and compares
   both directions: emitted-but-unregistered catches instrumentation added
   without documentation, registered-but-never-emitted catches documentation
   describing something that no longer exists. Neither the type system nor
   the generated docs can catch that second case.
"""


class Attribute(str):
    """
    A span or metric attribute key, carrying its own documentation.

    Subclasses `str` so it can be handed straight to `set_attribute()`.
    """

    __slots__ = ("description", "optional")

    def __new__(cls, name, description, optional=False):
        self = super().__new__(cls, name)
        self.description = description
        self.optional = optional
        return self

    def __repr__(self):
        return f"Attribute({str(self)!r})"


class SpanName(str):
    "A span name, carrying its documentation and the attributes it may set."

    __slots__ = ("attributes", "description", "prefix")

    def __new__(cls, name, description, attributes=(), prefix=False):
        self = super().__new__(cls, name)
        self.description = description
        self.attributes = tuple(attributes)
        # True for `datasette.hook.` - emitted names have a variable suffix,
        # so the conformance test matches by prefix rather than equality.
        self.prefix = prefix
        return self

    def __repr__(self):
        return f"SpanName({str(self)!r})"


class MetricName(str):
    "A metric name, carrying its instrument kind, unit and attributes."

    __slots__ = ("attributes", "description", "kind", "unit")

    def __new__(cls, name, kind, unit, description, attributes=()):
        self = super().__new__(cls, name)
        self.kind = kind
        self.unit = unit
        self.description = description
        self.attributes = tuple(attributes)
        return self

    def __repr__(self):
        return f"MetricName({str(self)!r})"


COUNTER = "Counter"
HISTOGRAM = "Histogram"
GAUGE = "Observable gauge"


# --- Attributes -----------------------------------------------------------
#
# Shared attributes are defined once and referenced by every span that sets
# them, so "which spans carry db.namespace?" is answerable by grep.

DB_SYSTEM = Attribute("db.system", "Always ``sqlite``.")
DB_NAMESPACE = Attribute("db.namespace", "Name of the database being queried.")
DB_QUERY_TEXT = Attribute(
    "db.query.text",
    "The SQL, truncated to 2048 characters. Never the parameter values.",
)
DB_QUERY_PARAMETER = Attribute(
    "db.query.parameter.<name>",
    "Value of one bound SQL parameter. Only present when the "
    ":ref:`setting_trace_sql_parameters` setting is enabled - off by default.",
    optional=True,
)

PARAM_COUNT = Attribute(
    "datasette.param_count",
    "Number of bound parameters. Recorded instead of the values themselves.",
    optional=True,
)
TIME_LIMIT_MS = Attribute(
    "datasette.time_limit_ms",
    "The :ref:`setting_sql_time_limit_ms` value this query ran under.",
    optional=True,
)
ROWS_RETURNED = Attribute(
    "datasette.rows_returned", "Number of rows the operation returned.", optional=True
)
TRUNCATED = Attribute(
    "datasette.truncated",
    "True if the result was cut short by :ref:`setting_max_returned_rows`.",
    optional=True,
)
INTERRUPTED = Attribute(
    "datasette.interrupted",
    "True if the query was cancelled for exceeding the time limit. The span "
    "status is also set to ``ERROR``.",
    optional=True,
)
SQL_ERROR_SUPPRESSED = Attribute(
    "datasette.sql_error_suppressed",
    "True when the query failed but the caller passed ``log_sql_errors=False``, "
    "meaning it was probing and treats failure as an expected answer. Facet "
    "suggestion does this against every column.",
    optional=True,
)
EXECUTESCRIPT = Attribute(
    "datasette.executescript",
    "True for ``execute_write_script()``, which runs multiple statements.",
    optional=True,
)
EXECUTEMANY = Attribute(
    "datasette.executemany",
    "True for ``execute_write_many()``. Parameter values are never recorded for "
    "this call, whose parameter sequence can hold thousands of rows.",
    optional=True,
)
ISOLATED_CONNECTION = Attribute(
    "datasette.isolated_connection",
    "True if the write ran on its own connection rather than the shared write "
    "connection.",
)
TRANSACTION = Attribute(
    "datasette.transaction",
    "False for statements such as ``VACUUM`` that cannot run inside a transaction.",
)

PLUGIN = Attribute(
    "datasette.plugin", "Name of the plugin providing the implementation."
)
CODE_FUNCTION = Attribute("code.function", "Name of the implementation function.")
HOOK_CALL_COUNT = Attribute(
    "datasette.hook.call_count",
    "Number of dispatches represented by an aggregated span.",
    optional=True,
)
HOOK_TOTAL_DURATION_MS = Attribute(
    "datasette.hook.total_duration_ms",
    "Wall time actually spent inside the hook, summed across every dispatch. "
    "This is also the aggregate span's own duration - deliberately, so that a "
    "hook dispatched at spread-out points does not appear as one long span "
    "covering everything that happened between its first and last call.",
    optional=True,
)
HOOK_WINDOW_MS = Attribute(
    "datasette.hook.window_ms",
    "First dispatch to last, including everything Datasette did in between. "
    "Deliberately not the span's duration - see ``datasette.hook.total_duration_ms``.",
    optional=True,
)
HOOK_AGGREGATED = Attribute(
    "datasette.hook.aggregated",
    "True on a span that represents many dispatches rather than one.",
    optional=True,
)

ACTION = Attribute(
    "datasette.action", "The permission action being checked.", optional=True
)
ACTIONS = Attribute(
    "datasette.actions",
    "The permission actions requested, when more than one was checked at once.",
    optional=True,
)
RESOURCE = Attribute(
    "datasette.resource",
    "The resource being checked, as ``parent`` or ``parent/child``. Omitted when "
    "there is no resource.",
    optional=True,
)
ACTOR_PRESENT = Attribute(
    "datasette.actor_present",
    "Whether an actor was supplied. **The actor itself is never recorded.**",
)
PERMISSION_CACHED = Attribute(
    "datasette.permission.cached",
    "True if every requested action was already resolved in this request's "
    "permission cache, meaning the span represents no work.",
)
PERMISSION_RESULT = Attribute(
    "datasette.result",
    "The verdict, for a single-action check. Omitted for multi-action checks, "
    "where one attribute per action would be unbounded cardinality.",
    optional=True,
)
RESOURCES_RETURNED = Attribute(
    "datasette.resources_returned", "Number of resources the actor may access."
)

TEMPLATE = Attribute(
    "datasette.template",
    "The selected template name, or ``<string>`` for a template built with "
    "``from_string()``.",
)
VIEW_NAME = Attribute(
    "datasette.view_name", "The view that requested the render.", optional=True
)
FACET_TYPE = Attribute(
    "datasette.facet_type", "The facet class type: ``column``, ``array`` or ``date``."
)
FACET_PHASE = Attribute(
    "datasette.facet_phase", "``results`` for calculation, ``suggest`` for suggestion."
)
FORMAT = Attribute("datasette.format", "The output format, such as ``json``.")
TABLE = Attribute(
    "datasette.table",
    "The table being exported. Omitted for query pages.",
    optional=True,
)
ROWS_WRITTEN = Attribute("datasette.rows_written", "Rows written to the response.")
PAGES_FETCHED = Attribute(
    "datasette.pages_fetched",
    "Pages of results fetched. More than one only when streaming.",
)
STREAM = Attribute(
    "datasette.stream",
    "True for a full ``?_stream=1`` export rather than a single page.",
)

OPERATION = Attribute("datasette.operation", "``read`` or ``write``.")
ERROR_TYPE = Attribute(
    "error.type",
    "The exception class name. Present only when the operation failed.",
    optional=True,
)


# --- Spans ----------------------------------------------------------------

DB_QUERY = SpanName(
    "db.query",
    "A SQL operation issued by Datasette, covering the full round trip "
    "including any time spent queued for a thread.",
    (
        DB_SYSTEM,
        DB_NAMESPACE,
        DB_QUERY_TEXT,
        DB_QUERY_PARAMETER,
        PARAM_COUNT,
        TIME_LIMIT_MS,
        ROWS_RETURNED,
        TRUNCATED,
        INTERRUPTED,
        SQL_ERROR_SUPPRESSED,
        EXECUTESCRIPT,
        EXECUTEMANY,
    ),
)

DB_QUERY_EXECUTE = SpanName(
    "db.query.execute",
    "The read executing inside a SQL worker thread. Child of ``db.query``; the "
    "gap between the two is time spent waiting for a thread.",
)

DB_WRITE_QUEUE_WAIT = SpanName(
    "db.write.queue_wait",
    "Time a write spent waiting in its database's write queue before the write "
    "thread picked it up. Child of ``db.query``.",
)

DB_WRITE_EXECUTE = SpanName(
    "db.write.execute",
    "The write executing on the write thread. Child of ``db.query``.",
    (ISOLATED_CONNECTION, TRANSACTION),
)

HOOK = SpanName(
    "datasette.hook.",
    "One plugin hook implementation running, named for the hook - for example "
    "``datasette.hook.extra_body_script``. For an ``async def`` implementation "
    "the span covers the ``await``, not pluggy's synchronous dispatch. A few "
    "very high-frequency hooks - currently ``render_cell`` and "
    "``permission_resources_sql`` - are aggregated instead, producing one span "
    "per plugin per request carrying ``datasette.hook.call_count`` rather than "
    "one span per dispatch. Aggregation applies only inside a request; the same "
    "hooks dispatched from CLI or plugin code still get a span each.",
    (
        PLUGIN,
        CODE_FUNCTION,
        HOOK_CALL_COUNT,
        HOOK_TOTAL_DURATION_MS,
        HOOK_WINDOW_MS,
        HOOK_AGGREGATED,
    ),
    prefix=True,
)

PERMISSION_CHECK = SpanName(
    "datasette.permission_check",
    "A permission decision, covering the whole batch when several actions are "
    "resolved together.",
    (ACTION, ACTIONS, RESOURCE, ACTOR_PRESENT, PERMISSION_CACHED, PERMISSION_RESULT),
)

ALLOWED_RESOURCES = SpanName(
    "datasette.allowed_resources",
    "The bulk resource-listing path - which tables or databases may this actor " "see.",
    (ACTION, RESOURCE, ACTOR_PRESENT, RESOURCES_RETURNED),
)

PERMISSION_RESOURCES_SQL = SpanName(
    "datasette.permission_resources_sql",
    "The SQL-building and hook-gathering phase nested inside "
    "``datasette.allowed_resources``.",
    (ACTION, RESOURCE, ACTOR_PRESENT),
)

RENDER_TEMPLATE = SpanName(
    "datasette.render_template",
    "Rendering a Jinja template into an HTML response. Covers template "
    "selection and context building as well as the render, so the plugin hooks "
    "awaited while building the context nest inside it.",
    (TEMPLATE, VIEW_NAME),
)

FACET_RESULTS = SpanName(
    "datasette.facet_results",
    "One facet class calculating its results. The SQL it runs nests inside it.",
    (FACET_TYPE,),
)

FACET_SUGGEST = SpanName(
    "datasette.facet_suggest",
    "One facet class deciding which facets to suggest. Often more expensive "
    "than the facets themselves, because suggestion probes every column.",
    (FACET_TYPE,),
)

RENDER = SpanName(
    "datasette.render",
    "Dispatch to an output renderer. CSV is not included here, since it streams.",
    (FORMAT, ROWS_RETURNED),
)

CSV_STREAM = SpanName(
    "datasette.csv_stream",
    "Writing a CSV response body. Runs *after* the view has returned, while the "
    "body is being sent, and for a ``?_stream=1`` export is where nearly all of "
    "the request's time goes.",
    (STREAM, DB_NAMESPACE, TABLE, ROWS_WRITTEN, PAGES_FETCHED),
)

SPANS = (
    DB_QUERY,
    DB_QUERY_EXECUTE,
    DB_WRITE_QUEUE_WAIT,
    DB_WRITE_EXECUTE,
    HOOK,
    PERMISSION_CHECK,
    ALLOWED_RESOURCES,
    PERMISSION_RESOURCES_SQL,
    RENDER_TEMPLATE,
    FACET_RESULTS,
    FACET_SUGGEST,
    RENDER,
    CSV_STREAM,
)


# --- Metrics --------------------------------------------------------------

M_OPERATION_DURATION = MetricName(
    "db.client.operation.duration",
    HISTOGRAM,
    "s",
    "Duration of a SQL operation. The standard OpenTelemetry semantic "
    "convention metric, and the one that survives trace sampling.",
    (DB_SYSTEM, DB_NAMESPACE, OPERATION, ERROR_TYPE),
)

M_WRITE_QUEUE_WAIT = MetricName(
    "datasette.write.queue_wait",
    HISTOGRAM,
    "s",
    "Time each write waited in its database's write queue. The metric "
    "counterpart of the ``db.write.queue_wait`` span.",
    (DB_NAMESPACE,),
)

M_QUERIES_INTERRUPTED = MetricName(
    "datasette.sql.queries.interrupted",
    COUNTER,
    "{query}",
    "Queries cancelled for exceeding :ref:`setting_sql_time_limit_ms`. Worth "
    "alerting on: a rising rate means the limit is too tight or a table has "
    "outgrown its queries.",
    (DB_NAMESPACE,),
)

M_THREADS_LIMIT = MetricName(
    "datasette.sql.threads.limit",
    GAUGE,
    "{thread}",
    "Maximum concurrent read queries - the :ref:`setting_num_sql_threads` "
    "value. Not reported when ``num_sql_threads`` is ``0``, since then queries "
    "run on the event loop and there is no pool.",
)

M_THREADS_QUEUE_DEPTH = MetricName(
    "datasette.sql.threads.queue_depth",
    GAUGE,
    "{query}",
    "Read queries waiting for a free thread. **This is the saturation "
    "signal** - sustained above zero means requests are queueing on "
    "``num_sql_threads``.",
)

M_QUERIES_PENDING = MetricName(
    "datasette.sql.queries.pending",
    GAUGE,
    "{query}",
    "Read queries submitted to the pool and not yet complete. Summed across "
    "databases and compared against the thread limit, this is pool "
    "utilisation.",
    (DB_NAMESPACE,),
)

M_WRITE_QUEUE_DEPTH = MetricName(
    "datasette.write.queue_depth",
    GAUGE,
    "{write}",
    "Writes queued behind a database's single write thread. Backpressure that "
    "raising ``num_sql_threads`` cannot relieve. Not reported for a database "
    "that has never been written to.",
    (DB_NAMESPACE,),
)

M_CONNECTIONS_OPEN = MetricName(
    "datasette.connections.open",
    GAUGE,
    "{connection}",
    "Open SQLite file connections currently tracked for closing.",
    (DB_NAMESPACE,),
)

M_TEMPLATE_RENDER_DURATION = MetricName(
    "datasette.template.render.duration",
    HISTOGRAM,
    "s",
    "Time spent rendering templates. Template names are bounded by the "
    "templates that exist, so this is safe to break down by. Note that a "
    "template rendering a slot nests, so the inner render is counted twice.",
    (TEMPLATE,),
)

M_FACET_DURATION = MetricName(
    "datasette.facet.duration",
    HISTOGRAM,
    "s",
    "Time spent on one facet. Suggestion and calculation have very different "
    "costs and are worth separating.",
    (FACET_TYPE, FACET_PHASE),
)

M_FACETS_TIMED_OUT = MetricName(
    "datasette.facets.timed_out",
    COUNTER,
    "{facet}",
    "Facet calculations abandoned for exceeding "
    ":ref:`setting_facet_time_limit_ms`. A timed-out facet is dropped from the "
    "page silently, so without this the failure is invisible.",
    (FACET_TYPE,),
)

M_CSV_ROWS_STREAMED = MetricName(
    "datasette.csv.rows_streamed",
    COUNTER,
    "{row}",
    "Rows written to CSV responses. The throughput measure for exports.",
)

METRICS = (
    M_OPERATION_DURATION,
    M_WRITE_QUEUE_WAIT,
    M_QUERIES_INTERRUPTED,
    M_THREADS_LIMIT,
    M_THREADS_QUEUE_DEPTH,
    M_QUERIES_PENDING,
    M_WRITE_QUEUE_DEPTH,
    M_CONNECTIONS_OPEN,
    M_TEMPLATE_RENDER_DURATION,
    M_FACET_DURATION,
    M_FACETS_TIMED_OUT,
    M_CSV_ROWS_STREAMED,
)


def span_for(emitted_name):
    """
    Resolve an emitted span name to its registry entry, or None.

    Handles the `datasette.hook.<name>` family, whose emitted names carry a
    suffix that is not knowable in advance.
    """
    for span in SPANS:
        if span.prefix:
            if emitted_name.startswith(span):
                return span
        elif emitted_name == span:
            return span
    return None


def attribute_allowed(span, emitted_key):
    """
    Whether `emitted_key` is a registered attribute of `span`.

    `db.query.parameter.<name>` is matched by prefix: the suffix is the SQL
    parameter's own name, which is arbitrary.
    """
    for attribute in span.attributes:
        if attribute.endswith("<name>"):
            if emitted_key.startswith(attribute[: -len("<name>")]):
                return True
        elif emitted_key == attribute:
            return True
    return False
