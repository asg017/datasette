# OpenTelemetry demo

Datasette core depends on `opentelemetry-api` only. It emits spans and metrics and nothing else — it
never creates a `TracerProvider` or a `MeterProvider`, never configures an exporter, and never sets a
sampler. With no SDK installed every span and every instrument is a no-op and costs approximately
nothing.

That means "turning telemetry on" is entirely the job of whoever runs Datasette. This directory shows
several ways to do it, **none of which needs Docker** — including a real OTLP export over the wire,
and Jaeger driven from its own binary.

| | |
|---|---|
| `trace_demo.py` | Self-contained span waterfall. No network, no agent |
| `metrics_demo.py` | Saturates the SQL thread pool and prints the gauges |
| `otlp_receiver.py` | A ~120 line OTLP/HTTP receiver, to verify a real export |
| `plugins/otel_asgi.py` | Gives each request a root span — needed for any trace UI |

## 1. A span waterfall, in about ten seconds

```bash
uv run python demos/otel/trace_demo.py
```

No containers, no collector, no network. The script installs an in-memory SDK exporter — doing the
job `opentelemetry-instrument` would normally do — builds a 100 row × 8 column table in a temporary
directory, makes one request through the real ASGI stack, and prints the spans as a tree.

Abridged real output:

```
GET /demo/wide -> 200

db.query                                                 0.17ms  |#                                       |
      db.query.text = select 1 from sqlite_master where type='table' and name=?
      datasette.rows_returned = 1
  db.query.execute                                       0.06ms  |#                                       |
datasette.permission_check                               0.37ms  |#                                       |
      datasette.action = view-table
  datasette.hook.permission_resources_sql  x6            0.22ms  |#                                       |
        datasette.plugin = datasette.default_permissions
        (collapsed, summed duration)
  db.query                                               0.15ms  |#                                       |
        db.query.text = WITH a0_rules AS ( SELECT parent, child, allow, reason,...
    db.query.execute                                     0.08ms  |#                                       |
...
datasette.facet_suggest                                  3.97ms  |  ##                                    |
      datasette.facet_type = column
  db.query  x10                                          3.82ms  |  ##                                    |
...
datasette.hook.render_cell                               1.44ms  |  #                                     |
      datasette.plugin = demo-plugin
      datasette.hook.call_count = 800
...
datasette.render_template                               57.89ms  |       ################################ |
      datasette.template = table.html
  datasette.hook.extra_body_script                      51.09ms  |       ############################     |
        datasette.plugin = demo-plugin

125 spans total
  db.query                    : 44
  datasette.hook.*            : 15
  datasette.permission_check  : 15
  datasette.facet_*           : 6
  datasette.render_template   : 1

Template rendering took 57.9ms of this request. Before that
span existed it was an unexplained gap between the last query
and the response.

Note: render_cell was dispatched 800 times (100 rows x 8 columns) but produced 1 span, not 800.
```

Five things worth looking at in that output:

- **The deliberately slow plugin is obvious, and it is nested where it belongs.** `demo-plugin`'s
  `extra_body_script` sleeps for 50ms and owns the waterfall. It is an `async def` implementation, so
  the span covers the `await` rather than pluggy's synchronous dispatch — which is the only reason it
  reads as 51ms instead of 0ms. It appears *inside* `datasette.render_template`, because that is
  genuinely when it runs: the hook is awaited while the template context is being built.
- **`datasette.render_template` accounts for the tail of the request.** 57.9ms of a request whose
  queries total a couple of milliseconds. Without this span that time is a gap in the trace, which is
  the least useful shape a trace can have — it tells you where the time is *not*.
- **`render_cell` is aggregated.** It was dispatched once per cell, 800 times, but produces a single
  span carrying `datasette.hook.call_count`. One span per dispatch would overflow
  `BatchSpanProcessor`'s default 2048-entry queue on a single page and silently drop everything
  else.
- **Permission checks repeat.** 15 `datasette.permission_check` spans for one page. Repeated sibling
  spans are collapsed to `xN` lines by the demo's printer, otherwise they bury the rest of the trace.
  This is meant to look repetitive — it is the N+1 shape, and `datasette.permission.cached = true`
  marks the 11 of those 15 that were served from cache and did no work at all.
- **`permission_resources_sql` is aggregated too.** It is dispatched 16 times per page per
  implementing plugin — 96 dispatches, which used to be 96 spans and 45% of this trace. It does no
  I/O (it returns SQL *fragments* that are concatenated into 3 real queries), so per-dispatch spans
  were measuring the instrumentation. Now it is 6 spans, one per implementation, carrying
  `datasette.hook.call_count = 16`.

Note the `datasette.facet_suggest` span with ten queries under it: facet *suggestion* probes every
column and is frequently more expensive than calculating the facets you actually asked for. That is
what `?_nosuggest=1` and the `suggest_facets` setting turn off.

Writes are not exercised here because this is a read-only page. Hitting a write endpoint adds
`db.write.queue_wait` and `db.write.execute` spans as children of `db.query`, which is how you tell
"the write was slow" apart from "the write waited 490ms behind another writer".

## 2. The question spans cannot answer

```bash
uv run python demos/otel/metrics_demo.py
```

A trace tells you a query took 170ms. It does not tell you that 130ms of that was spent waiting for
one of only three threads — "how many threads are busy right now" is a level, not an event, so no
span can carry it. That is what metrics are for.

The script registers a SQL function that sleeps, fires 12 concurrent 40ms queries at a pool of 3
threads, and samples the gauges while they are in flight. Real output:

```
num_sql_threads = 3, firing 12 concurrent 40ms queries

wall clock                     : 170ms
  if fully serialised          : 480ms
  with 3 threads perfectly used : 160ms

Peak values sampled while the queries were in flight:

  datasette.sql.queries.pending  {db.namespace=demo} = 12
  datasette.sql.threads.queue_depth = 9

Final collection:

  datasette.sql.threads.limit
      3
  db.client.operation.duration  {datasette.operation=read, db.namespace=demo, db.system=sqlite}
      count=16 sum=1.2605s min=0.0001s max=0.1695s
  datasette.write.queue_wait  {db.namespace=demo}
      count=1 sum=0.0002s min=0.0002s max=0.0002s
```

`queue_depth = 9` is the whole point: 12 queries, 3 threads, 9 of them sitting in a queue. Sustained
above zero in production means requests are backing up on `num_sql_threads`, and no amount of
reading traces would have told you that.

Note also `max=0.1695s` on the duration histogram against a query whose actual work is 40ms. The
gap is queue time. The two numbers together — 170ms observed, 40ms of work — are what distinguishes
"my queries are slow" from "my pool is too small".

## 3. Real Datasette, spans on your terminal

This is the replacement for the removed `?_trace=1`:

```bash
pip install opentelemetry-distro opentelemetry-instrumentation

OTEL_TRACES_EXPORTER=console OTEL_METRICS_EXPORTER=none OTEL_LOGS_EXPORTER=none \
  opentelemetry-instrument datasette mydb.db
```

Then make a request and watch stdout.

Add `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS=datasette` and each span's `resource` block carries
Datasette's own `service.version` (and a `service.name` default of `datasette`) without typing
either by hand:

```bash
pip install opentelemetry-distro opentelemetry-instrumentation

OTEL_TRACES_EXPORTER=console OTEL_METRICS_EXPORTER=none OTEL_LOGS_EXPORTER=none \
OTEL_EXPERIMENTAL_RESOURCE_DETECTORS=datasette \
OTEL_SERVICE_NAME=datasette \
  opentelemetry-instrument datasette mydb.db
```

Real `resource` block from that run:

```json
"resource": {
    "attributes": {
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "1.44.0",
        "service.name": "datasette",
        "service.version": "1.0a37",
        "service.instance.id": "98bcc4b9-6589-4240-9236-5bb67c498fa4",
        "telemetry.auto.version": "0.65b0"
    },
    "schema_url": ""
}
```

`OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` is required, not optional decoration: as of the pinned
`opentelemetry-sdk`, resource detection takes a fast path that skips entry-point scanning
entirely unless this variable names a detector (or `*`) - registering the entry point makes the
detector discoverable, it does not make it run. And `OTEL_SERVICE_NAME=datasette` above is what
you'd normally set anyway (see below); leave it out and `service.name` still comes from
Datasette's detector, since it is only a default, not a value that fights an operator's explicit
configuration.

Two things that will otherwise look like bugs:

- **`opentelemetry-instrument` is required.** Setting `OTEL_TRACES_EXPORTER` and running plain
  `datasette` produces nothing at all. That environment variable is read by the SDK's
  auto-configuration, which only runs under the agent — and core never installs a provider itself.
- **Output is not instant.** The default `BatchSpanProcessor` flushes roughly every 10 seconds.

## 4. A real OTLP export, still with no Docker

The console exporter proves spans exist, but not that they survive a real export. `otlp_receiver.py`
is a ~120 line OTLP/HTTP receiver that closes that gap — real protobuf, over the wire, no collector
and no containers.

Terminal 1:

```bash
uv run --with opentelemetry-proto python demos/otel/otlp_receiver.py
```

Terminal 2:

```bash
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=none \
OTEL_LOGS_EXPORTER=none \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_SERVICE_NAME=datasette \
OTEL_BSP_SCHEDULE_DELAY=1000 \
  uv run --with opentelemetry-exporter-otlp-proto-http \
    opentelemetry-instrument datasette mydb.db
```

Load a page, wait ~10s for the batch flush, then Ctrl-C the receiver. Real output from one request
against a 200-row table:

```
received 35 spans (35 total)
received 186 spans (221 total)

=== 221 spans ===
  datasette.hook.permission_resources_sql         96       8.32ms total
  db.query                                        43      21.57ms total
  db.query.execute                                42       9.94ms total
  datasette.permission_check                      15       4.83ms total
  db.write.queue_wait                              3       0.61ms total
  db.write.execute                                 3       5.82ms total
  ...

slowest spans:
       5.47ms  db.write.execute
       3.23ms  db.query
       2.26ms  datasette.permission_check
```

Note `db.write.queue_wait` and `db.write.execute` appearing separately — that is the distinction
between "the write was slow" and "the write waited behind another writer".

The receiver is a debugging aid, not a backend: nothing is persisted, it speaks OTLP/HTTP only (not
gRPC), and it ignores metrics and logs.

## 5. Jaeger, with the binary rather than Docker

Jaeger ingests OTLP directly, so nothing extra is needed between it and Datasette. Assuming the
Jaeger binary is on your PATH:

**Terminal 1 — Jaeger.**

```bash
jaeger                                          # Jaeger v2
COLLECTOR_OTLP_ENABLED=true jaeger-all-in-one   # Jaeger v1
```

Both listen on **16686** (UI), **4317** (OTLP/gRPC) and **4318** (OTLP/HTTP). Do not run
`otlp_receiver.py` at the same time — it binds 4318 as well, and Jaeger replaces it here.

**Terminal 2 — Datasette**, with the `otel_asgi` plugin from `plugins/` so requests get a root span:

```bash
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=none \
OTEL_LOGS_EXPORTER=none \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_SERVICE_NAME=datasette \
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=demo \
OTEL_BSP_SCHEDULE_DELAY=1000 \
  uv run --with opentelemetry-exporter-otlp-proto-http \
         --with opentelemetry-instrumentation-asgi \
    opentelemetry-instrument datasette mydb.db \
      --plugins-dir demos/otel/plugins -p 8001
```

Two of those are worth calling out:

- **`OTEL_SERVICE_NAME=datasette`** is what you pick from Jaeger's Service dropdown. Leave it out
  and the SDK defaults to `unknown_service:<executable>`, which is where a mysterious
  "unknown service" entry in the dropdown comes from.
- **`OTEL_BSP_SCHEDULE_DELAY=1000`** drops the batch flush from its ~10 second default to ~1 second.
  Measured: all 225 spans of a request arrive within a second instead of after ten. For a demo this
  is the difference between "it works" and "it looks broken". Do not use it in production — it
  trades export efficiency for latency.

**Terminal 3 — make a request**, then wait about a second:

```bash
curl -s -o /dev/null http://localhost:8001/mydb/sometable
```

**Verify:** open <http://localhost:16686>, choose service `datasette`, click Find Traces. You want
the trace named `GET /mydb/sometable` — roughly 190 spans:

- `db.query` with `db.query.execute` nested inside it — that nesting crosses a thread boundary
- `datasette.hook.*`, one per plugin hook implementation
- `datasette.permission_check`, each fanning out to `permission_resources_sql`
- on a write endpoint, `db.write.queue_wait` and `db.write.execute` as siblings

### Reading the result

**Two tiny orphan traces, alongside the one real trace.** Startup work — `register_events`,
`register_actions`, `register_column_types`, the internal catalog's `db.query`/`db.write.execute`
spans, and the `prepare_connection` warm-up each of those triggers the first time a database is
touched — runs from `invoke_startup()` before any request exists, and now shares one
`datasette.startup` trace rather than producing a separate orphan per hook and per query.

Measured on one startup plus one table-page request, with this plugin loaded — **27 distinct trace
IDs before, 3 after**:

```
before: 27 traces                        after: 3 traces
   1 : GET /orphans/t          98 spans     1 : GET /orphans/t          98 spans
  13 : db.query                             1 : datasette.startup       43 spans
   9 : db.write.* / write_wrapper           1 : datasette.hook.asgi_wrapper
   3 : datasette.hook.register_*
   1 : datasette.hook.asgi_wrapper
```

The one remaining orphan is dispatched before `invoke_startup()` ever runs — `asgi_wrapper`, from
`.app()` — along with `register_output_renderer` from `Datasette.__init__` if you count from process
start. Both are left alone rather than folded into a span held open across object construction.
`tests/test_telemetry.py` pins the `datasette.startup` behaviour.

**`db.query` spans are `SpanKind.CLIENT`**, which is what semantic conventions say a database client
span should be, so a trace UI can render them as database calls rather than as internal work. Their
children — `db.query.execute`, `db.write.execute`, `db.write.queue_wait` — stay `INTERNAL`: they are
Datasette's decomposition of one logical query, and marking them CLIENT too would make a single
query look like several database calls.

### Why the plugin is needed

Without `plugins/otel_asgi.py` everything still gets traced, but every span is a root span. Measured
on one request: **75 root spans without it, 1 with it** (plus the startup spans above). A trace UI is
close to unusable in the first case.

## 6. Sending it to a real backend

Any OTLP-compatible backend works through the same agent — Datasette needs no configuration of its
own. Point the endpoint at your collector instead of the demo receiver:

```bash
pip install opentelemetry-distro opentelemetry-exporter-otlp opentelemetry-instrumentation-asgi

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_SERVICE_NAME=datasette-fec \
OTEL_METRICS_EXPORTER=none \
OTEL_LOGS_EXPORTER=none \
  uv run \
  --with opentelemetry-distro \
  --with opentelemetry-exporter-otlp \
  --with opentelemetry-instrumentation-asgi \
  --with datasette-libfec \
  opentelemetry-instrument datasette fec.db --plugins-dir demos/otel/plugins
```

**Always set `OTEL_SERVICE_NAME`.** Without it the SDK falls back to
`unknown_service:<executable>`, and your traces land under a service by that name rather than under
`datasette`.

**Always set `OTEL_METRICS_EXPORTER=none` and `OTEL_LOGS_EXPORTER=none`** unless your backend really
does accept all three signals. `opentelemetry-distro` defaults every signal to OTLP, so the agent
will try to ship metrics and logs alongside traces. Jaeger only implements the traces service, so
you get a stream of:

```
Failed to export metrics to localhost:4317, error code: StatusCode.UNIMPLEMENTED,
error details: unknown service opentelemetry.proto.collector.metrics.v1.MetricsService
```

Tracing still works throughout — it is the metrics pipeline failing, not yours — but the noise
buries anything useful.

**But `OTEL_METRICS_EXPORTER=none` also throws away Datasette's own metrics.** If you want the thread
pool gauges from section 2 in production, send metrics somewhere that accepts them rather than
turning the exporter off. A Prometheus scrape endpoint needs no collector at all:

```bash
OTEL_SERVICE_NAME=datasette \
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=prometheus \
OTEL_LOGS_EXPORTER=none \
OTEL_EXPORTER_PROMETHEUS_PORT=9464 \
  uv run --with opentelemetry-exporter-prometheus \
         --with opentelemetry-exporter-otlp-proto-http \
         --with opentelemetry-instrumentation-asgi \
    opentelemetry-instrument datasette mydb.db --plugins-dir demos/otel/plugins
```

Then `curl http://localhost:9464/metrics` and look for `datasette_sql_threads_queue_depth`.

The `otel_asgi` plugin is worth loading here too: it creates the per-request root span that
Datasette's spans nest underneath. Without it the nesting among Datasette's own spans is still
correct, but there is no enclosing HTTP span, so a trace UI shows dozens of unrelated single-span
traces per page instead of one request.

This last path is the standard OpenTelemetry agent contract, documented for completeness; it has not
been verified here against a specific vendor's collector.

## 7. Exemplars: linking a metric spike to a trace

Once both signals are on — an SDK tracer provider as well as a metrics one, which is what the agent
in sections 4 to 6 installs — each histogram measurement also carries the trace of the request that
produced it, with no extra configuration on Datasette's side. The SDK attaches the current trace ID
and span ID to any measurement recorded inside a sampled span, and every metric on the query path is
recorded inside one. (The metrics demo in section 2 installs no tracer provider, so it shows none of
this.) Four queries of increasing cost, each in its own span, produced one exemplar per query on
`db.client.operation.duration`:

```
db.client.operation.duration count=4
  exemplars: 4
    value=0.001564s trace_id=ddfaf45fd4e14913497d7efeac95f381 span_id=fd5792bdbb01e533
    value=0.006320s trace_id=34aea775ade11a3c5f716695731000fe span_id=25ed9e29dd84dbee
    value=0.045253s trace_id=a65cb58d1460a179f0d04046ff51ed0d span_id=7f34d6378c85d062
    value=0.305240s trace_id=6089f4c515c221c0ca7bb53667b37ac8 span_id=0516f4a6641eaa0b
```

Exemplars are kept one per histogram bucket, so bucket boundaries decide how many distinct traces a
metric can point at. The same four queries, run against an earlier set of bucket boundaries under
which all four fell into a single `(0, 5]` second bucket, produced one exemplar instead of four —
fixing the boundaries changed more than the quantiles, it also multiplied the traces reachable from
this metric.

**The pinned `opentelemetry-exporter-prometheus` (`0.65b0`) does not emit exemplars at all** — the
word `exemplar` does not appear anywhere in its source, and rendering the workload above through
that exporter in the OpenMetrics format — the only exposition format that can carry an exemplar —
produced zero exemplar markers.
The OTLP exporter carries exemplars through unchanged, so if they need to reach Prometheus, route
them through an OTLP collector rather than through Datasette's own Prometheus exporter. On that path,
the Prometheus server needs `--enable-feature=exemplar-storage` and a scrape in the OpenMetrics
format — its default text format has no syntax for exemplars — and Grafana needs the Prometheus data
source's exemplar configuration (`exemplarTraceIdDestinations`) pointed at a tracing data source
before it draws one as a clickable point. See the `internals_telemetry` section of the main docs for
both, with links to the primary sources.

An exemplar only exists for a trace that was sampled. With the tracer provider's sampler set to
`ALWAYS_OFF`, the same workload produced `exemplars: 0` on every data point rather than a link to a
trace that was never kept — at low sampling rates most measurements carry no exemplar, but the ones
that do always resolve to a real trace.

## Privacy

`db.query.text` **is** recorded on spans, truncated to 2048 characters. SQL **parameter values are
not recorded by default** — only `datasette.param_count`. Actor identifiers are never recorded on
spans — only `datasette.actor_present`.

Parameter values can be turned on with the `trace_sql_parameters` setting, which defaults to `off`.
Read that setting's documentation before enabling it: permission checks bind the actor as a SQL
parameter, and canned queries using `_cookie_*` or `_header_*` magic parameters can bind session
cookies and `Authorization` headers as ordinary parameters.

Metrics carry no SQL text, no parameter values and no actor information. Their only non-numeric
attribute is `db.namespace`, the database name.

On a public Datasette instance the SQL text is user-supplied. If you export to a third-party vendor,
that text leaves your infrastructure.

See the `internals_telemetry` section of the Datasette documentation for the full span and metric
reference.
