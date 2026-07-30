# OpenTelemetry demo

Datasette core depends on `opentelemetry-api` only. It emits spans and nothing else — it never
creates a `TracerProvider`, never configures an exporter, and never sets a sampler. With no SDK
installed every span is a no-op and costs approximately nothing.

That means "turning tracing on" is entirely the job of whoever runs Datasette. This directory shows
two ways to do it, neither of which needs any external service.

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
datasette.hook.render_cell                               1.44ms  |  #                                     |
      datasette.plugin = demo-plugin
      datasette.hook.call_count = 800
...
datasette.hook.extra_body_script                        51.11ms  |       ############################### |
      datasette.plugin = demo-plugin

208 spans total
  db.query                    : 44
  datasette.hook.*            : 105
  datasette.permission_check  : 15

Note: render_cell was dispatched 800 times (100 rows x 8 columns) but produced 1 span, not 800.
```

Three things worth looking at in that output:

- **The deliberately slow plugin is obvious.** `demo-plugin`'s `extra_body_script` sleeps for 50ms
  and owns the waterfall. It is an `async def` implementation, so the span covers the `await` rather
  than pluggy's synchronous dispatch — which is the only reason it reads as 51ms instead of 0ms.
- **`render_cell` is aggregated.** It was dispatched once per cell, 800 times, but produces a single
  span carrying `datasette.hook.call_count`. One span per dispatch would overflow
  `BatchSpanProcessor`'s default 2048-entry queue on a single page and silently drop everything
  else.
- **Permission checks repeat.** 15 `datasette.permission_check` spans for one page, each fanning out
  to `permission_resources_sql` hook calls. Repeated sibling spans are collapsed to `xN` lines by
  the demo's printer, otherwise they bury the rest of the trace. This is meant to look repetitive.

Writes are not exercised here because this is a read-only page. Hitting a write endpoint adds
`db.write.queue_wait` and `db.write.execute` spans as children of `db.query`, which is how you tell
"the write was slow" apart from "the write waited 490ms behind another writer".

## 2. Real Datasette, spans on your terminal

This is the replacement for the removed `?_trace=1`:

```bash
pip install opentelemetry-distro opentelemetry-instrumentation

OTEL_TRACES_EXPORTER=console OTEL_METRICS_EXPORTER=none OTEL_LOGS_EXPORTER=none \
  opentelemetry-instrument datasette mydb.db
```

Then make a request and watch stdout.

Two things that will otherwise look like bugs:

- **`opentelemetry-instrument` is required.** Setting `OTEL_TRACES_EXPORTER` and running plain
  `datasette` produces nothing at all. That environment variable is read by the SDK's
  auto-configuration, which only runs under the agent — and core never installs a provider itself.
- **Output is not instant.** The default `BatchSpanProcessor` flushes roughly every 10 seconds.

## 3. Sending it somewhere real

Any OTLP-compatible backend works, via the standard agent — Datasette needs no configuration of its
own:

```bash
pip install opentelemetry-distro opentelemetry-exporter-otlp opentelemetry-instrumentation-asgi

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  opentelemetry-instrument datasette mydb.db
```

Adding `opentelemetry-instrumentation-asgi` is worthwhile: it creates the per-request root span that
Datasette's spans then nest underneath. Without it you get correctly nested Datasette spans, but no
enclosing HTTP span.

This third path is not exercised by anything in this directory and has not been verified end to end
against a running collector — it is the standard OpenTelemetry agent contract, documented here for
completeness.

## Privacy

`db.query.text` **is** recorded, truncated to 2048 characters. SQL **parameter values are never
recorded** — only `datasette.param_count`. Actor identifiers are never recorded — only
`datasette.actor_present`.

On a public Datasette instance the SQL text is user-supplied. If you export to a third-party vendor,
that text leaves your infrastructure.

See the `internals_telemetry` section of the Datasette documentation for the full span reference.
