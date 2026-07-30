"""
Print a span waterfall for a single Datasette request.

Run it with no arguments and no infrastructure:

    uv run python demos/otel/trace_demo.py

It builds a small database in a temporary directory, installs an in-memory
OpenTelemetry SDK exporter, makes one request through the real ASGI stack and
prints the resulting spans as a tree.

Datasette core never installs a `TracerProvider` - the block at the top of this
file is doing the job that `opentelemetry-instrument` would normally do for
you. That is the whole point: core only emits spans, and whoever runs Datasette
decides where they go.
"""

import asyncio
import sqlite3
import sys
import tempfile
from pathlib import Path

try:
    from opentelemetry import trace as otel_trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
except ImportError:
    sys.exit(
        "This demo needs the OpenTelemetry SDK:\n"
        "    pip install opentelemetry-sdk\n"
        "(Datasette itself only depends on opentelemetry-api.)"
    )

# Must happen before any span is started: the module-level tracer in
# datasette.telemetry resolves its concrete tracer once and caches it.
exporter = InMemorySpanExporter()
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(exporter))
otel_trace.set_tracer_provider(provider)

from datasette import hookimpl
from datasette.app import Datasette
from datasette.plugins import pm

ROWS = 100
COLUMNS = 8


class DemoPlugin:
    "A plugin that is deliberately slow, so it shows up in the waterfall."

    __name__ = "demo-plugin"

    @hookimpl
    def extra_body_script(self):
        async def inner():
            await asyncio.sleep(0.05)
            return ""

        return inner()

    @hookimpl
    def render_cell(self, value):
        # Dispatched once per cell - aggregated into a single span
        return None


def build_database(path):
    conn = sqlite3.connect(path)
    columns = ", ".join(f"c{i} text" for i in range(COLUMNS))
    conn.execute(f"create table wide (id integer primary key, {columns})")
    conn.executemany(
        "insert into wide values ({})".format(", ".join("?" * (COLUMNS + 1))),
        [[row] + [f"r{row}c{col}" for col in range(COLUMNS)] for row in range(ROWS)],
    )
    conn.commit()
    conn.close()


def print_tree(spans):
    children = {}
    roots = []
    by_id = {span.context.span_id: span for span in spans}
    for span in spans:
        parent = span.parent.span_id if span.parent is not None else None
        if parent is None or parent not in by_id:
            roots.append(span)
        else:
            children.setdefault(parent, []).append(span)

    if not spans:
        print("no spans captured")
        return

    origin = min(span.start_time for span in spans)
    total = max(span.end_time for span in spans) - origin or 1

    INTERESTING = (
        "db.query.text",
        "datasette.plugin",
        "datasette.action",
        "datasette.hook.call_count",
        "datasette.rows_returned",
        "datasette.template",
        "datasette.facet_type",
        "datasette.format",
        "datasette.rows_written",
    )

    def bar_for(start, end):
        offset = int(40 * (start - origin) / total)
        width = max(1, int(40 * (end - start) / total))
        return " " * offset + "#" * width

    def render(span, depth):
        duration_ms = (span.end_time - span.start_time) / 1e6
        label = "  " * depth + span.name
        bar = bar_for(span.start_time, span.end_time)
        print(f"{label:<52}{duration_ms:>9.2f}ms  |{bar:<40}|")
        for key in INTERESTING:
            value = (span.attributes or {}).get(key)
            if value is not None:
                text = str(value).replace("\n", " ")
                if len(text) > 58:
                    text = text[:55] + "..."
                print(f"{'  ' * depth}      {key} = {text}")
        render_children(span.context.span_id, depth + 1)

    def render_children(span_id, depth):
        siblings = sorted(children.get(span_id, []), key=lambda s: s.start_time)
        index = 0
        while index < len(siblings):
            run = [siblings[index]]
            while (
                index + len(run) < len(siblings)
                and siblings[index + len(run)].name == siblings[index].name
            ):
                run.append(siblings[index + len(run)])
            index += len(run)
            # Collapse long runs of identically-named siblings - otherwise a
            # single page's permission checks bury everything else. The count
            # is the interesting part anyway.
            if len(run) > 2:
                spent = sum(s.end_time - s.start_time for s in run) / 1e6
                label = f"{'  ' * depth}{run[0].name}  x{len(run)}"
                bar = bar_for(run[0].start_time, run[-1].end_time)
                print(f"{label:<52}{spent:>9.2f}ms  |{bar:<40}|")
                plugins = sorted(
                    {
                        str(s.attributes.get("datasette.plugin"))
                        for s in run
                        if (s.attributes or {}).get("datasette.plugin")
                    }
                )
                if plugins:
                    print(
                        f"{'  ' * depth}      datasette.plugin = {', '.join(plugins)}"
                    )
                print(f"{'  ' * depth}      (collapsed, summed duration)")
            else:
                for span in run:
                    render(span, depth)

    for root in sorted(roots, key=lambda s: s.start_time):
        render(root, 0)


async def main():
    tmpdir = Path(tempfile.mkdtemp())
    db_path = tmpdir / "demo.db"
    build_database(db_path)

    pm.register(DemoPlugin(), name="demo-plugin")
    ds = Datasette([str(db_path)])
    await ds.invoke_startup()

    # Warm up: first request pays for schema introspection and connection
    # setup, which would drown out everything else in the waterfall.
    await ds.client.get("/demo/wide")

    exporter.clear()
    response = await ds.client.get("/demo/wide")
    print(f"GET /demo/wide -> {response.status_code}\n")

    spans = exporter.get_finished_spans()
    print_tree(spans)

    names = [span.name for span in spans]
    print(f"\n{len(spans)} spans total")
    print(f"  db.query                    : {names.count('db.query')}")
    print(
        f"  datasette.hook.*            : {sum(1 for n in names if n.startswith('datasette.hook.'))}"
    )
    print(
        f"  datasette.permission_check  : {names.count('datasette.permission_check')}"
    )
    print(
        f"  datasette.facet_*           : {sum(1 for n in names if n.startswith('datasette.facet_'))}"
    )
    print(f"  datasette.render_template   : {names.count('datasette.render_template')}")
    render_template = [
        span for span in spans if span.name == "datasette.render_template"
    ]
    if render_template:
        ms = (render_template[0].end_time - render_template[0].start_time) / 1e6
        print(
            f"\nTemplate rendering took {ms:.1f}ms of this request. Before that "
            f"\nspan existed it was an unexplained gap between the last query "
            f"\nand the response."
        )
    render_cell = [span for span in spans if span.name == "datasette.hook.render_cell"]
    if render_cell:
        count = render_cell[0].attributes.get("datasette.hook.call_count")
        print(
            f"\nNote: render_cell was dispatched {count} times "
            f"({ROWS} rows x {COLUMNS} columns) but produced "
            f"{len(render_cell)} span, not {count}."
        )


if __name__ == "__main__":
    asyncio.run(main())
