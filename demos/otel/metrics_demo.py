"""
Saturate Datasette's SQL thread pool and print the metrics that show it.

Run it with no arguments and no infrastructure:

    uv run python demos/otel/metrics_demo.py

It builds a small database in a temporary directory, registers a deliberately
slow SQL function, installs an in-memory OpenTelemetry SDK metric reader, then
fires more concurrent queries than there are threads in the pool while
sampling the gauges in the background.

The point is the question spans cannot answer. A trace tells you a query took
170ms; it does not tell you that 130ms of that was spent waiting for one of
only three threads, because "how many threads are busy right now" is a level
rather than an event. That is what `datasette.sql.threads.queue_depth` reports.

Datasette core never installs a `MeterProvider` - the block near the top of
this file is doing the job that `opentelemetry-instrument` would normally do.
Core only emits, and whoever runs Datasette decides where it goes.
"""

import asyncio
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
except ImportError:
    sys.exit(
        "This demo needs the OpenTelemetry SDK:\n"
        "    pip install opentelemetry-sdk\n"
        "(Datasette itself only depends on opentelemetry-api.)"
    )

reader = InMemoryMetricReader()
otel_metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

from datasette import hookimpl
from datasette.app import Datasette
from datasette.plugins import pm

NUM_SQL_THREADS = 3
CONCURRENT_QUERIES = 12
QUERY_MS = 40


class SlowQueryPlugin:
    "Registers a SQL function that sleeps, so queries occupy a pool thread."

    __name__ = "slow-query-plugin"

    @hookimpl
    def prepare_connection(self, conn):
        conn.create_function("slow_ms", 1, lambda ms: time.sleep(ms / 1000) or ms)


def build_database(path):
    conn = sqlite3.connect(path)
    conn.execute("create table t (id integer primary key)")
    conn.executemany("insert into t (id) values (?)", [[i] for i in range(100)])
    conn.commit()
    conn.close()


def collect():
    "One collection cycle -> {(metric name, attributes tuple): data point}."
    points = {}
    data = reader.get_metrics_data()
    if data is None:
        return points
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                for point in metric.data.data_points:
                    key = (metric.name, tuple(sorted((point.attributes or {}).items())))
                    points[key] = point
    return points


async def sample_during_load(stop, peaks):
    "Poll the gauges while the load is running and keep the highest seen."
    while not stop.is_set():
        points = collect()
        for name in (
            "datasette.sql.threads.queue_depth",
            "datasette.sql.queries.pending",
        ):
            for (metric_name, attributes), point in points.items():
                if metric_name != name:
                    continue
                current = getattr(point, "value", 0)
                key = (name, attributes)
                if current > peaks.get(key, -1):
                    peaks[key] = current
        await asyncio.sleep(0.002)


def format_attributes(attributes):
    if not attributes:
        return ""
    return "  {" + ", ".join(f"{k}={v}" for k, v in attributes) + "}"


async def main():
    tmpdir = Path(tempfile.mkdtemp())
    db_path = tmpdir / "demo.db"
    build_database(db_path)

    pm.register(SlowQueryPlugin(), name="slow-query-plugin")
    ds = Datasette([str(db_path)], settings={"num_sql_threads": NUM_SQL_THREADS})
    await ds.invoke_startup()
    db = ds.get_database("demo")

    # Warm up so connection setup and schema introspection do not land in the
    # middle of the measurement.
    await db.execute("select 1")

    print(
        f"num_sql_threads = {NUM_SQL_THREADS}, "
        f"firing {CONCURRENT_QUERIES} concurrent {QUERY_MS}ms queries\n"
    )

    peaks = {}
    stop = asyncio.Event()
    sampler = asyncio.ensure_future(sample_during_load(stop, peaks))

    started = time.perf_counter()
    await asyncio.gather(
        *[db.execute(f"select slow_ms({QUERY_MS})") for _ in range(CONCURRENT_QUERIES)]
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    stop.set()
    await sampler

    # A write, so the write-queue metrics have something to report.
    await db.execute_write("create table if not exists written (id integer)")

    serial_ms = CONCURRENT_QUERIES * QUERY_MS
    ideal_ms = serial_ms / NUM_SQL_THREADS
    print(f"wall clock                     : {elapsed_ms:.0f}ms")
    print(f"  if fully serialised          : {serial_ms}ms")
    print(f"  with {NUM_SQL_THREADS} threads perfectly used : {ideal_ms:.0f}ms\n")

    print("Peak values sampled while the queries were in flight:\n")
    for (name, attributes), peak in sorted(peaks.items()):
        print(f"  {name}{format_attributes(attributes)} = {peak}")

    print("\nFinal collection:\n")
    points = collect()
    for (name, attributes), point in sorted(points.items()):
        if hasattr(point, "value"):
            rendered = str(point.value)
        else:
            rendered = (
                f"count={point.count} sum={point.sum:.4f}s "
                f"min={point.min:.4f}s max={point.max:.4f}s"
            )
        print(f"  {name}{format_attributes(attributes)}")
        print(f"      {rendered}")

    print(
        "\nThe queue_depth peak is the number of queries that were sitting "
        "\nwaiting for a thread. Raising num_sql_threads is what moves it."
    )
    ds.close()


if __name__ == "__main__":
    asyncio.run(main())
