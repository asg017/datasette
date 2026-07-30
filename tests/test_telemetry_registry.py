"""
Two-way conformance between `datasette/telemetry_registry.py` and what
Datasette actually emits.

This is the test that makes the generated documentation trustworthy. cog
guarantees the docs match the registry; this guarantees the registry matches
the code. Without it, both could agree with each other and be wrong.

It checks both directions, and the second one is the one nothing else catches:

- **emitted but not registered** - instrumentation was added without
  documenting it, so the reference page silently omits it.
- **registered but never emitted** - the reference page describes a span or
  attribute that no longer exists, which is worse than omitting it, because a
  reader will build a dashboard on it.
"""

import itertools

import pytest
import pytest_asyncio

pytest.importorskip("opentelemetry.sdk")

import sqlite_utils

from datasette import telemetry_registry as reg
from datasette.app import Datasette
from datasette.utils.sqlite import sqlite3

# Named in-memory databases are shared-cache, so two Datasette instances using
# the same name share one SQLite database - and the second `create table`
# fails. Every workload below therefore gets its own name.
_names = itertools.count()


def _unique(prefix):
    return f"{prefix}{next(_names)}"


async def build(prefix):
    "A Datasette with a uniquely named in-memory database, ready to exercise."
    name = _unique(prefix)
    ds = Datasette(memory=True)
    ds.add_memory_database(name)
    await ds.invoke_startup()
    return ds, name


async def exercise(ds, name):
    """
    Drive enough of Datasette to emit every span the registry claims exists.

    Each request is here because it is the only thing that produces some span
    or attribute - see the comments. If you add instrumentation on a path this
    does not reach, add the path rather than loosening the assertions.
    """
    db = ds.get_database(name)

    # Writes: db.write.execute, db.write.queue_wait, executescript, executemany
    await db.execute_write("create table t (id integer primary key, v text)")
    await db.execute_write_many(
        "insert into t (id, v) values (?, ?)", [[i, f"v{i}"] for i in range(30)]
    )
    await db.execute_write_script("create table t2 (id integer); drop table t2;")
    # transaction=False, for the datasette.transaction attribute's other value
    await db.execute_write("vacuum", transaction=False)
    # An isolated connection, for datasette.isolated_connection
    await db.execute_isolated_fn(lambda conn: conn.execute("select 1").fetchone())

    # Reads: db.query, db.query.execute, rows_returned, truncated, param_count
    await db.execute("select * from t where id > :n", {"n": 5})
    await db.execute("select * from t", truncate=True)

    # A suppressed error, for datasette.sql_error_suppressed
    with pytest.raises(sqlite3.OperationalError):
        await db.execute("select nope from t", log_sql_errors=False)

    # HTML page: render_template, hook spans, permission checks, facets
    assert (await ds.client.get(f"/{name}/t?_facet=v")).status_code == 200
    # JSON: datasette.render, and facet suggestion
    assert (
        await ds.client.get(f"/{name}/t.json?_extra=suggested_facets")
    ).status_code == 200
    # Row page
    assert (await ds.client.get(f"/{name}/t/1.json")).status_code == 200
    # CSV, streamed, so pages_fetched > 1
    assert (await ds.client.get(f"/{name}/t.csv?_stream=1&_size=10")).status_code == 200
    # Instance page, for allowed_resources / permission_resources_sql
    assert (await ds.client.get(f"/{name}.json")).status_code == 200


async def exercise_facet_timeout():
    """
    Force a facet calculation to exceed facet_time_limit_ms.

    Its own instance, because a 1ms facet limit would distort everything else.
    A timed-out facet is dropped from the page silently, so
    `datasette.facets.timed_out` is the only signal it happened - which makes
    it exactly the kind of thing that must not rot undetected.
    """
    name = _unique("facettimeout")
    ds = Datasette(memory=True, settings={"facet_time_limit_ms": 1})
    db = ds.add_memory_database(name)
    await db.execute_write_fn(
        lambda conn: sqlite_utils.Database(conn)["big"].insert_all(
            [{"id": i, "cat": f"c{i % 997}"} for i in range(5000)], pk="id"
        )
    )
    await ds.invoke_startup()
    response = await ds.client.get(
        f"/{name}/big.json?_facet=cat&_extra=facets_timed_out"
    )
    assert response.status_code == 200
    assert response.json()["facets_timed_out"], (
        "expected the facet to time out - if this stops being reliable, the "
        "row count or the time limit needs adjusting, not the assertion"
    )
    ds.close()


@pytest_asyncio.fixture
async def emitted(otel_spans):
    "Every (span name, attribute key) pair produced by a broad set of requests."
    ds, name = await build("registry_spans")
    otel_spans.clear()
    await exercise(ds, name)
    await exercise_facet_timeout()
    spans = otel_spans.get_finished_spans()
    assert spans, "no spans captured - the fixture is not exercising anything"
    pairs = set()
    names = set()
    for span in spans:
        names.add(span.name)
        for key in span.attributes or {}:
            pairs.add((span.name, key))
    ds.close()
    return {"names": names, "pairs": pairs, "spans": spans}


@pytest.mark.asyncio
async def test_every_emitted_span_is_registered(emitted):
    "A span added without a registry entry would be missing from the docs."
    unregistered = sorted(
        name for name in emitted["names"] if reg.span_for(name) is None
    )
    assert (
        not unregistered
    ), f"these spans are emitted but not in telemetry_registry.SPANS: {unregistered}"


@pytest.mark.asyncio
async def test_every_emitted_attribute_is_registered(emitted):
    "An attribute added without a registry entry would be missing from the docs."
    unregistered = sorted(
        f"{span_name} -> {key}"
        for span_name, key in emitted["pairs"]
        if not reg.attribute_allowed(reg.span_for(span_name), key)
    )
    assert (
        not unregistered
    ), "these span attributes are emitted but not registered: " + "\n".join(
        unregistered
    )


@pytest.mark.asyncio
async def test_every_registered_span_is_emitted(emitted):
    """
    The direction nothing else catches: the docs must not describe a span that
    no longer exists.
    """
    missing = sorted(
        span
        for span in reg.SPANS
        if not any(reg.span_for(name) is span for name in emitted["names"])
    )
    assert not missing, (
        f"these spans are documented but never emitted by the test workload: {missing}. "
        "Either the instrumentation was removed, or exercise() no longer reaches it."
    )


@pytest.mark.asyncio
async def test_every_non_optional_attribute_is_emitted(emitted):
    """
    Attributes not marked `optional` must actually appear on their span.

    Catches an attribute that was dropped from the code but left in the docs,
    and catches an attribute wrongly documented as always-present.
    """
    missing = []
    for span in reg.SPANS:
        emitted_keys = {
            key for name, key in emitted["pairs"] if reg.span_for(name) is span
        }
        if not emitted_keys:
            continue
        for attribute in span.attributes:
            if not attribute.optional and attribute not in emitted_keys:
                missing.append(f"{span} -> {attribute}")
    assert not missing, (
        "these attributes are documented as always present but were not emitted: "
        + ", ".join(sorted(missing))
    )


@pytest.mark.asyncio
async def test_every_registered_metric_is_emitted(otel_metrics):
    "The same both-ways check for metrics."
    ds, name = await build("registry_metrics")
    await exercise(ds, name)
    await exercise_facet_timeout()
    # An interrupted query, the one metric a normal workload never produces
    slow_name = _unique("slow")
    slow = Datasette(memory=True, settings={"sql_time_limit_ms": 1})
    slow.add_memory_database(slow_name)
    from datasette.database import QueryInterrupted

    with pytest.raises(QueryInterrupted):
        await slow.get_database(slow_name).execute(
            "with recursive c(x) as (select 0 union all select x+1 from c) select * from c"
        )

    otel_metrics.collect()
    emitted = set(otel_metrics.snapshot)
    missing = sorted(str(m) for m in reg.METRICS if m not in emitted)
    assert not missing, f"documented but never emitted: {missing}"

    unregistered = sorted(
        name for name in emitted if name not in {str(m) for m in reg.METRICS}
    )
    assert not unregistered, f"emitted but not registered: {unregistered}"
    ds.close()
    slow.close()


# There is deliberately no test that a metric's unit matches the registry.
# `datasette/telemetry.py` builds each instrument with `unit=M_XXX.unit`, so
# the two sides are the same object and cannot disagree - a test would move
# both at once and pass no matter what. Name and unit drift is prevented by
# construction here; what the tests below cover is the part that isn't, which
# is whether the thing is emitted at all.


def test_registry_has_no_duplicate_names():
    assert len(set(reg.SPANS)) == len(reg.SPANS)
    assert len(set(reg.METRICS)) == len(reg.METRICS)


def test_registry_entries_are_documented():
    "Every entry carries a description - the docs are generated from these."
    for span in reg.SPANS:
        assert span.description.strip(), f"{span} has no description"
        for attribute in span.attributes:
            assert attribute.description.strip(), f"{span} -> {attribute} has none"
    for metric in reg.METRICS:
        assert metric.description.strip(), f"{metric} has no description"
        assert metric.unit, f"{metric} has no unit"
        assert metric.kind in (reg.COUNTER, reg.HISTOGRAM, reg.GAUGE)


def test_registry_entries_are_usable_as_plain_strings():
    "The str subclassing is the whole reason call sites need no wrapper API."
    assert reg.DB_NAMESPACE == "db.namespace"
    assert reg.DB_QUERY == "db.query"
    assert isinstance(reg.DB_QUERY, str)
    assert f"{reg.HOOK}render_cell" == "datasette.hook.render_cell"


def test_parameter_attribute_matching():
    "db.query.parameter.<name> matches any suffix, since names are arbitrary."
    assert reg.attribute_allowed(reg.DB_QUERY, "db.query.parameter.actor_id")
    assert reg.attribute_allowed(reg.DB_QUERY, "db.query.parameter.0")
    assert not reg.attribute_allowed(reg.DB_QUERY, "db.query.parameterX")


def test_hook_span_resolution():
    assert reg.span_for("datasette.hook.render_cell") is reg.HOOK
    assert reg.span_for("datasette.hook.anything_at_all") is reg.HOOK
    assert reg.span_for("db.query") is reg.DB_QUERY
    assert reg.span_for("not.a.datasette.span") is None


def test_sql_parameter_attributes_are_registered_when_enabled(otel_spans):
    """
    The opt-in parameter attributes must be registered too - they are the ones
    most likely to surprise someone reading a trace.
    """
    import asyncio

    async def inner():
        name = _unique("params")
        ds = Datasette(memory=True, settings={"trace_sql_parameters": "user"})
        ds.add_memory_database(name)
        await ds.invoke_startup()
        db = ds.get_database(name)
        await db.execute_write_fn(
            lambda conn: sqlite_utils.Database(conn)["t"].insert({"id": 1}, pk="id")
        )
        otel_spans.clear()
        await db.execute("select * from t where id = :wanted", {"wanted": 1})
        keys = [
            key
            for span in otel_spans.get_finished_spans()
            for key in (span.attributes or {})
            if key.startswith("db.query.parameter.")
        ]
        assert keys, "expected parameter attributes with the setting enabled"
        for key in keys:
            assert reg.attribute_allowed(reg.DB_QUERY, key)
        ds.close()

    asyncio.get_event_loop_policy()
    asyncio.run(inner())
