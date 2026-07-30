"""
Tests for the opt-in trace_sql_parameters setting.

Recording parameter values is off by default and the default must stay
provably safe - tests/test_telemetry.py's
test_no_span_attribute_ever_contains_a_parameter_value covers that for a
default instance, and test_default_is_off below covers it here.

The two modes exist because of a specific hazard rather than general caution:
permission SQL binds json.dumps(actor) as :actor on every permission check, so
"user" excludes the internal database structurally rather than by denylisting
parameter names.
"""

import pytest

pytest.importorskip("opentelemetry.sdk")

from datasette.app import INTERNAL_DB_NAME, Datasette
from datasette.telemetry import MAX_PARAM_LENGTH, MAX_PARAMS
from datasette.utils import StartupError

SECRET = "SUPER_SECRET_PARAM_VALUE_XYZ_123"


def _query_spans(otel_spans, namespace=None):
    spans = [
        span for span in otel_spans.get_finished_spans() if span.name == "db.query"
    ]
    if namespace is not None:
        spans = [s for s in spans if s.attributes.get("db.namespace") == namespace]
    return spans


def _param_attributes(span):
    return {
        key: value
        for key, value in (span.attributes or {}).items()
        if key.startswith("db.query.parameter.")
    }


async def _run_query(mode, sql="select :secret", params=None):
    ds = Datasette(memory=True, settings={"trace_sql_parameters": mode})
    db = ds.add_memory_database("paramtest")
    await db.execute(sql, params if params is not None else {"secret": SECRET})
    return ds


@pytest.mark.asyncio
async def test_default_is_off(otel_spans):
    ds = Datasette(memory=True)
    assert ds.setting("trace_sql_parameters") == "off"
    db = ds.add_memory_database("paramtest")
    await db.execute("select :secret", {"secret": SECRET})

    spans = _query_spans(otel_spans, "paramtest")
    assert spans
    span = spans[-1]
    assert _param_attributes(span) == {}
    # The count is still recorded - that was never the sensitive part
    assert span.attributes["datasette.param_count"] == 1


@pytest.mark.asyncio
async def test_user_mode_records_named_parameters(otel_spans):
    await _run_query("user")

    spans = _query_spans(otel_spans, "paramtest")
    assert spans
    assert _param_attributes(spans[-1]) == {"db.query.parameter.secret": SECRET}


@pytest.mark.asyncio
async def test_positional_parameters_are_numbered(otel_spans):
    await _run_query("user", sql="select ?, ?", params=["first", "second"])

    spans = _query_spans(otel_spans, "paramtest")
    assert spans
    assert _param_attributes(spans[-1]) == {
        "db.query.parameter.0": "first",
        "db.query.parameter.1": "second",
    }


@pytest.mark.asyncio
async def test_blob_parameters_never_record_their_contents(otel_spans):
    secret_bytes = SECRET.encode("utf-8")
    await _run_query("user", sql="select :blob", params={"blob": secret_bytes})

    spans = _query_spans(otel_spans, "paramtest")
    assert spans
    recorded = _param_attributes(spans[-1])["db.query.parameter.blob"]
    assert recorded == f"<bytes[{len(secret_bytes)}]>"
    assert SECRET not in recorded


@pytest.mark.asyncio
async def test_long_parameter_values_are_truncated(otel_spans):
    long_value = "x" * (MAX_PARAM_LENGTH + 500)
    await _run_query("user", sql="select :long", params={"long": long_value})

    spans = _query_spans(otel_spans, "paramtest")
    assert spans
    recorded = _param_attributes(spans[-1])["db.query.parameter.long"]
    assert len(recorded) == MAX_PARAM_LENGTH + len("…[truncated]")
    assert recorded.endswith("…[truncated]")


@pytest.mark.asyncio
async def test_parameter_count_is_capped(otel_spans):
    params = {f"p{i}": i for i in range(MAX_PARAMS + 20)}
    sql = "select " + ", ".join(f":p{i}" for i in range(MAX_PARAMS + 20))
    await _run_query("user", sql=sql, params=params)

    spans = _query_spans(otel_spans, "paramtest")
    assert spans
    span = spans[-1]
    assert len(_param_attributes(span)) == MAX_PARAMS
    # The true count is still reported, so the cap is visible rather than silent
    assert span.attributes["datasette.param_count"] == MAX_PARAMS + 20


@pytest.mark.asyncio
async def test_user_mode_does_not_record_internal_database_parameters(
    ds_client, otel_spans
):
    """
    The load-bearing test: permission SQL binds json.dumps(actor) as :actor,
    so recording internal-database parameters would export actor identity on
    every permission check - undoing the guarantee the permission spans exist
    to provide.
    """
    ds = ds_client.ds
    original = ds._settings["trace_sql_parameters"]
    ds._settings["trace_sql_parameters"] = "user"
    try:
        await ds.client.get(
            "/fixtures/facetable",
            cookies={"ds_actor": ds_client.actor_cookie({"id": SECRET})},
        )
    finally:
        ds._settings["trace_sql_parameters"] = original

    internal_spans = _query_spans(otel_spans, INTERNAL_DB_NAME)
    assert internal_spans, "expected internal database queries during a request"
    for span in internal_spans:
        assert _param_attributes(span) == {}

    # And the actor id appears nowhere at all, on any span
    for span in otel_spans.get_finished_spans():
        for value in (span.attributes or {}).values():
            if isinstance(value, str):
                assert SECRET not in value


@pytest.mark.asyncio
async def test_all_mode_does_record_internal_database_parameters(ds_client, otel_spans):
    """
    'all' is documented as exporting actor identity. Prove that it really
    does, otherwise the "user" test above is only proving that nothing was
    there to leak in the first place.
    """
    ds = ds_client.ds
    original = ds._settings["trace_sql_parameters"]
    ds._settings["trace_sql_parameters"] = "all"
    try:
        await ds.client.get(
            "/fixtures/facetable",
            cookies={"ds_actor": ds_client.actor_cookie({"id": SECRET})},
        )
    finally:
        ds._settings["trace_sql_parameters"] = original

    internal_spans = _query_spans(otel_spans, INTERNAL_DB_NAME)
    assert internal_spans
    recorded = [
        value
        for span in internal_spans
        for value in _param_attributes(span).values()
        if isinstance(value, str)
    ]
    assert recorded, "expected 'all' to record internal database parameters"
    assert any(SECRET in value for value in recorded), (
        "expected the actor id to be exported under 'all' - if this stops being "
        "true the 'user' mode test above becomes vacuous"
    )


def test_invalid_mode_fails_loudly_at_startup():
    with pytest.raises(StartupError) as excinfo:
        Datasette(memory=True, settings={"trace_sql_parameters": "yes"})
    assert "trace_sql_parameters" in str(excinfo.value)
    assert "'off', 'user', 'all'" in str(excinfo.value)
