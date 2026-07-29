import json

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.trace import StatusCode

from datasette.telemetry import MAX_SQL_LENGTH, sql_attribute

SECRET_PARAM_VALUE = "SUPER_SECRET_PARAM_VALUE_XYZ_123"


def _db_query_spans(otel_spans):
    return [span for span in otel_spans.get_finished_spans() if span.name == "db.query"]


def _all_attribute_values(otel_spans):
    "Every attribute value across every finished span, for the 'no leaked param values' test."
    values = []
    for span in otel_spans.get_finished_spans():
        for value in (span.attributes or {}).values():
            values.append(value)
        for event in span.events:
            for value in (event.attributes or {}).values():
                values.append(value)
    return values


@pytest.mark.asyncio
async def test_db_query_span_basic_attributes(ds_client, otel_spans):
    response = await ds_client.get("/fixtures/-/query.json?sql=select+1")
    assert response.status_code == 200

    spans = _db_query_spans(otel_spans)
    assert spans, "expected at least one db.query span"
    span = spans[-1]

    assert span.attributes["db.system"] == "sqlite"
    assert span.attributes["db.namespace"] == "fixtures"
    assert span.attributes["db.query.text"] == "select 1"
    assert span.attributes["datasette.rows_returned"] == 1
    assert span.attributes["datasette.truncated"] is False
    assert isinstance(span.attributes["datasette.time_limit_ms"], int)
    assert span.status.status_code == StatusCode.UNSET


def test_sql_attribute_truncates_at_2048():
    short_sql = "select 1"
    assert sql_attribute(short_sql) == "select 1"

    long_sql = "select 1 -- " + ("x" * 3000)
    truncated = sql_attribute(long_sql)
    assert len(truncated) == MAX_SQL_LENGTH + len("…[truncated]")
    assert truncated.startswith("select 1 -- ")
    assert truncated.endswith("…[truncated]")


@pytest.mark.asyncio
async def test_db_query_text_is_truncated_in_real_span(ds_client, otel_spans):
    # A long trailing SQL comment keeps the query valid and executable while
    # pushing db.query.text well past the 2048 char cap.
    long_sql = "select 1 -- " + ("x" * 3000)
    response = await ds_client.get("/fixtures/-/query.json", params={"sql": long_sql})
    assert response.status_code == 200

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    recorded = span.attributes["db.query.text"]
    assert len(recorded) == MAX_SQL_LENGTH + len("…[truncated]")
    assert recorded.endswith("…[truncated]")


@pytest.mark.asyncio
async def test_no_span_attribute_ever_contains_a_parameter_value(ds_client, otel_spans):
    response = await ds_client.get(
        "/fixtures/-/query.json",
        params={"sql": "select :secret", "secret": SECRET_PARAM_VALUE},
    )
    assert response.status_code == 200
    # Sanity check the value really did flow through as a bound parameter,
    # not inlined into the SQL text, otherwise this test would be vacuous.
    assert SECRET_PARAM_VALUE in json.dumps(response.json())

    for value in _all_attribute_values(otel_spans):
        if isinstance(value, str):
            assert SECRET_PARAM_VALUE not in value
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, str):
                    assert SECRET_PARAM_VALUE not in item

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert "select :secret" in span.attributes["db.query.text"]
    assert span.attributes.get("datasette.param_count") == 1


@pytest.mark.asyncio
async def test_query_interrupted_sets_error_status(ds_client, otel_spans):
    response = await ds_client.get(
        "/fixtures/-/query.json",
        params={"sql": "select sleep(0.05)", "_timelimit": 5},
    )
    assert response.status_code == 400

    spans = _db_query_spans(otel_spans)
    assert spans
    span = spans[-1]
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["datasette.interrupted"] is True
    assert span.events
    assert all(event.name == "exception" for event in span.events)
