"""
Tests for `datasette_resource_detector`, the OpenTelemetry resource detector
that contributes Datasette's `service.version` (and a `service.name`
default) to whichever Resource an OTel-SDK-based agent builds.

This module lives outside the `datasette` package - see its docstring for
why - so, unlike `datasette/telemetry.py`, its tests may import the SDK
directly without contradicting the "core is API-only" invariant. The
invariant itself is what `test_datasette_package_never_imports_the_sdk`
below checks.
"""

import importlib.metadata
import pathlib

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.resources import Resource

from datasette.version import __version__
from datasette_resource_detector import DatasetteResourceDetector


def test_detector_reports_real_datasette_version():
    "The whole point of the ticket: service.version tracks datasette.__version__."
    resource = DatasetteResourceDetector().detect()
    assert resource.attributes["service.version"] == __version__


def test_detector_defaults_service_name_to_datasette():
    resource = DatasetteResourceDetector().detect()
    assert resource.attributes["service.name"] == "datasette"


def test_entry_point_is_registered_and_loads_the_detector():
    """
    The SDK discovers detectors through the `opentelemetry_resource_detector`
    entry point group, by name. If the entry point were missing or pointed
    at the wrong object, the detector would never run even though the class
    itself works - this is what catches that.
    """
    eps = list(
        importlib.metadata.entry_points(
            group="opentelemetry_resource_detector", name="datasette"
        )
    )
    assert len(eps) == 1, (
        "expected exactly one 'datasette' entry in the "
        "opentelemetry_resource_detector group"
    )
    loaded = eps[0].load()
    assert loaded is DatasetteResourceDetector


def test_resource_create_picks_up_the_detector_when_opted_in(monkeypatch):
    """
    Registering the entry point is not enough on its own: as of the pinned
    opentelemetry-sdk, `Resource.create()` only scans
    `opentelemetry_resource_detector` entry points when
    `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` names this detector (or `*`) -
    otherwise it takes a fast path that never calls entry_points() at all.
    This is the real, through-the-SDK path end to end, as opposed to
    test_detector_reports_real_datasette_version's direct instantiation.
    """
    monkeypatch.setenv("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS", "datasette")
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
    monkeypatch.delenv("OTEL_RESOURCE_ATTRIBUTES", raising=False)
    resource = Resource.create()
    assert resource.attributes["service.version"] == __version__
    assert resource.attributes["service.name"] == "datasette"


def test_resource_create_without_opt_in_does_not_run_the_detector(monkeypatch):
    "The other half of the fast-path behaviour: no opt-in, no detector."
    monkeypatch.delenv("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS", raising=False)
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
    monkeypatch.delenv("OTEL_RESOURCE_ATTRIBUTES", raising=False)
    resource = Resource.create()
    assert "service.version" not in resource.attributes


def test_explicit_otel_service_name_wins_over_the_detector_default(monkeypatch):
    """
    A detector that overrides an operator's explicit configuration is worse
    than no detector - this is the precedence guarantee the ticket calls
    out by name. The SDK always appends its own "otel" detector (which reads
    OTEL_SERVICE_NAME) after every entry-point-registered detector unless an
    operator explicitly reorders it, so this holds under default ordering.
    """
    monkeypatch.setenv("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS", "datasette")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "operators-real-service-name")
    monkeypatch.delenv("OTEL_RESOURCE_ATTRIBUTES", raising=False)
    resource = Resource.create()
    assert resource.attributes["service.name"] == "operators-real-service-name"
    # The version is Datasette's own and nothing else supplies it, so it
    # still comes through even though the name was overridden.
    assert resource.attributes["service.version"] == __version__


def test_datasette_package_never_imports_the_sdk():
    """
    The architecture invariant this whole module exists to preserve: nothing
    under `datasette/` may reference `opentelemetry.sdk`. This is the same
    check as `grep -rn 'opentelemetry\\.sdk' datasette/`, pinned in the
    suite rather than only run by hand.
    """
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    hits = [
        str(path)
        for path in (repo_root / "datasette").rglob("*.py")
        if "opentelemetry.sdk" in path.read_text()
    ]
    assert not hits, f"datasette/ references opentelemetry.sdk in: {hits}"
