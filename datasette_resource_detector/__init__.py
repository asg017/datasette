"""
An OpenTelemetry resource detector contributing Datasette's own
``service.version`` (and a ``service.name`` default) to the ``Resource`` an
OTel-SDK-based agent builds around a running Datasette process.

**Why this is not inside the ``datasette`` package.** ``datasette`` core
depends on ``opentelemetry-api`` only - see the module docstring of
``datasette/telemetry.py`` - and never creates a provider, never configures
an exporter, and never imports ``opentelemetry.sdk``. That is checked
mechanically: ``grep -rn 'opentelemetry\\.sdk' datasette/`` must return
nothing.

``opentelemetry.sdk.resources.ResourceDetector`` is, as its dotted path
says, an SDK class. A module that subclasses it therefore imports the SDK -
and there is no way to define this class without doing that somewhere, since
the SDK's loader (below) needs a real ``opentelemetry.sdk.resources.Resource``
instance back from ``detect()``, not a plain dict. Rather than hide that
import behind a lazy `import` *inside* a module that still lives under
``datasette/`` - which would keep the dependency out of the import graph at
runtime but not out of the grep, and would only be honest if the check were
loosened for this one file - this module lives as a sibling top-level
package instead. It ships in the same wheel (``pyproject.toml``'s
``packages.find`` include of ``"datasette*"`` picks it up because its name
starts with ``datasette``), but it is not part of the ``datasette`` package's
own namespace or import graph: nothing under ``datasette/`` imports it, so
``import datasette`` / ``import datasette.app`` never reaches it, with or
without the SDK installed.

The only thing that ever imports this module is the SDK's own
``opentelemetry.sdk.resources._build_resource_detectors()``, via
``importlib.metadata.EntryPoint.load()`` against the
``opentelemetry_resource_detector`` entry point group - see
``pyproject.toml``. That only happens when the SDK is already installed and
already running (nothing else calls that function), so by the time this
module's ``opentelemetry.sdk`` import executes, the SDK is guaranteed to be
present. A bare ``pip install datasette`` with no SDK present never imports
this module at all.

**This is opt-in even with the SDK installed.** As of the pinned
``opentelemetry-sdk``, ``Resource.create()`` takes a fast path that returns
only the SDK's own two built-in detectors (``service_instance``, ``otel``)
and never calls ``entry_points()`` at all, *unless* the
``OTEL_EXPERIMENTAL_RESOURCE_DETECTORS`` environment variable names this
detector - its entry point name, ``datasette`` - or ``*``. Registering the
entry point makes the detector discoverable; it does not make it run. See
the "Resource attributes" section of the OpenTelemetry docs for the operator
side of this.

**Precedence.** ``service.name`` is contributed only as a default. The SDK
always appends its own ``otel`` detector - the one that reads
``OTEL_SERVICE_NAME`` and ``OTEL_RESOURCE_ATTRIBUTES`` - after every
entry-point-registered detector, unless an operator explicitly reorders it
in ``OTEL_EXPERIMENTAL_RESOURCE_DETECTORS``, and a later detector's
attributes win when resources are merged. So under normal (default-ordering)
use, an explicit ``OTEL_SERVICE_NAME`` always overrides the default set
here.
"""

from opentelemetry.sdk.resources import Resource, ResourceDetector
from opentelemetry.semconv.resource import ResourceAttributes

from datasette.version import __version__

# service.name and service.version are standard OpenTelemetry semantic
# conventions, not names Datasette owns - so their keys come from
# opentelemetry.semconv, the same way datasette/telemetry_registry.py's own
# docstring describes for db.system and friends. "datasette" itself is a
# value, not an attribute name, and is already used the same way as the
# instrumentation scope name in datasette/telemetry.py's
# `otel_trace.get_tracer("datasette", ...)`.
DEFAULT_SERVICE_NAME = "datasette"


class DatasetteResourceDetector(ResourceDetector):
    """
    Contributes ``service.version`` (Datasette's own version) and a
    ``service.name`` default of ``"datasette"`` - see the module docstring
    above for why an explicit ``OTEL_SERVICE_NAME`` still wins.
    """

    def detect(self) -> Resource:
        return Resource(
            {
                ResourceAttributes.SERVICE_NAME: DEFAULT_SERVICE_NAME,
                ResourceAttributes.SERVICE_VERSION: __version__,
            }
        )
