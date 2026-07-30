"""
Wrap Datasette's ASGI app so each HTTP request gets a root span.

`opentelemetry-instrument` will not do this on its own. Auto-instrumentation
only picks up frameworks that ship an instrumentor entry point, and Datasette's
raw ASGI app is not one of them - so without this plugin every span Datasette
emits is a root span, and a trace UI shows dozens of unrelated single-span
traces per page instead of one request you can actually read.

Load it with:

    datasette mydb.db --plugins-dir demos/otel/plugins

and make sure `opentelemetry-instrumentation-asgi` is installed. See the
README in this directory for the full command.
"""

from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware

from datasette import hookimpl


@hookimpl
def asgi_wrapper(datasette):
    def wrap(app):
        return OpenTelemetryMiddleware(app)

    return wrap
