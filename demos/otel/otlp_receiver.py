"""
A minimal OTLP/HTTP trace receiver, in about a hundred lines of Python.

Run it, point Datasette's OpenTelemetry agent at it, and get a real end-to-end
export - over the wire, in the real protobuf wire format - without Docker, a
collector, or Jaeger:

    # terminal 1
    uv run --with opentelemetry-proto python demos/otel/otlp_receiver.py

    # terminal 2
    OTEL_TRACES_EXPORTER=otlp \\
    OTEL_METRICS_EXPORTER=none \\
    OTEL_LOGS_EXPORTER=none \\
    OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \\
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \\
    OTEL_SERVICE_NAME=datasette \\
      uv run --with opentelemetry-exporter-otlp-proto-http \\
        opentelemetry-instrument datasette mydb.db

Load a page, wait ~10s for the batch processor to flush, then press Ctrl-C
here for a summary.

This is a debugging aid, not a tracing backend: it does not persist anything,
speaks only OTLP/HTTP (not gRPC), and ignores metrics and logs. For anything
real, export to an actual backend instead.
"""

import gzip
import signal
import sys
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceRequest,
        ExportTraceServiceResponse,
    )
except ImportError:
    sys.exit(
        "This receiver needs the OpenTelemetry protobuf definitions:\n"
        "    uv run --with opentelemetry-proto python demos/otel/otlp_receiver.py"
    )

HOST = "127.0.0.1"
PORT = 4318

received = []


def attribute_value(value):
    for field in ("string_value", "int_value", "double_value", "bool_value"):
        if value.HasField(field):
            return getattr(value, field)
    return None


class OTLPHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # the default handler logs every request to stderr

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        request = ExportTraceServiceRequest()
        request.ParseFromString(body)
        batch = 0
        for resource_spans in request.resource_spans:
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    batch += 1
                    received.append(
                        {
                            "name": span.name,
                            "parent_id": span.parent_span_id.hex() or None,
                            "duration_ms": (
                                span.end_time_unix_nano - span.start_time_unix_nano
                            )
                            / 1e6,
                            "attributes": {
                                a.key: attribute_value(a.value) for a in span.attributes
                            },
                        }
                    )
        print(f"received {batch} spans ({len(received)} total)")

        payload = ExportTraceServiceResponse().SerializeToString()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def summarise():
    if not received:
        print("\nNo spans received.")
        print("Remember: the default BatchSpanProcessor flushes about every 10s,")
        print("and `opentelemetry-instrument` is required - core installs no provider.")
        return

    print(f"\n=== {len(received)} spans ===")
    for name, count in Counter(span["name"] for span in received).most_common():
        total_ms = sum(s["duration_ms"] for s in received if s["name"] == name)
        print(f"  {name:<45}{count:>5}  {total_ms:>9.2f}ms total")

    slowest = sorted(received, key=lambda s: -s["duration_ms"])[:5]
    print("\nslowest spans:")
    for span in slowest:
        plugin = span["attributes"].get("datasette.plugin")
        suffix = f"  [{plugin}]" if plugin else ""
        print(f"  {span['duration_ms']:>9.2f}ms  {span['name']}{suffix}")

    roots = [span for span in received if not span["parent_id"]]
    print(f"\n{len(roots)} root spans")
    print(
        "Install opentelemetry-instrumentation-asgi and wrap Datasette's ASGI app to\n"
        "get a single per-request root span for these to nest under."
    )


if __name__ == "__main__":
    try:
        server = HTTPServer((HOST, PORT), OTLPHandler)
    except OSError as error:
        sys.exit(
            f"Could not listen on {HOST}:{PORT} ({error}).\n"
            "Something else is already using the OTLP port - most likely an "
            "earlier copy of this receiver that is still running."
        )
    print(f"OTLP/HTTP receiver listening on http://{HOST}:{PORT}")
    print("Press Ctrl-C for a summary.")

    # serve_forever() runs on a worker thread and the main thread just waits,
    # so the summary still prints when this is launched through a wrapper such
    # as `uv run`, where relying on KeyboardInterrupt alone is unreliable.
    stop = threading.Event()
    threading.Thread(target=server.serve_forever, daemon=True).start()

    def request_stop(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        stop.wait()
    except KeyboardInterrupt:
        pass
    server.shutdown()
    summarise()
