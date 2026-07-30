"""
Render the span and metric reference in ``internals.rst`` from
``datasette/telemetry_registry.py``.

Driven by cog, and ``cog --check docs/*.rst`` runs in CI - so adding a span
without documenting it, or documenting one that no longer exists, is a build
failure rather than something a reader discovers later.
"""


def _attribute_lines(cog, attributes):
    if not attributes:
        cog.out("    No attributes.\n\n")
        return
    cog.out("    Attributes:\n\n")
    for attribute in attributes:
        suffix = " *(optional)*" if attribute.optional else ""
        cog.out(f"    - ``{attribute}``{suffix} - {attribute.description}\n")
    cog.out("\n")


def spans(cog):
    from datasette.telemetry_registry import SPANS

    cog.out("\n")
    for span in SPANS:
        title = f"{span}*" if span.prefix else str(span)
        cog.out(f"``{title}``\n")
        cog.out(f"    {span.description}\n\n")
        _attribute_lines(cog, span.attributes)


def metrics(cog):
    from datasette.telemetry_registry import METRICS

    cog.out("\n")
    for metric in METRICS:
        cog.out(f"``{metric}``\n")
        cog.out(f"    {metric.kind}, unit ``{metric.unit}``. {metric.description}\n\n")
        if metric.attributes:
            cog.out("    Attributes:\n\n")
            for attribute in metric.attributes:
                suffix = " *(optional)*" if attribute.optional else ""
                cog.out(f"    - ``{attribute}``{suffix}\n")
            cog.out("\n")
