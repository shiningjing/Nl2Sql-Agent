"""Thin OTel bridge — exports TraceLogger events as OpenTelemetry spans.

Does NOT replace TraceLogger. Runs alongside it: each call to tlog._emit()
writes JSONL as before, and this bridge reads the in-memory events to
construct OTel spans for Jaeger/Honeycomb/Grafana visualization.

Usage:
    from observability.otel_bridge import export_trace
    export_trace(tlog, service_name="nl2sql-agent")
"""

from __future__ import annotations

import time as _time
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource

try:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    _OTLP_AVAILABLE = True
except ImportError:
    OTLPSpanExporter = None  # type: ignore[assignment]
    _OTLP_AVAILABLE = False
from opentelemetry.trace import Status, StatusCode

_provider: TracerProvider | None = None
_initialized: bool = False


def _init_provider(service_name: str = "nl2sql-agent",
                   otlp_endpoint: str = "") -> TracerProvider:
    """One-time OTel SDK init. Safe to call multiple times."""
    global _provider, _initialized
    if _initialized and _provider is not None:
        return _provider

    resource = Resource.create({"service.name": service_name})
    _provider = TracerProvider(resource=resource)

    # Console exporter for local dev
    _provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    # OTLP exporter for Jaeger/Grafana (optional)
    if otlp_endpoint and _OTLP_AVAILABLE and OTLPSpanExporter:
        _provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint))
        )

    trace.set_tracer_provider(_provider)
    _initialized = True
    return _provider


def export_trace(tlog: Any, service_name: str = "nl2sql-agent",
                 otlp_endpoint: str = "") -> list[dict]:
    """Convert TraceLogger events → OTel spans, flush, return span summaries.

    Returns list of {name, duration_ms, status, attributes_keys} for verification.
    """
    events = getattr(tlog, "events", [])
    if not events:
        return []

    _init_provider(service_name, otlp_endpoint)
    tracer = trace.get_tracer(service_name)

    # ── Pass 1: build span tree ──
    # node_enter + node_exit pairs → parent spans
    # llm_call events → child spans of their node
    node_spans: dict[str, Any] = {}  # node_name → span
    span_summaries: list[dict] = []
    open_spans: dict[str, tuple[Any, float]] = {}  # node_name → (span, start_ts)

    for ev in events:
        event_type = ev.get("event", "")

        if event_type == "node_enter":
            node_name = ev.get("node", "?")
            span = tracer.start_span(node_name)
            span.set_attribute("trace_id", ev.get("trace_id", ""))
            if ev.get("model"):
                span.set_attribute("llm.model", ev["model"])
            if ev.get("prompt_version"):
                span.set_attribute("prompt.version", ev["prompt_version"])
            open_spans[node_name] = (span, _time.time())
            node_spans[node_name] = span

        elif event_type == "node_exit":
            node_name = ev.get("node", "?")
            pair = open_spans.pop(node_name, None)
            if pair is None:
                continue
            span, _ = pair
            span.set_attribute("duration_s", ev.get("duration_s", 0))
            for k, v in (ev.get("meta", {}) or {}).items():
                span.set_attribute(f"meta.{k}", str(v)[:200])

            status = ev.get("status", "success")
            if status == "error":
                span.set_status(Status(StatusCode.ERROR, str(ev.get("error", ""))[:200]))
            elif status == "skipped":
                span.set_status(Status(StatusCode.OK, "skipped"))
            else:
                span.set_status(Status(StatusCode.OK))

            span.end()
            span_summaries.append({
                "name": node_name,
                "duration_s": ev.get("duration_s", 0),
                "status": status,
            })

        elif event_type == "llm_call":
            parent_node = ev.get("node", "llm")
            parent_span = node_spans.get(parent_node)
            span = tracer.start_span("llm_call", context=trace.set_span_in_context(parent_span) if parent_span else None)  # type: ignore[arg-type]
            span.set_attribute("llm.model", ev.get("model", ""))
            span.set_attribute("llm.prompt_tokens", ev.get("prompt_tokens", 0))
            span.set_attribute("llm.completion_tokens", ev.get("completion_tokens", 0))
            span.set_attribute("llm.total_tokens", ev.get("total_tokens", 0))
            span.set_attribute("llm.duration_s", ev.get("duration_s", 0))
            if parent_node:
                span.set_attribute("parent.node", parent_node)
            span.set_status(Status(StatusCode.OK))
            span.end()
            span_summaries.append({
                "name": f"llm_call[{parent_node}]",
                "duration_s": ev.get("duration_s", 0),
                "status": "success",
            })

        elif event_type == "sql_exec":
            parent_span = node_spans.get("executor") or node_spans.get("voter")
            span = tracer.start_span("sql_exec", context=trace.set_span_in_context(parent_span) if parent_span else None)  # type: ignore[arg-type]
            span.set_attribute("sql.success", ev.get("success", False))
            span.set_attribute("sql.row_count", ev.get("row_count", 0))
            span.set_attribute("sql.duration_s", ev.get("duration_s", 0))
            if ev.get("error"):
                span.set_attribute("sql.error", str(ev["error"])[:200])
                span.set_status(Status(StatusCode.ERROR, str(ev["error"])[:200]))
            else:
                span.set_status(Status(StatusCode.OK))
            span.end()

    # Close any remaining open spans (missing node_exit)
    for node_name, (span, _) in open_spans.items():
        span.set_status(Status(StatusCode.ERROR, "node_exit missing"))
        span.end()
        span_summaries.append({
            "name": node_name,
            "duration_s": 0,
            "status": "incomplete",
        })

    _provider.force_flush() if _provider else None  # type: ignore[union-attr]
    return span_summaries
