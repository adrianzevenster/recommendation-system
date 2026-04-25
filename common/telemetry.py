from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from common.config import settings

_INITIALIZED = False


def setup_tracing(service_name: str):
    global _INITIALIZED
    if _INITIALIZED:
        return trace.get_tracer(service_name)

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    RequestsInstrumentor().instrument()
    _INITIALIZED = True
    return trace.get_tracer(service_name)


def instrument_fastapi(app, service_name: str):
    tracer = setup_tracing(service_name)
    FastAPIInstrumentor.instrument_app(app)
    return tracer
