import os

# Set before any service modules are imported so pydantic-settings picks these up.
# Tests run against localhost defaults — no real infra needed for unit tests.
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
