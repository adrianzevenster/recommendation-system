from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "service"
    log_level: str = "INFO"

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "recsys"
    postgres_user: str = "recsys"
    postgres_password: str = "recsys"

    redis_host: str = "redis"
    redis_port: int = 6379

    kafka_bootstrap_servers: str = "redpanda:9092"
    kafka_topic_events: str = "user-events"
    kafka_group_id: str = "recsys-group"

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "minio"
    minio_secret_key: str = "minio123"
    minio_bucket: str = "raw-events"

    otel_exporter_otlp_endpoint: str = "http://jaeger:4317"
    api_port: int = 8000
    metrics_port: int = 9000
    recommendation_limit_default: int = 10

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
