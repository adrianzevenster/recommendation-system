from prometheus_client import Counter, Gauge, Histogram

REQUEST_LATENCY = Histogram(
    "recsys_request_latency_seconds",
    "Latency for recommendation requests",
    ["endpoint"],
)
RECOMMENDATION_REQUESTS = Counter(
    "recsys_recommendation_requests_total",
    "Recommendation requests",
    ["context"],
)
EVENTS_CONSUMED = Counter(
    "recsys_events_consumed_total",
    "Events consumed",
    ["event_type"],
)
EVENTS_GENERATED = Counter(
    "recsys_events_generated_total",
    "Events generated",
    ["event_type"],
)
MODELS_TRAINED = Counter(
    "recsys_models_trained_total",
    "Training runs completed",
)
CANDIDATES_GENERATED = Histogram(
    "recsys_candidates_generated",
    "Candidates generated before ranking",
    ["source"],
)
ACTIVE_USERS_GAUGE = Gauge(
    "recsys_active_users_simulated",
    "Active users in simulator window",
)
