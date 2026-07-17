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

# Offline evaluation metrics — updated after each training run
EVAL_NDCG_AT_10 = Gauge(
    "recsys_eval_ndcg_at_10",
    "NDCG@10 from the most recent offline evaluation run",
)
EVAL_HIT_RATE_AT_10 = Gauge(
    "recsys_eval_hit_rate_at_10",
    "Hit rate@10 from the most recent offline evaluation run",
)
EVAL_COVERAGE = Gauge(
    "recsys_eval_catalog_coverage",
    "Fraction of the catalog appearing in any user's top-10",
)

# Online serving metrics
COLD_START_REQUESTS = Counter(
    "recsys_cold_start_requests_total",
    "Requests served via the cold-start (trending) fallback path",
)
EXPERIMENT_REQUESTS = Counter(
    "recsys_experiment_requests_total",
    "Requests assigned to an A/B experiment",
    ["experiment", "variant"],
)
