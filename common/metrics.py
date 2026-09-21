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

WEIGHT_ROLLBACKS = Counter(
    "recsys_weight_rollbacks_total",
    "Training runs where new LR weights were rejected due to NDCG regression",
)

# Online serving quality — tracked per recommendation position
ONLINE_CTR_BY_POSITION = Histogram(
    "recsys_online_ctr_by_position",
    "Click-through rate observations by recommendation position",
    ["position_bucket"],
)
ONLINE_ENGAGEMENT_RATE = Gauge(
    "recsys_online_engagement_rate",
    "Rolling engagement rate (play_start+complete+watchlist_add / impressions) over last 1000 feedback events",
)
FEEDBACK_EVENTS = Counter(
    "recsys_feedback_events_total",
    "Feedback events received",
    ["event_type"],
)

# Data quality and training pipeline health
TRAINING_DATA_QUALITY_FAILURES = Counter(
    "recsys_training_data_quality_failures_total",
    "Training runs aborted due to data quality checks",
    ["reason"],
)
ARTIFACTS_SAVED = Counter(
    "recsys_artifacts_saved_total",
    "Model artifacts successfully saved to object storage",
)

# Coverage trend — updated after each training run
EVAL_COVERAGE_DELTA = Gauge(
    "recsys_eval_coverage_delta",
    "Change in catalog coverage since the previous training run (positive = improving)",
)

EVAL_MRR_AT_10 = Gauge(
    "recsys_eval_mrr_at_10",
    "Mean Reciprocal Rank@10 from the most recent offline evaluation run",
)

EVAL_WATCH_TIME_NDCG_AT_10 = Gauge(
    "recsys_eval_watch_time_ndcg_at_10",
    "Watch-time-weighted NDCG@10: gain proportional to log(watch_seconds) rather than binary relevance",
)

# Business metrics — computed at training time from trailing 7-day interaction window
RETENTION_7DAY = Gauge(
    "recsys_retention_7day",
    "Fraction of last-week's active users who returned with an engagement event this week",
)

AVG_ATTRIBUTED_WATCH_SECONDS = Gauge(
    "recsys_avg_attributed_watch_seconds",
    "Average watch_seconds for recommendation-attributed plays (position > 0) over the last 7 days",
)

ATTRIBUTED_WATCH_TIME_SECONDS = Counter(
    "recsys_attributed_watch_time_seconds_total",
    "Cumulative watch_seconds for recommendation-attributed plays (position > 0)",
)

# Dead-letter queue consumer metrics
DLQ_EVENTS_ARCHIVED = Counter(
    "recsys_dlq_events_archived_total",
    "Dead-letter events successfully archived to object storage",
    ["event_type"],
)
DLQ_REPLAY_FAILURES = Counter(
    "recsys_dlq_replay_failures_total",
    "Dead-letter events that could not be archived or parsed",
)
