import logging
import math
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from prometheus_client import start_http_server
from sklearn.linear_model import LogisticRegression
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy import delete, select, text

from common.config import settings
from common.db import SessionLocal, engine
from common.eval import (
    DEFAULT_WEIGHTS,
    ENGAGEMENT_EVENTS,
    WEIGHT_KEYS,
    build_feature_matrix,
    build_score_lookups,
    evaluate_model,
)
from common.logging_utils import configure_logging
from common.metrics import (
    EVAL_COVERAGE,
    EVAL_HIT_RATE_AT_10,
    EVAL_NDCG_AT_10,
    MODELS_TRAINED,
)
from common.models import (
    Base,
    Interaction,
    Item,
    ItemNeighbor,
    ModelEvaluation,
    ModelVersion,
    RankingWeights,
    TrendingItem,
)
from common.telemetry import setup_tracing

SERVICE_NAME = "trainer"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
tracer = setup_tracing(SERVICE_NAME)

# Minimum samples required to trust the logistic regression over defaults
_MIN_TRAINING_SAMPLES = 50
_MIN_POSITIVE_LABELS = 5
# Fraction of interactions held out for offline evaluation
_EVAL_FRACTION = 0.20


def serve_metrics() -> None:
    start_http_server(settings.metrics_port)


def wait_for_postgres() -> None:
    for _ in range(30):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:
            logger.info("Waiting for Postgres", extra={"error": str(exc)})
            time.sleep(2)
    raise RuntimeError("Postgres unavailable")


# ---------------------------------------------------------------------------
# Interaction weighting
# ---------------------------------------------------------------------------

def interaction_weight(event_type: str, completion_pct: float) -> float:
    base = {
        "impression": 0.2,
        "click": 0.5,
        "play_start": 1.0,
        "watch_progress": 1.5,
        "complete": 2.2,
        "watchlist_add": 0.8,
    }.get(event_type, 0.1)
    return base + (completion_pct / 100.0)


# ---------------------------------------------------------------------------
# Temporal split
# ---------------------------------------------------------------------------

def temporal_split(
    interactions: list[Interaction],
    eval_fraction: float = _EVAL_FRACTION,
) -> tuple[list[Interaction], list[Interaction]]:
    """
    Split interactions into train/test by timestamp (chronological order).
    The training set is the first (1 - eval_fraction) fraction by time;
    the test set is the remaining tail.

    All neighbor tables are built from the FULL interaction history so that
    new items don't become invisible during evaluation — the split is used
    only for computing metrics and learning ranking weights.
    """
    if not interactions:
        return [], []
    sorted_ixs = sorted(interactions, key=lambda x: x.event_ts)
    split_idx = int(len(sorted_ixs) * (1.0 - eval_fraction))
    return sorted_ixs[:split_idx], sorted_ixs[split_idx:]


# ---------------------------------------------------------------------------
# Candidate-table builders (unchanged from original, work on full dataset)
# ---------------------------------------------------------------------------

def build_collaborative_neighbors(interactions: list[Interaction], top_k: int = 8):
    user_items: dict[str, list] = defaultdict(list)
    for ix in interactions:
        if ix.event_type not in {"play_start", "watch_progress", "complete", "watchlist_add", "click"}:
            continue
        user_items[ix.user_id].append(ix.item_id)

    co_counts: dict[str, Counter] = defaultdict(Counter)
    item_counts: Counter = Counter()

    for items in user_items.values():
        unique_items = list(dict.fromkeys(items))
        for item in unique_items:
            item_counts[item] += 1
        for i in range(len(unique_items)):
            for j in range(i + 1, len(unique_items)):
                a, b = unique_items[i], unique_items[j]
                co_counts[a][b] += 1
                co_counts[b][a] += 1

    neighbors = []
    for source_item, related in co_counts.items():
        scored = []
        for neighbor, count in related.items():
            denom = math.sqrt(item_counts[source_item] * item_counts[neighbor]) or 1.0
            scored.append((neighbor, count / denom))
        for neighbor, score in sorted(scored, key=lambda x: x[1], reverse=True)[:top_k]:
            neighbors.append((source_item, neighbor, float(round(score, 6)), "collaborative"))
    return neighbors


def build_content_neighbors(items: list[Item], top_k: int = 8):
    if not items:
        return []
    corpus = [
        f"{item.title} {item.genres.replace(',', ' ')} {item.actors} {item.director} {item.synopsis}"
        for item in items
    ]
    vectorizer = TfidfVectorizer(stop_words="english")
    matrix = vectorizer.fit_transform(corpus)
    sim = cosine_similarity(matrix)

    neighbors = []
    for i, item in enumerate(items):
        scores = [(items[j].item_id, float(sim[i][j])) for j in range(len(items)) if i != j]
        for neighbor, score in sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]:
            neighbors.append((item.item_id, neighbor, float(round(score, 6)), "content"))
    return neighbors


def build_trending(interactions: list[Interaction]):
    region_scores: dict[str, Counter] = defaultdict(Counter)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    for ix in interactions:
        if ix.event_ts.replace(tzinfo=timezone.utc) < cutoff:
            continue
        region_scores[ix.region][ix.item_id] += interaction_weight(ix.event_type, ix.completion_pct)

    return [
        (region, item_id, float(round(score, 6)))
        for region, scores in region_scores.items()
        for item_id, score in scores.most_common(10)
    ]


# ---------------------------------------------------------------------------
# Weight learning
# ---------------------------------------------------------------------------

def learn_ranking_weights(
    X: list,
    y: list,
) -> tuple[list[float], int]:
    """
    Fit a logistic regression on (signal_features, engagement_label) pairs.

    Returns (weights, sample_count).
    weights is a length-6 list [collab, content, session, trending, freshness, genre_bonus]
    normalised to sum to 1.0 with all values ≥ 0.

    Falls back to DEFAULT_WEIGHTS when the training set is too small or fitting
    fails — logged at WARNING so the caller always receives a valid weight vector.
    """
    n_samples = len(X)
    n_positives = sum(y)

    if n_samples < _MIN_TRAINING_SAMPLES or n_positives < _MIN_POSITIVE_LABELS:
        logger.warning(
            "Insufficient data for weight learning — using defaults",
            extra={"samples": n_samples, "positives": n_positives},
        )
        return DEFAULT_WEIGHTS, n_samples

    try:
        X_arr = np.array(X, dtype=np.float32)
        y_arr = np.array(y, dtype=np.int32)

        clf = LogisticRegression(
            max_iter=500,
            class_weight="balanced",
            solver="lbfgs",
            random_state=42,
        )
        clf.fit(X_arr, y_arr)

        # Clip negatives: a negative coefficient means the signal hurts predictions
        # in this dataset; we fall back to zero rather than invert the signal.
        raw = np.maximum(clf.coef_[0], 0.0)
        total = raw.sum()
        if total == 0.0:
            logger.warning("All learned weights are ≤ 0 — using defaults")
            return DEFAULT_WEIGHTS, n_samples

        weights = (raw / total).tolist()
        logger.info(
            "Learned ranking weights",
            extra=dict(zip(WEIGHT_KEYS, [round(w, 4) for w in weights])),
        )
        return weights, n_samples

    except Exception as exc:
        logger.exception("Weight learning failed — using defaults", extra={"error": str(exc)})
        return DEFAULT_WEIGHTS, n_samples


# ---------------------------------------------------------------------------
# Main training loop
# ---------------------------------------------------------------------------

def run_training_once() -> None:
    with tracer.start_as_current_span("training_run"):
        with SessionLocal() as session:
            items = session.execute(select(Item).where(Item.is_active.is_(True))).scalars().all()
            interactions = session.execute(select(Interaction)).scalars().all()

            if not interactions:
                logger.info("No interactions yet — skipping training run")
                return

            # ----------------------------------------------------------------
            # Step 1: Temporal split (for eval and weight learning)
            # ----------------------------------------------------------------
            train_ixs, test_ixs = temporal_split(interactions)
            logger.info(
                "Temporal split",
                extra={"total": len(interactions), "train": len(train_ixs), "test": len(test_ixs)},
            )

            # ----------------------------------------------------------------
            # Step 2: Build candidate tables from FULL interaction history
            # ----------------------------------------------------------------
            collaborative = build_collaborative_neighbors(interactions)
            content = build_content_neighbors(items)
            trending = build_trending(interactions)

            session.execute(delete(ItemNeighbor))
            session.execute(delete(TrendingItem))

            session.add_all([
                ItemNeighbor(
                    source_item_id=src,
                    neighbor_item_id=nbr,
                    score=score,
                    algorithm=algo,
                )
                for src, nbr, score, algo in collaborative + content
            ])
            session.add_all([
                TrendingItem(region=region, item_id=item_id, score=score)
                for region, item_id, score in trending
            ])

            version = f"hybrid-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            session.add(ModelVersion(version=version, description="co-occurrence + tfidf hybrid"))
            session.flush()  # make neighbor rows visible for lookups below

            # ----------------------------------------------------------------
            # Step 3: Build in-memory score lookups from freshly written tables
            # ----------------------------------------------------------------
            collab_rows = session.execute(
                select(ItemNeighbor).where(ItemNeighbor.algorithm == "collaborative")
            ).scalars().all()
            content_rows = session.execute(
                select(ItemNeighbor).where(ItemNeighbor.algorithm == "content")
            ).scalars().all()
            trending_rows = session.execute(select(TrendingItem)).scalars().all()

            collab_map, content_map, trending_map = build_score_lookups(
                collab_rows, content_rows, trending_rows
            )
            item_map = {item.item_id: item for item in items}

            # ----------------------------------------------------------------
            # Step 4: Learn ranking weights from training split
            # ----------------------------------------------------------------
            X, y = build_feature_matrix(train_ixs, items, collab_map, content_map, trending_map)
            weights, sample_count = learn_ranking_weights(X, y)

            session.add(RankingWeights(
                model_version=version,
                collaborative=weights[0],
                content=weights[1],
                session=weights[2],
                trending=weights[3],
                freshness=weights[4],
                genre_bonus=weights[5],
                sample_count=sample_count,
            ))

            # ----------------------------------------------------------------
            # Step 5: Evaluate on held-out test split
            # ----------------------------------------------------------------
            eval_metrics = evaluate_model(
                test_ixs, train_ixs, collab_map, content_map, trending_map, item_map, weights
            )

            session.add(ModelEvaluation(
                model_version=version,
                ndcg_at_10=eval_metrics["ndcg_at_k"],
                hit_rate_at_10=eval_metrics["hit_rate_at_k"],
                coverage=eval_metrics["coverage"],
                test_user_count=eval_metrics["test_user_count"],
            ))

            session.commit()

            # ----------------------------------------------------------------
            # Step 6: Update Prometheus gauges
            # ----------------------------------------------------------------
            MODELS_TRAINED.inc()
            EVAL_NDCG_AT_10.set(eval_metrics["ndcg_at_k"])
            EVAL_HIT_RATE_AT_10.set(eval_metrics["hit_rate_at_k"])
            EVAL_COVERAGE.set(eval_metrics["coverage"])

            logger.info(
                "Training run complete",
                extra={
                    "version": version,
                    "collaborative_edges": len(collaborative),
                    "content_edges": len(content),
                    "trending_rows": len(trending),
                    "weight_samples": sample_count,
                    "learned_weights": dict(zip(WEIGHT_KEYS, [round(w, 4) for w in weights])),
                    "ndcg_at_10": round(eval_metrics["ndcg_at_k"], 4),
                    "hit_rate_at_10": round(eval_metrics["hit_rate_at_k"], 4),
                    "coverage": round(eval_metrics["coverage"], 4),
                    "test_users": eval_metrics["test_user_count"],
                },
            )


def main() -> None:
    threading.Thread(target=serve_metrics, daemon=True).start()
    wait_for_postgres()
    Base.metadata.create_all(bind=engine)
    while True:
        try:
            run_training_once()
        except Exception as exc:
            logger.exception("Training failed", extra={"error": str(exc)})
        time.sleep(60)


if __name__ == "__main__":
    main()
