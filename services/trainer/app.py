import signal
import threading
from collections import defaultdict
from datetime import timedelta

from sqlalchemy import desc, select

from common.ann import build_ann_index
from common.config import settings
from common.db import SessionLocal, engine
from common.eval import (
    DEFAULT_WEIGHTS,
    WEIGHT_KEYS,
    evaluate_model,
    interaction_weight,  # re-exported for tests
    temporal_split,
)
from common.metrics import (
    ATTRIBUTED_WATCH_TIME_SECONDS,
    AVG_ATTRIBUTED_WATCH_SECONDS,
    COMPLETION_RATE,
    EVAL_COVERAGE,
    EVAL_COVERAGE_DELTA,
    EVAL_HIT_RATE_AT_10,
    EVAL_MRR_AT_10,
    EVAL_NDCG_AT_10,
    EVAL_WATCH_TIME_NDCG_AT_10,
    EVENT_VOLUME,
    MODELS_TRAINED,
    RETENTION_7DAY,
    TRAINING_DATA_QUALITY_FAILURES,
    WEIGHT_ROLLBACKS,
)
from common.models import (
    Base,
    Interaction,
    Item,
    ModelEvaluation,
    ModelVersion,
    RankingWeights,
)

# Shared runtime singletons, lifecycle helpers, and constants.
from .runtime import (
    _MIN_SEGMENT_INTERACTIONS,
    _handle_signal,
    _stop,
    _utcnow,
    logger,
    serve_metrics,
    tracer,
    wait_for_postgres,
)
# Cohesive logic modules. Imported here so existing import paths
# (`from services.trainer.app import <name>`) keep working.
from .artifacts import _save_ann_artifacts, _save_model_artifacts
from .business import _compute_business_metrics
from .experiments import _auto_conclude_experiments
from .persistence import _upsert_neighbors, _upsert_trending, _validate_training_data
from .ranking import (
    _learn_lr_weights,
    _precompute_user_recommendations,
    _score_user_candidates_offline,
)
from .signals import (
    build_collaborative_neighbors,
    build_content_neighbors,
    build_mf_neighbors,
    build_trending,
)


# ---------------------------------------------------------------------------
# Training entry point
# ---------------------------------------------------------------------------

def run_training_once() -> None:
    with tracer.start_as_current_span("training_run"):
        # Locals that need to survive past the first session block
        item_map: dict = {}
        collab_map: dict = {}
        content_map: dict = {}
        trending_map: dict = {}
        serving_weights = DEFAULT_WEIGHTS[:]
        segment_weights_map: dict = {}

        with SessionLocal() as session:
            items = session.execute(select(Item).where(Item.is_active.is_(True))).scalars().all()
            lookback_cutoff = _utcnow() - timedelta(days=settings.training_lookback_days)
            interactions = session.execute(
                select(Interaction).where(Interaction.event_ts >= lookback_cutoff)
            ).scalars().all()

            # Emit distribution-shift gauges before the quality gate so alerts
            # fire even when training is aborted by the low-completion-rate check.
            _play_starts = sum(1 for ix in interactions if ix.event_type == "play_start")
            _completes = sum(1 for ix in interactions if ix.event_type == "complete")
            COMPLETION_RATE.set(_completes / _play_starts if _play_starts > 0 else 0.0)
            EVENT_VOLUME.set(len(interactions))

            _validate_training_data(interactions, items)

            # Split BEFORE building neighbour graphs to prevent eval data from
            # leaking into the collab signal used during evaluation.
            train_ixs, eval_ixs = temporal_split(interactions)

            # --- Build signal models from train split only ---
            collaborative_cooc = build_collaborative_neighbors(train_ixs)
            collaborative_mf, item_embeddings = build_mf_neighbors(train_ixs)
            content, content_vectorizer = build_content_neighbors(items)
            # Trending is operational (time-windowed), not eval-sensitive — use all interactions
            trending = build_trending(interactions)

            # Blend co-occurrence and MF collaborative signals by averaging shared pairs
            cooc_map_raw: dict[tuple, float] = {(s, n): sc for s, n, sc, _ in collaborative_cooc}
            mf_map_raw: dict[tuple, float] = {(s, n): sc for s, n, sc, _ in collaborative_mf}
            all_pairs = set(cooc_map_raw) | set(mf_map_raw)
            collaborative = []
            for (src, nbr) in all_pairs:
                scores = [s for s in [cooc_map_raw.get((src, nbr)), mf_map_raw.get((src, nbr))] if s is not None]
                avg = round(sum(scores) / len(scores), 6)
                collaborative.append((src, nbr, avg, "collaborative"))

            # --- Upsert neighbours/trending (zero-blackout swap) ---
            run_ts = _utcnow()
            _upsert_neighbors(session, collaborative + content, run_ts)
            _upsert_trending(session, trending, run_ts)

            # --- Build in-memory lookup maps ---
            for src, nbr, score, _ in collaborative:
                collab_map.setdefault(src, {})[nbr] = score
            for src, nbr, score, _ in content:
                content_map.setdefault(src, {})[nbr] = score
            for region, item_id, score in trending:
                trending_map.setdefault(region, {})[item_id] = score
            item_map = {item.item_id: item for item in items}

            # --- Learn global ranking weights via logistic regression ---
            weights, sample_count, scaler, lr = _learn_lr_weights(
                train_ixs, items, collab_map, content_map, trending_map
            )
            if sample_count >= 50:
                logger.info(
                    "LR weights learned",
                    extra={"sample_count": sample_count, "weights": dict(zip(WEIGHT_KEYS, weights))},
                )
            else:
                logger.info(
                    "Insufficient data for LR weight learning, using defaults",
                    extra={"sample_count": sample_count},
                )

            # --- Per-segment (per-region) weight learning ---
            region_ixs_map: dict[str, list] = defaultdict(list)
            for ix in train_ixs:
                region_ixs_map[ix.region].append(ix)

            for region, region_train_ixs in region_ixs_map.items():
                if len(region_train_ixs) < _MIN_SEGMENT_INTERACTIONS:
                    continue
                seg_weights, seg_count, _, _ = _learn_lr_weights(
                    region_train_ixs, items, collab_map, content_map, trending_map
                )
                if seg_count >= 50:
                    segment_weights_map[region] = seg_weights
                    logger.info(
                        "Per-segment LR weights learned",
                        extra={"region": region, "sample_count": seg_count,
                               "weights": dict(zip(WEIGHT_KEYS, seg_weights))},
                    )

            # --- Offline evaluation ---
            eval_result = evaluate_model(
                eval_ixs, train_ixs, collab_map, content_map, trending_map, item_map, weights
            )

            # --- Business metrics (trailing 7-day window, uses same session) ---
            business_metrics = _compute_business_metrics(session)

            # --- Regression guard: only deploy weights when NDCG hasn't dropped ---
            prev_eval = session.execute(
                select(ModelEvaluation).order_by(desc(ModelEvaluation.created_at))
            ).scalars().first()

            EVAL_COVERAGE_DELTA.set(
                eval_result["coverage"] - prev_eval.coverage if prev_eval else 0.0
            )

            deploy_weights = True
            serving_weights = weights[:]
            if prev_eval and prev_eval.ndcg_at_10 > 0.0:
                if eval_result["ndcg_at_k"] < prev_eval.ndcg_at_10 * 0.95:
                    logger.warning(
                        "NDCG regressed vs previous run — keeping existing weights",
                        extra={
                            "new_ndcg": round(eval_result["ndcg_at_k"], 4),
                            "prev_ndcg": round(prev_eval.ndcg_at_10, 4),
                        },
                    )
                    deploy_weights = False
                    WEIGHT_ROLLBACKS.inc()
                    prev_row = session.execute(
                        select(RankingWeights)
                        .where(RankingWeights.segment.is_(None))
                        .order_by(desc(RankingWeights.created_at))
                    ).scalars().first()
                    if prev_row:
                        serving_weights = [
                            prev_row.collaborative, prev_row.content, prev_row.session,
                            prev_row.trending, prev_row.freshness, prev_row.genre_bonus,
                        ]
                    else:
                        serving_weights = DEFAULT_WEIGHTS[:]

            # --- Persist version metadata, weights, and eval results ---
            version = f"hybrid-{_utcnow().strftime('%Y%m%d%H%M%S')}"
            session.add(ModelVersion(version=version, description="co-occurrence+mf+tfidf+lr-weights"))
            if deploy_weights:
                # Global weights (segment=None)
                session.add(
                    RankingWeights(
                        model_version=version,
                        segment=None,
                        collaborative=weights[0],
                        content=weights[1],
                        session=weights[2],
                        trending=weights[3],
                        freshness=weights[4],
                        genre_bonus=weights[5],
                        sample_count=sample_count,
                    )
                )
                # Per-segment weights
                for region, seg_w in segment_weights_map.items():
                    region_count = len(region_ixs_map.get(region, []))
                    session.add(
                        RankingWeights(
                            model_version=version,
                            segment=region,
                            collaborative=seg_w[0],
                            content=seg_w[1],
                            session=seg_w[2],
                            trending=seg_w[3],
                            freshness=seg_w[4],
                            genre_bonus=seg_w[5],
                            sample_count=region_count,
                        )
                    )

            session.add(
                ModelEvaluation(
                    model_version=version,
                    ndcg_at_10=eval_result["ndcg_at_k"],
                    hit_rate_at_10=eval_result["hit_rate_at_k"],
                    mrr_at_10=eval_result["mrr_at_k"],
                    watch_time_ndcg_at_10=eval_result["watch_time_ndcg_at_k"],
                    coverage=eval_result["coverage"],
                    test_user_count=eval_result["test_user_count"],
                    retention_7d=business_metrics["retention_7d"],
                    avg_attributed_watch_s=business_metrics["avg_attributed_watch_s"],
                )
            )
            # Commit all DB changes before the (potentially slow) Redis precompute
            session.commit()

            # Auto-conclude experiments that have exceeded their max_duration_days
            _auto_conclude_experiments(session)

        # --- Save model artifacts to object storage (non-critical, outside session) ---
        _save_model_artifacts(version, content_vectorizer, scaler, lr)
        ann_index, ann_id_list = build_ann_index(item_embeddings)
        _save_ann_artifacts(version, ann_index, ann_id_list)

        # --- Update Prometheus gauges ---
        EVAL_NDCG_AT_10.set(eval_result["ndcg_at_k"])
        EVAL_HIT_RATE_AT_10.set(eval_result["hit_rate_at_k"])
        EVAL_MRR_AT_10.set(eval_result["mrr_at_k"])
        EVAL_WATCH_TIME_NDCG_AT_10.set(eval_result["watch_time_ndcg_at_k"])
        EVAL_COVERAGE.set(eval_result["coverage"])
        RETENTION_7DAY.set(business_metrics["retention_7d"])
        AVG_ATTRIBUTED_WATCH_SECONDS.set(business_metrics["avg_attributed_watch_s"])
        if business_metrics["total_attributed_watch_s"] > 0:
            ATTRIBUTED_WATCH_TIME_SECONDS.inc(business_metrics["total_attributed_watch_s"])
        MODELS_TRAINED.inc()

        # --- Pre-compute per-user top-K for low-latency serving (fresh session) ---
        with SessionLocal() as precompute_session:
            _precompute_user_recommendations(
                precompute_session, item_map, collab_map, content_map, trending_map,
                serving_weights, segment_weights_map=segment_weights_map,
            )

        logger.info(
            "Completed training run",
            extra={
                "version": version,
                "collaborative_edges": len(collaborative),
                "content_edges": len(content),
                "trending_rows": len(trending),
                "mf_edges": len(collaborative_mf),
                "ndcg_at_10": round(eval_result["ndcg_at_k"], 4),
                "hit_rate_at_10": round(eval_result["hit_rate_at_k"], 4),
                "mrr_at_10": round(eval_result["mrr_at_k"], 4),
                "watch_time_ndcg_at_10": round(eval_result["watch_time_ndcg_at_k"], 4),
                "coverage": round(eval_result["coverage"], 4),
                "retention_7d": business_metrics["retention_7d"],
                "avg_attributed_watch_s": business_metrics["avg_attributed_watch_s"],
                "prior_week_users": business_metrics["prior_week_users"],
                "returned_users": business_metrics["returned_users"],
                "weights_deployed": deploy_weights,
                "segments_trained": list(segment_weights_map.keys()),
                "serving_weights": dict(zip(WEIGHT_KEYS, serving_weights)),
            },
        )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    threading.Thread(target=serve_metrics, daemon=True).start()
    wait_for_postgres()
    Base.metadata.create_all(bind=engine)
    backoff = 60
    while not _stop.is_set():
        try:
            run_training_once()
            backoff = 60
        except ValueError as exc:
            TRAINING_DATA_QUALITY_FAILURES.labels(reason=str(exc)[:64]).inc()
            logger.error("Training aborted: data quality failure", extra={"reason": str(exc)})
            backoff = min(backoff * 2, 3600)
            logger.info("Retrying after backoff", extra={"seconds": backoff})
        except Exception as exc:
            logger.exception("Training failed", extra={"error": str(exc)})
            backoff = min(backoff * 2, 3600)
            logger.info("Retrying after backoff", extra={"seconds": backoff})
        _stop.wait(timeout=backoff)


if __name__ == "__main__":
    main()
