import hashlib
import json
import pathlib
import time
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import desc, func, select

from common.config import settings
from common.db import SessionLocal, engine
from common.eval import WEIGHT_KEYS
from common.metrics import (
    COLD_START_REQUESTS,
    EXPERIMENT_REQUESTS,
    FEEDBACK_EVENTS,
    ONLINE_CTR_BY_POSITION,
    ONLINE_ENGAGEMENT_RATE,
    RECOMMENDATION_REQUESTS,
    REQUEST_LATENCY,
)
from common.models import Base, Experiment, Interaction, Item, User
from common.telemetry import instrument_fastapi

# Shared runtime singletons + constants
from .runtime import (
    SERVICE_NAME,
    _COLD_START_THRESHOLD,
    _engagement_window,
    _ONLINE_ENGAGEMENT_EVENTS,
    redis_client,
)
# Cohesive logic modules. Imported here so existing import paths
# (`from services.recommendation_api.app import <name>`) keep working.
from .caches import (
    _exp_cache,
    _exp_lock,
    _experiment_assignment,
    _load_active_experiment,
    _load_ranking_weights,
)
from .coldstart import _build_cold_start_response
from .ranking import (
    fetch_candidate_scores,
    fetch_session_candidates,
    get_latest_model_version,
    normalize_scores,
    rank_candidates,
    _serve_from_cache,
)
from .schemas import ExperimentConclude, ExperimentCreate, FeedbackEvent
from .stats import _z_test_two_proportions

_STATIC = pathlib.Path(__file__).parent / "static"


# ---------------------------------------------------------------------------
# FastAPI app setup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="Recommendation API", lifespan=lifespan)
instrument_fastapi(app, SERVICE_NAME)

# ---------------------------------------------------------------------------
# Optional API key auth — disabled when settings.api_key is empty
# ---------------------------------------------------------------------------

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _verify_api_key(key: str = Security(_API_KEY_HEADER)) -> None:
    if settings.api_key and key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def ui():
    return HTMLResponse((_STATIC / "index.html").read_text())


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.get("/recommendations/{user_id}", dependencies=[Depends(_verify_api_key)])
def recommendations(
    user_id: str,
    context: str = Query(default="home"),
    limit: int = Query(default=settings.recommendation_limit_default, ge=1, le=25),
    offset: int = Query(default=0, ge=0),
    device_type: str | None = Query(default=None),
):
    started = time.perf_counter()
    RECOMMENDATION_REQUESTS.labels(context=context).inc()

    with SessionLocal() as session:
        user = session.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Unknown user")

        recent_items: list[str] = redis_client.zrevrange(f"recent:{user_id}", 0, 9)

        # watched: ZSET keyed by timestamp — fetch all items watched within the rolling window
        watched: set[str] = set(redis_client.zrange(f"watched:{user_id}", 0, -1))

        # Priority: device-type weights → region weights → global weights
        weights = _load_ranking_weights(
            session,
            segment=user.region,
            primary=f"device:{device_type}" if device_type else None,
        )
        experiment_info: dict | None = None

        experiment = _load_active_experiment(session)
        if experiment:
            variant_weights, variant = _experiment_assignment(user_id, experiment)
            experiment_info = {"name": experiment.name, "variant": variant}
            EXPERIMENT_REQUESTS.labels(experiment=experiment.name, variant=variant).inc()
            if variant_weights is not None:
                weights = variant_weights

        is_cold_start = len(recent_items) < _COLD_START_THRESHOLD
        cached_hit = False
        if is_cold_start:
            COLD_START_REQUESTS.inc()
            recs = _build_cold_start_response(session, user, limit + offset)[offset:]
        else:
            # Prefer per-experiment precomputed cache for variant users;
            # fall back to regular precomputed cache; then live scoring.
            cached_raw = None
            if experiment_info and experiment_info["variant"] == "variant":
                cached_raw = redis_client.get(f"precomputed:{user_id}:exp:{experiment.name}")
            if cached_raw is None and not experiment_info:
                cached_raw = redis_client.get(f"precomputed:{user_id}")

            if cached_raw:
                recs = _serve_from_cache(
                    session, user, json.loads(cached_raw), watched, limit, offset
                )
                cached_hit = True
            else:
                collab, content, trending = fetch_candidate_scores(session, recent_items, user.region)
                session_scores = fetch_session_candidates(recent_items)
                recs = rank_candidates(
                    session, user, collab, content, session_scores, trending, watched, limit + offset, weights
                )[offset:]

        model_version = get_latest_model_version(session)

    for i, rec in enumerate(recs):
        rec["position"] = offset + i + 1

    elapsed = time.perf_counter() - started
    REQUEST_LATENCY.labels(endpoint="/recommendations/{user_id}").observe(elapsed)

    return {
        "user_id": user_id,
        "context": context,
        "offset": offset,
        "model_version": model_version,
        "is_cold_start": is_cold_start,
        "cached": cached_hit,
        "experiment": experiment_info,
        "weights": dict(zip(WEIGHT_KEYS, [round(w, 4) for w in weights])),
        "recent_items": recent_items,
        "recommendations": recs,
        "latency_ms": round(elapsed * 1000, 2),
    }


# ---------------------------------------------------------------------------
# Experiment management endpoints
# ---------------------------------------------------------------------------

@app.post("/experiments", status_code=201, dependencies=[Depends(_verify_api_key)])
def create_experiment(body: ExperimentCreate):
    try:
        validated = body.validated_weights()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    with SessionLocal() as session:
        existing = session.get(Experiment, body.name)
        if existing:
            raise HTTPException(status_code=409, detail=f"Experiment '{body.name}' already exists")

        for exp in session.execute(
            select(Experiment).where(Experiment.is_active.is_(True))
        ).scalars().all():
            exp.is_active = False

        experiment = Experiment(
            name=body.name,
            description=body.description,
            traffic_pct=body.traffic_pct,
            variant_weights=json.dumps(validated),
            is_active=True,
        )
        session.add(experiment)
        session.commit()

        with _exp_lock:
            _exp_cache["ts"] = 0.0

    return {"name": body.name, "traffic_pct": body.traffic_pct, "variant_weights": validated}


@app.get("/experiments")
def list_experiments():
    with SessionLocal() as session:
        rows = session.execute(
            select(Experiment).order_by(desc(Experiment.created_at))
        ).scalars().all()
        return [
            {
                "name": r.name,
                "description": r.description,
                "traffic_pct": r.traffic_pct,
                "variant_weights": json.loads(r.variant_weights or "{}"),
                "is_active": r.is_active,
                "created_at": r.created_at.isoformat(),
                "max_duration_days": r.max_duration_days,
                "concluded_at": r.concluded_at.isoformat() if r.concluded_at else None,
                "winning_variant": r.winning_variant,
                "conclusion_reason": r.conclusion_reason,
            }
            for r in rows
        ]


@app.post("/experiments/{name}/conclude", status_code=200, dependencies=[Depends(_verify_api_key)])
def conclude_experiment(name: str, body: ExperimentConclude):
    """Manually conclude an experiment, recording the winner and reason."""
    from datetime import datetime, timezone
    with SessionLocal() as session:
        experiment = session.get(Experiment, name)
        if not experiment:
            raise HTTPException(status_code=404, detail=f"Experiment '{name}' not found")

        experiment.is_active = False
        experiment.concluded_at = datetime.now(timezone.utc).replace(tzinfo=None)
        experiment.winning_variant = body.winning_variant
        experiment.conclusion_reason = body.conclusion_reason
        session.commit()

        with _exp_lock:
            _exp_cache["ts"] = 0.0

    return {
        "name": name,
        "is_active": False,
        "winning_variant": body.winning_variant,
        "conclusion_reason": body.conclusion_reason,
    }


@app.delete("/experiments/{name}", status_code=200, dependencies=[Depends(_verify_api_key)])
def deactivate_experiment(name: str):
    with SessionLocal() as session:
        experiment = session.get(Experiment, name)
        if not experiment:
            raise HTTPException(status_code=404, detail=f"Experiment '{name}' not found")
        experiment.is_active = False
        session.commit()

        with _exp_lock:
            _exp_cache["ts"] = 0.0

    return {"name": name, "is_active": False}


@app.get("/experiments/{name}/results")
def experiment_results(name: str):
    """
    Engagement and CTR lift between control and variant since the experiment started.

    Aggregates via SQL GROUP BY to avoid loading raw interaction rows —
    safe to call on long-running experiments with millions of events.
    Uses a two-proportion z-test; |z| >= 1.96 is p < 0.05.
    """
    _STRONG_ENGAGEMENT = frozenset({"play_start", "complete", "watchlist_add"})

    with SessionLocal() as session:
        experiment = session.get(Experiment, name)
        if not experiment:
            raise HTTPException(status_code=404, detail=f"Experiment '{name}' not found")

        _WATCH_EVENTS = frozenset({"play_start", "watch_progress", "complete"})

        # Aggregate in the database — (user_id, event_type, count, total_watch_s) instead of raw rows
        agg_rows = session.execute(
            select(
                Interaction.user_id,
                Interaction.event_type,
                func.count().label("cnt"),
                func.sum(Interaction.watch_seconds).label("total_watch_s"),
            )
            .where(Interaction.event_ts >= experiment.created_at)
            .group_by(Interaction.user_id, Interaction.event_type)
        ).all()

        empty_bucket: dict = {
            "users": 0, "impressions": 0, "engagements": 0,
            "clicks": 0, "engagement_rate": 0.0, "ctr": 0.0,
            "avg_watch_seconds": 0.0,
        }
        if not agg_rows:
            return {
                "name": name,
                "is_active": experiment.is_active,
                "traffic_pct": experiment.traffic_pct,
                "since": experiment.created_at.isoformat(),
                "control": empty_bucket,
                "variant": empty_bucket,
                "engagement_lift_pct": 0.0,
                "engagement_z_score": 0.0,
                "ctr_lift_pct": 0.0,
                "ctr_z_score": 0.0,
                "watch_time_lift_pct": 0.0,
                "significant": False,
            }

        # Bucket users deterministically (same hash as recommendation endpoint)
        user_buckets: dict[str, str] = {}
        for row in agg_rows:
            if row.user_id not in user_buckets:
                bucket = int(hashlib.md5(f"{row.user_id}:{name}".encode()).hexdigest(), 16) % 100
                user_buckets[row.user_id] = "variant" if bucket < experiment.traffic_pct else "control"

        stats: dict[str, dict] = {
            "control": {"users": set(), "impressions": 0, "engagements": 0, "clicks": 0,
                        "total_watch_s": 0, "play_events": 0},
            "variant": {"users": set(), "impressions": 0, "engagements": 0, "clicks": 0,
                        "total_watch_s": 0, "play_events": 0},
        }
        for row in agg_rows:
            b = user_buckets[row.user_id]
            stats[b]["users"].add(row.user_id)
            if row.event_type == "impression":
                stats[b]["impressions"] += row.cnt
            elif row.event_type == "click":
                stats[b]["clicks"] += row.cnt
            elif row.event_type in _STRONG_ENGAGEMENT:
                stats[b]["engagements"] += row.cnt
            if row.event_type in _WATCH_EVENTS:
                stats[b]["total_watch_s"] += int(row.total_watch_s or 0)
                stats[b]["play_events"] += row.cnt

        def _summarize(s: dict) -> dict:
            imp = s["impressions"]
            plays = s["play_events"]
            return {
                "users": len(s["users"]),
                "impressions": imp,
                "engagements": s["engagements"],
                "clicks": s["clicks"],
                "engagement_rate": round(s["engagements"] / imp, 4) if imp else 0.0,
                "ctr": round(s["clicks"] / imp, 4) if imp else 0.0,
                "avg_watch_seconds": round(s["total_watch_s"] / plays, 2) if plays else 0.0,
            }

        ctrl, var = stats["control"], stats["variant"]
        eng_lift, eng_z = _z_test_two_proportions(
            ctrl["impressions"], ctrl["engagements"],
            var["impressions"], var["engagements"],
        )
        ctr_lift, ctr_z = _z_test_two_proportions(
            ctrl["impressions"], ctrl["clicks"],
            var["impressions"], var["clicks"],
        )

        ctrl_avg_watch = ctrl["total_watch_s"] / ctrl["play_events"] if ctrl["play_events"] else 0.0
        var_avg_watch = var["total_watch_s"] / var["play_events"] if var["play_events"] else 0.0
        watch_time_lift_pct = round(
            (var_avg_watch - ctrl_avg_watch) / ctrl_avg_watch * 100, 2
        ) if ctrl_avg_watch else 0.0

        return {
            "name": name,
            "is_active": experiment.is_active,
            "traffic_pct": experiment.traffic_pct,
            "since": experiment.created_at.isoformat(),
            "control": _summarize(ctrl),
            "variant": _summarize(var),
            "engagement_lift_pct": eng_lift,
            "engagement_z_score": eng_z,
            "ctr_lift_pct": ctr_lift,
            "ctr_z_score": ctr_z,
            "watch_time_lift_pct": watch_time_lift_pct,
            "significant": abs(eng_z) >= 1.96,
        }


# ---------------------------------------------------------------------------
# Feedback endpoint
# ---------------------------------------------------------------------------

@app.post("/feedback", status_code=201, dependencies=[Depends(_verify_api_key)])
def submit_feedback(body: FeedbackEvent):
    from datetime import datetime, timezone
    with SessionLocal() as session:
        if not session.get(User, body.user_id):
            raise HTTPException(status_code=404, detail=f"Unknown user '{body.user_id}'")
        user = session.get(User, body.user_id)
        if not session.get(Item, body.item_id):
            raise HTTPException(status_code=404, detail=f"Unknown item '{body.item_id}'")

        event_id = f"fb_{uuid4()}"
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        session.add(Interaction(
            event_id=event_id,
            user_id=body.user_id,
            item_id=body.item_id,
            event_type=body.event_type,
            watch_seconds=body.watch_seconds,
            completion_pct=body.completion_pct,
            position=body.position,
            region=user.region,
            device_type="feedback",
            event_ts=now,
            ingestion_ts=now,
        ))
        session.commit()

    FEEDBACK_EVENTS.labels(event_type=body.event_type).inc()

    if body.event_type == "click" and body.position >= 1:
        if body.position == 1:
            bucket = "1"
        elif body.position <= 3:
            bucket = "2-3"
        elif body.position <= 5:
            bucket = "4-5"
        else:
            bucket = "6-10"
        ONLINE_CTR_BY_POSITION.labels(position_bucket=bucket).observe(1.0)

    _engagement_window.append(1 if body.event_type in _ONLINE_ENGAGEMENT_EVENTS else 0)
    ONLINE_ENGAGEMENT_RATE.set(sum(_engagement_window) / len(_engagement_window))

    return {"status": "ok", "event_id": event_id}


# ---------------------------------------------------------------------------
# Users listing endpoint
# ---------------------------------------------------------------------------

@app.get("/users")
def list_users(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    region: str | None = Query(default=None),
):
    with SessionLocal() as session:
        q = select(User)
        count_q = select(func.count()).select_from(User)
        if region:
            q = q.where(User.region == region)
            count_q = count_q.where(User.region == region)
        total = session.execute(count_q).scalar_one()
        users = session.execute(q.offset(offset).limit(limit)).scalars().all()
        return {
            "users": [
                {"user_id": u.user_id, "region": u.region, "maturity_rating": u.maturity_rating}
                for u in users
            ],
            "total": total,
            "offset": offset,
            "limit": limit,
        }
