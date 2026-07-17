import hashlib
import json
import logging
import pathlib
import threading
import time
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from typing import Any

from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import desc, select

from common.config import settings
from common.db import SessionLocal, engine
from common.eval import DEFAULT_WEIGHTS, WEIGHT_KEYS
from common.logging_utils import configure_logging
from common.metrics import (
    CANDIDATES_GENERATED,
    COLD_START_REQUESTS,
    EXPERIMENT_REQUESTS,
    RECOMMENDATION_REQUESTS,
    REQUEST_LATENCY,
)
from common.models import (
    Base,
    Experiment,
    Item,
    ItemNeighbor,
    ModelVersion,
    RankingWeights,
    TrendingItem,
    User,
)
from common.redis_client import get_redis
from common.telemetry import instrument_fastapi

SERVICE_NAME = "recommendation-api"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
redis_client = get_redis()

_STATIC = pathlib.Path(__file__).parent / "static"

MATURE_ORDER = {"G": 0, "PG": 1, "PG-13": 2, "R": 3}

# Users with fewer than this many items in their recent set are treated as cold-start
_COLD_START_THRESHOLD = 3
# How long to cache DB-sourced config (weights, experiments) before re-reading
_CACHE_TTL_SECONDS = 60.0


# ---------------------------------------------------------------------------
# In-memory caches for ranking weights and active experiment
# ---------------------------------------------------------------------------

_weights_cache: dict = {"data": DEFAULT_WEIGHTS[:], "ts": 0.0}
_weights_lock = threading.Lock()

_exp_cache: dict = {"data": None, "ts": 0.0}
_exp_lock = threading.Lock()


def _load_ranking_weights(session) -> list[float]:
    """Return the latest learned ranking weights, falling back to defaults."""
    now = time.monotonic()
    with _weights_lock:
        if now - _weights_cache["ts"] > _CACHE_TTL_SECONDS:
            row = session.execute(
                select(RankingWeights).order_by(desc(RankingWeights.created_at))
            ).scalars().first()
            _weights_cache["data"] = (
                [row.collaborative, row.content, row.session,
                 row.trending, row.freshness, row.genre_bonus]
                if row else DEFAULT_WEIGHTS[:]
            )
            _weights_cache["ts"] = now
    return _weights_cache["data"]


def _load_active_experiment(session):
    """Return the currently active Experiment row, or None."""
    now = time.monotonic()
    with _exp_lock:
        if now - _exp_cache["ts"] > _CACHE_TTL_SECONDS:
            _exp_cache["data"] = session.execute(
                select(Experiment)
                .where(Experiment.is_active.is_(True))
                .order_by(desc(Experiment.created_at))
            ).scalars().first()
            _exp_cache["ts"] = now
    return _exp_cache["data"]


def _experiment_assignment(user_id: str, experiment: Experiment) -> tuple[list[float], str]:
    """
    Deterministically assign a user to control or variant based on a hash of
    their user_id.  Returns (weights_to_use, variant_name).
    """
    bucket = int(hashlib.md5(user_id.encode()).hexdigest(), 16) % 100
    if bucket < experiment.traffic_pct:
        raw = json.loads(experiment.variant_weights or "{}")
        if len(raw) == len(WEIGHT_KEYS) and all(k in raw for k in WEIGHT_KEYS):
            return [raw[k] for k in WEIGHT_KEYS], "variant"
    return None, "control"


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
# Pure helper functions
# ---------------------------------------------------------------------------

def normalize_scores(score_map: dict[str, float]) -> dict[str, float]:
    if not score_map:
        return {}
    max_score = max(score_map.values()) or 1.0
    return {k: v / max_score for k, v in score_map.items()}


def get_latest_model_version(session) -> str:
    version = session.execute(
        select(ModelVersion).order_by(desc(ModelVersion.created_at))
    ).scalars().first()
    return version.version if version else "untrained"


def fetch_candidate_scores(session, recent_items: list[str], region: str):
    collab: Counter = Counter()
    content: Counter = Counter()
    trending: Counter = Counter()

    if recent_items:
        for row in session.execute(
            select(ItemNeighbor).where(
                ItemNeighbor.source_item_id.in_(recent_items),
                ItemNeighbor.algorithm == "collaborative",
            )
        ).scalars().all():
            collab[row.neighbor_item_id] += row.score

        for row in session.execute(
            select(ItemNeighbor).where(
                ItemNeighbor.source_item_id.in_(recent_items),
                ItemNeighbor.algorithm == "content",
            )
        ).scalars().all():
            content[row.neighbor_item_id] += row.score

    for row in session.execute(
        select(TrendingItem).where(TrendingItem.region == region)
    ).scalars().all():
        trending[row.item_id] = row.score

    return normalize_scores(dict(collab)), normalize_scores(dict(content)), normalize_scores(dict(trending))


def fetch_session_candidates(recent_items: list[str]) -> dict[str, float]:
    # Session signal is a simple recency boost derived from the last 5 titles.
    session_scores: dict[str, float] = {}
    weight = 1.0
    for item_id in reversed(recent_items[-5:]):
        session_scores[item_id] = session_scores.get(item_id, 0.0) + weight
        weight *= 0.7
    return normalize_scores(session_scores)


def _genre_jaccard(genres_a: str, genres_b: str) -> float:
    """Jaccard similarity between two comma-separated genre strings."""
    a = {g.strip() for g in genres_a.split(",")}
    b = {g.strip() for g in genres_b.split(",")}
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def _mmr_rerank(
    scored: list[tuple],
    limit: int,
    lambda_mmr: float = 0.7,
) -> list[tuple]:
    """
    Maximal Marginal Relevance re-ranking.

    Balances relevance (the pre-computed score) against diversity (genre
    dissimilarity to already-selected items).  lambda_mmr=1.0 is pure
    relevance; lambda_mmr=0.0 is pure diversity.
    """
    if not scored:
        return []

    selected: list[tuple] = []
    remaining = list(scored)

    while remaining and len(selected) < limit:
        if not selected:
            selected.append(remaining.pop(0))
            continue

        best_mmr = -float("inf")
        best_idx = 0
        for idx, (item, score, reason, components) in enumerate(remaining):
            max_sim = max(
                _genre_jaccard(item.genres, s[0].genres) for s in selected
            )
            mmr_score = lambda_mmr * score - (1.0 - lambda_mmr) * max_sim
            if mmr_score > best_mmr:
                best_mmr = mmr_score
                best_idx = idx

        selected.append(remaining.pop(best_idx))

    return selected


def rank_candidates(
    session,
    user: User,
    collab: dict[str, float],
    content: dict[str, float],
    session_scores: dict[str, float],
    trending: dict[str, float],
    watched: set[str],
    limit: int,
    weights: list[float],
) -> list[dict[str, Any]]:
    genre_affinity = {
        k: float(v)
        for k, v in redis_client.hgetall(f"genre_affinity:{user.user_id}").items()
    }

    # Personalized trending: blend DB trending with real-time Redis popularity
    # weighted by the user's genre affinity so popular items in preferred genres
    # score higher than cold regional trending items.
    pop_raw = dict(redis_client.zrange(f"popular:{user.region}", 0, -1, withscores=True))
    pop_max = max(pop_raw.values(), default=1.0) or 1.0

    candidate_ids = set(collab) | set(content) | set(session_scores) | set(trending) | set(pop_raw)
    CANDIDATES_GENERATED.labels(source="merged").observe(len(candidate_ids))

    items = (
        session.execute(select(Item).where(Item.item_id.in_(candidate_ids))).scalars().all()
        if candidate_ids else []
    )
    item_map = {item.item_id: item for item in items}
    scored = []

    for item_id in candidate_ids:
        if item_id in watched:
            continue
        item = item_map.get(item_id)
        if item is None or not item.is_active:
            continue
        if user.region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue
        if MATURE_ORDER.get(item.maturity_rating, 0) > MATURE_ORDER.get(user.maturity_rating, 2):
            continue

        genre_bonus = min(
            sum(genre_affinity.get(g.strip(), 0.0) for g in item.genres.split(",")) / 10.0,
            1.0,
        )
        freshness = min(max((item.release_year - 2020) / 6.0, 0.0), 1.0)

        # Blend DB trending (batch-updated by trainer) with real-time Redis
        # popularity personalised by genre affinity.
        rt_pop = (pop_raw.get(item_id, 0.0) / pop_max) * (1.0 + 0.5 * genre_bonus)
        blended_trending = 0.6 * trending.get(item_id, 0.0) + 0.4 * min(rt_pop, 1.0)

        w = weights
        score = (
            w[0] * collab.get(item_id, 0.0)
            + w[1] * content.get(item_id, 0.0)
            + w[2] * session_scores.get(item_id, 0.0)
            + w[3] * blended_trending
            + w[4] * freshness
            + w[5] * genre_bonus
        )
        components = {
            "collaborative": collab.get(item_id, 0.0),
            "content": content.get(item_id, 0.0),
            "session": session_scores.get(item_id, 0.0),
            "trending": round(blended_trending, 4),
            "freshness": freshness,
            "genre_bonus": genre_bonus,
        }
        reason = max(components, key=components.get)
        scored.append((item, score, reason, components))

    scored.sort(key=lambda x: x[1], reverse=True)

    # MMR re-ranking: pick top 3× candidates then apply diversity re-ranking
    # so the final list balances relevance with genre variety.
    reranked = _mmr_rerank(scored[: limit * 3], limit=limit, lambda_mmr=0.7)

    return [
        {
            "item_id": item.item_id,
            "title": item.title,
            "genres": item.genres.split(","),
            "score": round(score, 4),
            "reason": reason,
            "components": {k: round(v, 4) for k, v in components.items()},
        }
        for item, score, reason, components in reranked
    ]


# ---------------------------------------------------------------------------
# Cold-start path
# ---------------------------------------------------------------------------

def _build_cold_start_response(session, user: User, limit: int) -> list[dict[str, Any]]:
    """
    For users with no watch history, serve the regional trending list filtered
    by region availability and maturity rating.  Absolute fallback (no trending
    data yet) is the newest active items in the catalog.
    """
    trending_rows = session.execute(
        select(TrendingItem)
        .where(TrendingItem.region == user.region)
        .order_by(desc(TrendingItem.score))
        .limit(limit * 3)
    ).scalars().all()

    if not trending_rows:
        # Pre-trainer fallback: serve freshest items
        raw_items = session.execute(
            select(Item)
            .where(Item.is_active.is_(True))
            .order_by(desc(Item.release_year))
            .limit(limit * 3)
        ).scalars().all()
        item_map = {i.item_id: i for i in raw_items}
        candidates = [(i.item_id, 0.0) for i in raw_items]
    else:
        item_ids = [r.item_id for r in trending_rows]
        items = session.execute(select(Item).where(Item.item_id.in_(item_ids))).scalars().all()
        item_map = {i.item_id: i for i in items}
        candidates = [(r.item_id, r.score) for r in trending_rows]

    results = []
    for item_id, raw_score in candidates:
        item = item_map.get(item_id)
        if not item or not item.is_active:
            continue
        if user.region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue
        if MATURE_ORDER.get(item.maturity_rating, 0) > MATURE_ORDER.get(user.maturity_rating, 2):
            continue
        trend_score = round(raw_score, 4)
        freshness = round(min(max((item.release_year - 2020) / 6.0, 0.0), 1.0), 4)
        results.append({
            "item_id": item.item_id,
            "title": item.title,
            "genres": item.genres.split(","),
            "score": trend_score,
            "reason": "trending",
            "components": {
                "collaborative": 0.0,
                "content": 0.0,
                "session": 0.0,
                "trending": trend_score,
                "freshness": freshness,
                "genre_bonus": 0.0,
            },
        })
        if len(results) >= limit:
            break
    return results


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class FeedbackEvent(BaseModel):
    user_id: str
    item_id: str
    event_type: str
    watch_seconds: int = 0
    completion_pct: float = 0.0

class ExperimentCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    description: str = ""
    traffic_pct: int = Field(..., ge=1, le=99)
    variant_weights: dict[str, float] = Field(
        ...,
        description="Must contain all six keys: collaborative, content, session, "
                    "trending, freshness, genre_bonus.  Values must be ≥ 0 and sum to 1.",
    )

    def validated_weights(self) -> dict[str, float]:
        missing = set(WEIGHT_KEYS) - set(self.variant_weights)
        if missing:
            raise ValueError(f"Missing weight keys: {missing}")
        total = sum(self.variant_weights.values())
        if not (0.99 <= total <= 1.01):
            raise ValueError(f"Weights must sum to 1.0, got {total:.4f}")
        if any(v < 0 for v in self.variant_weights.values()):
            raise ValueError("All weights must be ≥ 0")
        return {k: self.variant_weights[k] for k in WEIGHT_KEYS}


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
):
    started = time.perf_counter()
    RECOMMENDATION_REQUESTS.labels(context=context).inc()

    with SessionLocal() as session:
        user = session.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Unknown user")

        recent_items: list[str] = redis_client.zrevrange(f"recent:{user_id}", 0, 9)
        watched: set[str] = set(redis_client.smembers(f"watched:{user_id}"))

        weights = _load_ranking_weights(session)
        experiment_info: dict | None = None

        # A/B assignment — overrides learned weights if user falls in the variant bucket
        experiment = _load_active_experiment(session)
        if experiment:
            variant_weights, variant = _experiment_assignment(user_id, experiment)
            experiment_info = {"name": experiment.name, "variant": variant}
            EXPERIMENT_REQUESTS.labels(experiment=experiment.name, variant=variant).inc()
            if variant_weights is not None:
                weights = variant_weights

        # Cold-start path
        is_cold_start = len(recent_items) < _COLD_START_THRESHOLD
        if is_cold_start:
            COLD_START_REQUESTS.inc()
            recs = _build_cold_start_response(session, user, limit + offset)[offset:]
        else:
            collab, content, trending = fetch_candidate_scores(session, recent_items, user.region)
            session_scores = fetch_session_candidates(recent_items)
            recs = rank_candidates(
                session, user, collab, content, session_scores, trending, watched, limit + offset, weights
            )[offset:]

        model_version = get_latest_model_version(session)

    elapsed = time.perf_counter() - started
    REQUEST_LATENCY.labels(endpoint="/recommendations/{user_id}").observe(elapsed)

    return {
        "user_id": user_id,
        "context": context,
        "offset": offset,
        "model_version": model_version,
        "is_cold_start": is_cold_start,
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

        # Deactivate all other experiments before activating the new one
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

        # Invalidate experiment cache
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
            }
            for r in rows
        ]


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


# ---------------------------------------------------------------------------
# Feedback endpoint — write explicit user signals to the interactions table
# ---------------------------------------------------------------------------

@app.post("/feedback", status_code=201, dependencies=[Depends(_verify_api_key)])
def submit_feedback(body: FeedbackEvent):
    from datetime import datetime
    with SessionLocal() as session:
        if not session.get(User, body.user_id):
            raise HTTPException(status_code=404, detail=f"Unknown user '{body.user_id}'")
        user = session.get(User, body.user_id)
        if not session.get(Item, body.item_id):
            raise HTTPException(status_code=404, detail=f"Unknown item '{body.item_id}'")

        event_id = f"fb_{uuid4()}"
        now = datetime.utcnow()
        session.add(Interaction(
            event_id=event_id,
            user_id=body.user_id,
            item_id=body.item_id,
            event_type=body.event_type,
            watch_seconds=body.watch_seconds,
            completion_pct=body.completion_pct,
            region=user.region,
            device_type="feedback",
            event_ts=now,
            ingestion_ts=now,
        ))
        session.commit()

    return {"status": "ok", "event_id": event_id}


# ---------------------------------------------------------------------------
# Users listing endpoint — used by the UI to populate the user selector
# ---------------------------------------------------------------------------

@app.get("/users")
def list_users(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    region: str | None = Query(default=None),
):
    from sqlalchemy import func
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
