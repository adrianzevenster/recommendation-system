"""
Offline evaluation utilities shared by the trainer and tests.

Responsibilities:
  - Ranking metrics: NDCG@K, hit-rate@K, catalog coverage
  - Score lookup table construction from ORM rows
  - Offline candidate generation (mirrors the API ranking without Redis)
  - Feature matrix construction for logistic-regression weight learning
"""
import math
from collections import Counter, defaultdict

ENGAGEMENT_EVENTS = frozenset({"play_start", "watch_progress", "complete", "watchlist_add"})

# Mirrors interaction_weight() in the trainer — kept here to avoid circular imports
_EVENT_BASE_WEIGHT: dict[str, float] = {
    "impression": 0.2,
    "click": 0.5,
    "play_start": 1.0,
    "watch_progress": 1.5,
    "complete": 2.2,
    "watchlist_add": 0.8,
}

DEFAULT_WEIGHTS = [0.35, 0.25, 0.20, 0.10, 0.05, 0.05]
WEIGHT_KEYS = ["collaborative", "content", "session", "trending", "freshness", "genre_bonus"]

_MATURE_ORDER = {"G": 0, "PG": 1, "PG-13": 2, "R": 3}


# ---------------------------------------------------------------------------
# Ranking metrics
# ---------------------------------------------------------------------------

def dcg_at_k(relevant_set: set, ranked_items: list, k: int) -> float:
    return sum(
        1.0 / math.log2(i + 2)
        for i, item in enumerate(ranked_items[:k])
        if item in relevant_set
    )


def ndcg_at_k(relevant_set: set, ranked_items: list, k: int) -> float:
    if not relevant_set:
        return 0.0
    ideal = sum(1.0 / math.log2(i + 2) for i in range(min(len(relevant_set), k)))
    return dcg_at_k(relevant_set, ranked_items, k) / ideal if ideal else 0.0


def hit_rate_at_k(relevant_set: set, ranked_items: list, k: int) -> float:
    return float(bool(relevant_set & set(ranked_items[:k])))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize(d: dict) -> dict:
    if not d:
        return {}
    max_v = max(d.values()) or 1.0
    return {k: v / max_v for k, v in d.items()}


def _user_signal_scores(
    played_items: list,
    region: str,
    collab_map: dict,
    content_map: dict,
    trending_map: dict,
) -> tuple[dict, dict, dict, dict]:
    """Compute the four candidate-level signal scores for a single user."""
    collab: Counter = Counter()
    content: Counter = Counter()
    for src in played_items[-10:]:
        for n, s in collab_map.get(src, {}).items():
            collab[n] += s
        for n, s in content_map.get(src, {}).items():
            content[n] += s

    norm_collab = _normalize(dict(collab))
    norm_content = _normalize(dict(content))

    sess: dict = {}
    w = 1.0
    for item_id in reversed(played_items[-5:]):
        sess[item_id] = sess.get(item_id, 0.0) + w
        w *= 0.7
    norm_sess = _normalize(sess)

    trending = trending_map.get(region, {})
    return norm_collab, norm_content, norm_sess, trending


# ---------------------------------------------------------------------------
# Score-lookup construction
# ---------------------------------------------------------------------------

def build_score_lookups(
    collab_rows: list,
    content_rows: list,
    trending_rows: list,
) -> tuple[dict, dict, dict]:
    """
    Convert ORM ItemNeighbor / TrendingItem rows into fast in-memory dicts.

    Returns:
      collab_map:   source_item_id  -> {neighbor_item_id: score}
      content_map:  source_item_id  -> {neighbor_item_id: score}
      trending_map: region          -> {item_id: score}
    """
    collab_map: dict[str, dict[str, float]] = defaultdict(dict)
    for r in collab_rows:
        collab_map[r.source_item_id][r.neighbor_item_id] = r.score

    content_map: dict[str, dict[str, float]] = defaultdict(dict)
    for r in content_rows:
        content_map[r.source_item_id][r.neighbor_item_id] = r.score

    trending_map: dict[str, dict[str, float]] = defaultdict(dict)
    for r in trending_rows:
        trending_map[r.region][r.item_id] = r.score

    return dict(collab_map), dict(content_map), dict(trending_map)


# ---------------------------------------------------------------------------
# Offline candidate generation (no Redis, no live DB session)
# ---------------------------------------------------------------------------

def rank_candidates_offline(
    played_items: list,
    region: str,
    collab_map: dict,
    content_map: dict,
    trending_map: dict,
    item_map: dict,
    weights: list,
    k: int = 10,
) -> list:
    """
    Reproduce the API's ranking logic without Redis or a live session.

    weights: [collab_w, content_w, session_w, trending_w, freshness_w, genre_bonus_w]
    genre_bonus is omitted offline because we have no per-user Redis affinity state.
    """
    collab, content, sess, trending = _user_signal_scores(
        played_items, region, collab_map, content_map, trending_map
    )
    candidates = set(collab) | set(content) | set(sess) | set(trending)

    scored = []
    for item_id in candidates:
        item = item_map.get(item_id)
        if item is None or not item.is_active:
            continue
        if region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue

        freshness = min(max((item.release_year - 2020) / 6.0, 0.0), 1.0)
        score = (
            weights[0] * collab.get(item_id, 0.0)
            + weights[1] * content.get(item_id, 0.0)
            + weights[2] * sess.get(item_id, 0.0)
            + weights[3] * trending.get(item_id, 0.0)
            + weights[4] * freshness
            # weights[5] (genre_bonus) intentionally omitted
        )
        scored.append((item_id, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [item_id for item_id, _ in scored[:k]]


# ---------------------------------------------------------------------------
# Model evaluation
# ---------------------------------------------------------------------------

def evaluate_model(
    test_interactions: list,
    train_interactions: list,
    collab_map: dict,
    content_map: dict,
    trending_map: dict,
    item_map: dict,
    weights: list,
    k: int = 10,
) -> dict:
    """
    Evaluate model quality on held-out test interactions.

    The train set is used only to seed candidate generation (user history).
    The test set provides the ground-truth positive labels.

    Returns a dict: {ndcg_at_k, hit_rate_at_k, coverage, test_user_count, k}
    """
    train_played: dict[str, list] = defaultdict(list)
    for ix in train_interactions:
        if ix.event_type in ENGAGEMENT_EVENTS:
            train_played[ix.user_id].append(ix.item_id)

    user_region: dict[str, str] = {}
    for ix in (*train_interactions, *test_interactions):
        user_region[ix.user_id] = ix.region

    test_positives: dict[str, set] = defaultdict(set)
    for ix in test_interactions:
        if ix.event_type in ENGAGEMENT_EVENTS:
            test_positives[ix.user_id].add(ix.item_id)

    ndcg_scores: list[float] = []
    hit_scores: list[float] = []
    all_recommended: set[str] = set()

    for user_id, positive_set in test_positives.items():
        played = train_played.get(user_id, [])
        region = user_region.get(user_id, "GLOBAL")

        top_k = rank_candidates_offline(
            played, region, collab_map, content_map, trending_map, item_map, weights, k
        )
        ndcg_scores.append(ndcg_at_k(positive_set, top_k, k))
        hit_scores.append(hit_rate_at_k(positive_set, top_k, k))
        all_recommended.update(top_k)

    n = len(ndcg_scores)
    catalog_size = len(item_map)
    return {
        "ndcg_at_k": sum(ndcg_scores) / n if n else 0.0,
        "hit_rate_at_k": sum(hit_scores) / n if n else 0.0,
        "coverage": len(all_recommended) / catalog_size if catalog_size else 0.0,
        "test_user_count": len(test_positives),
        "k": k,
    }


# ---------------------------------------------------------------------------
# Feature matrix for learning ranking weights
# ---------------------------------------------------------------------------

def build_feature_matrix(
    train_interactions: list,
    items: list,
    collab_map: dict,
    content_map: dict,
    trending_map: dict,
) -> tuple[list, list]:
    """
    Build (X, y) training pairs for logistic-regression weight learning.

    Each row in X corresponds to a (user, candidate_item) pair observed in
    training, with features:
        [collab, content, session, trending, freshness, genre_bonus]

    y[i] = 1 if the user engaged (played/completed/watchlisted) the item,
           0 otherwise (impression-only).

    Users with fewer than 2 played items are skipped — insufficient history
    to compute meaningful signal scores.
    """
    item_map = {item.item_id: item for item in items}

    user_ixs: dict[str, list] = defaultdict(list)
    for ix in train_interactions:
        user_ixs[ix.user_id].append(ix)

    X: list[list[float]] = []
    y: list[int] = []

    for user_id, ixs in user_ixs.items():
        region = ixs[0].region
        played = [ix.item_id for ix in ixs if ix.event_type in ENGAGEMENT_EVENTS]

        if len(played) < 2:
            continue

        collab, content, sess, trending = _user_signal_scores(
            played, region, collab_map, content_map, trending_map
        )

        # Genre affinity weighted by event strength (mirrors stream processor logic)
        user_genre_w: Counter = Counter()
        for ix in ixs:
            it = item_map.get(ix.item_id)
            if it:
                weight = _EVENT_BASE_WEIGHT.get(ix.event_type, 0.1) + (ix.completion_pct / 100.0)
                for g in it.genres.split(","):
                    user_genre_w[g.strip()] += weight

        candidates = set(collab) | set(content) | set(sess) | set(trending)
        positive_set = {ix.item_id for ix in ixs if ix.event_type in ENGAGEMENT_EVENTS}

        for item_id in candidates:
            item = item_map.get(item_id)
            if item is None:
                continue

            freshness = min(max((item.release_year - 2020) / 6.0, 0.0), 1.0)
            genre_bonus = min(
                sum(user_genre_w.get(g.strip(), 0.0) for g in item.genres.split(",")) / 10.0,
                1.0,
            )

            X.append([
                collab.get(item_id, 0.0),
                content.get(item_id, 0.0),
                sess.get(item_id, 0.0),
                trending.get(item_id, 0.0),
                freshness,
                genre_bonus,
            ])
            y.append(int(item_id in positive_set))

    return X, y
