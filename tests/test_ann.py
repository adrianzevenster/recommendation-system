"""Unit tests for the FAISS-backed ANN index utilities in common/ann.py."""
import numpy as np
import pytest

from common.ann import (
    build_ann_index,
    deserialize_index,
    query_index,
    serialize_index,
)


def _norm(vec) -> np.ndarray:
    vec = np.asarray(vec, dtype=np.float32)
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def _embeddings(n: int, dim: int = 8, seed: int = 0) -> dict[str, np.ndarray]:
    """n L2-normalised random embeddings keyed m000, m001, ... (insertion-ordered)."""
    rng = np.random.default_rng(seed)
    return {f"m{i:03d}": _norm(rng.standard_normal(dim)) for i in range(n)}


class TestBuildAnnIndex:
    def test_too_few_items_returns_none(self):
        index, id_list = build_ann_index(_embeddings(4))
        assert index is None
        assert id_list == []

    def test_builds_index_with_all_items(self):
        emb = _embeddings(10)
        index, id_list = build_ann_index(emb)
        assert index is not None
        assert index.ntotal == 10
        assert set(id_list) == set(emb)

    def test_id_list_preserves_insertion_order(self):
        emb = _embeddings(6)
        _, id_list = build_ann_index(emb)
        assert id_list == list(emb.keys())


class TestSerializeRoundTrip:
    def test_roundtrip_is_bytes_and_preserves_search(self):
        emb = _embeddings(12)
        index, id_list = build_ann_index(emb)

        data = serialize_index(index)
        assert isinstance(data, bytes)

        restored = deserialize_index(data)
        assert restored.ntotal == index.ntotal
        # A query against the restored index must match the original exactly.
        seed = [id_list[0]]
        assert query_index(index, id_list, seed, k=5) == query_index(restored, id_list, seed, k=5)


class TestQueryIndex:
    def test_none_index_returns_empty(self):
        assert query_index(None, [], ["m000"], k=5) == {}

    def test_empty_query_returns_empty(self):
        index, id_list = build_ann_index(_embeddings(8))
        assert query_index(index, id_list, [], k=5) == {}

    def test_unknown_query_ids_returns_empty(self):
        index, id_list = build_ann_index(_embeddings(8))
        assert query_index(index, id_list, ["does-not-exist"], k=5) == {}

    def test_excludes_seed_items(self):
        index, id_list = build_ann_index(_embeddings(10))
        seed = id_list[0]
        result = query_index(index, id_list, [seed], k=10)
        assert seed not in result

    def test_respects_k_limit(self):
        index, id_list = build_ann_index(_embeddings(30))
        result = query_index(index, id_list, [id_list[0]], k=5)
        assert len(result) <= 5

    def test_finds_nearest_neighbour(self):
        # Add a near-twin of the first item; querying the first should rank it top.
        emb = _embeddings(8, seed=1)
        first_id = next(iter(emb))
        emb["m999"] = _norm(emb[first_id] + 0.01)

        index, id_list = build_ann_index(emb)
        result = query_index(index, id_list, [first_id], k=3)

        assert result
        top_id = max(result, key=result.get)
        assert top_id == "m999"
        # Cosine of near-identical unit vectors is close to 1.0.
        assert result["m999"] > 0.9
