from datetime import datetime, timezone
import hashlib
import random
from typing import Iterable


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def deterministic_score(*parts: Iterable[str]) -> float:
    key = "|".join(map(str, parts))
    digest = hashlib.md5(key.encode()).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def weighted_choice(items: list[tuple[str, float]]) -> str:
    choices, weights = zip(*items)
    return random.choices(choices, weights=weights, k=1)[0]
