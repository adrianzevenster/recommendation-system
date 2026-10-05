"""Pydantic request schemas for the recommendation API."""
from pydantic import BaseModel, Field

from common.eval import WEIGHT_KEYS


class FeedbackEvent(BaseModel):
    user_id: str
    item_id: str
    event_type: str
    watch_seconds: int = 0
    completion_pct: float = 0.0
    position: int = -1


class ExperimentConclude(BaseModel):
    winning_variant: str = Field(..., pattern="^(control|variant|inconclusive)$")
    conclusion_reason: str = Field(default="manual", max_length=32)


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
