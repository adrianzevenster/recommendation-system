"""Statistical helpers for A/B experiment analysis."""
import math


def _z_test_two_proportions(
    n_control: int, k_control: int, n_variant: int, k_variant: int
) -> tuple[float, float]:
    """Two-proportion z-test. Returns (lift_pct, z_score). |z| >= 1.96 → p < 0.05."""
    if n_control == 0 or n_variant == 0:
        return 0.0, 0.0
    p1 = k_control / n_control
    p2 = k_variant / n_variant
    p_pool = (k_control + k_variant) / (n_control + n_variant)
    if p_pool in (0.0, 1.0):
        return 0.0, 0.0
    se = math.sqrt(p_pool * (1.0 - p_pool) * (1.0 / n_control + 1.0 / n_variant))
    if se == 0.0:
        return 0.0, 0.0
    z = (p2 - p1) / se
    lift = (p2 - p1) / p1 * 100.0 if p1 > 0.0 else 0.0
    return round(lift, 2), round(z, 4)
