"""Probability calculations for option analytics."""
from __future__ import annotations

import math
from typing import Literal

from scipy.stats import norm


def black_scholes_d1_d2(spot: float, strike: float, rate: float, iv: float, t: float) -> tuple[float, float]:
    if spot <= 0 or strike <= 0 or iv <= 0 or t <= 0:
        return 0.0, 0.0
    numerator = math.log(spot / strike) + (rate + 0.5 * iv ** 2) * t
    denominator = iv * math.sqrt(t)
    d1 = numerator / denominator
    d2 = d1 - iv * math.sqrt(t)
    return d1, d2


def probability_itm(option_type: Literal["C", "P"], spot: float, strike: float, rate: float,
                     iv: float, t: float) -> float:
    d1, d2 = black_scholes_d1_d2(spot, strike, rate, iv, t)
    if option_type == "C":
        return float(norm.cdf(d2))
    return float(norm.cdf(-d2))


def probability_of_touch(prob_itm_value: float) -> float:
    pot = 2 * prob_itm_value
    return float(max(0.0, min(1.0, pot)))
