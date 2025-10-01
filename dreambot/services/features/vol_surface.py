"""Vol surface analytics."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable
import math


@dataclass(frozen=True)
class TermStructure:
    iv_9d: float
    iv_30d: float
    iv_60d: float
    slope_9_30: float
    slope_30_60: float


def compute_term_structure(vols: Dict[int, float]) -> TermStructure:
    iv_9 = float(vols.get(9, 0.0))
    iv_30 = float(vols.get(30, 0.0))
    iv_60 = float(vols.get(60, 0.0))
    return TermStructure(
        iv_9d=iv_9,
        iv_30d=iv_30,
        iv_60d=iv_60,
        slope_9_30=iv_30 - iv_9,
        slope_30_60=iv_60 - iv_30,
    )


def compute_smile_skew(puts: Dict[float, float], calls: Dict[float, float], target_delta: float) -> float:
    put_iv = _nearest_delta_iv(puts, -target_delta)
    call_iv = _nearest_delta_iv(calls, target_delta)
    return put_iv - call_iv


def _nearest_delta_iv(vol_map: Dict[float, float], target_delta: float) -> float:
    if not vol_map:
        return 0.0
    best_delta = min(vol_map.keys(), key=lambda d: abs(d - target_delta))
    return float(vol_map[best_delta])


def realized_vol_gap(iv_front: float, realized_vol: float, mean: float, stdev: float) -> float:
    if stdev <= 0:
        return 0.0
    return (iv_front - realized_vol - mean) / stdev


def vol_of_vol(series: Iterable[float]) -> float:
    samples = list(series)
    if len(samples) < 2:
        return 0.0
    mean = sum(samples) / len(samples)
    variance = sum((x - mean) ** 2 for x in samples) / (len(samples) - 1)
    return math.sqrt(max(variance, 0.0))
