"""Signal gating rules."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..features.schemas import FeaturePacket


def liquidity_gate(features: FeaturePacket, max_age_ms: float, max_spread_pct: float) -> bool:
    micro = features.micro
    if micro["nbbo_age_ms"] > max_age_ms:
        return False
    if micro["spread_pct"] > max_spread_pct:
        return False
    if micro["spread_state"] == "stressed":
        return False
    return True


def context_gate(features: FeaturePacket, trend_threshold: float, adx_threshold: float) -> tuple[bool, float]:
    trend_score = max(min(features.vwap_slope * 1_000, 1.0), -1.0)
    adx_ok = features.adx_3m >= adx_threshold
    regime_score = 0.5 * (trend_score + (1 if adx_ok else -1))
    return regime_score > trend_threshold, regime_score


def probability_gate(features: FeaturePacket, pot_threshold: float) -> bool:
    return features.prob["pot_est"] >= pot_threshold


@dataclass
class GateResult:
    allowed: bool
    regime_score: float
    liquidity_score: float


def evaluate_gates(features: FeaturePacket, config: Mapping[str, float]) -> GateResult:
    liquidity_ok = liquidity_gate(features, config["nbbo_age_ms_max"], config["spread_pct_max"])
    liquidity_score = 1.0 if liquidity_ok else 0.0
    context_ok, regime_score = context_gate(features, config["trend_threshold"], config["adx_threshold"])
    probability_ok = probability_gate(features, config["pot_threshold"])
    allowed = liquidity_ok and context_ok and probability_ok
    return GateResult(allowed=allowed, regime_score=regime_score, liquidity_score=liquidity_score)
