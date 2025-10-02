"""Policy orchestration for signal generation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..features.schemas import FeaturePacket
from .gating import GateResult
from .playbooks import LiquidityContext, RegimeContext, build_intent


@dataclass
class PolicyConfig:
    trend_adx_threshold: float
    balance_adx_threshold: float
    pot_threshold: float


PLAYBOOKS = ["TREND_PULLBACK", "BALANCE_FADE", "ORB", "LATE_PUSH"]


def choose_playbook(features: FeaturePacket, gate: GateResult) -> str:
    if not gate.allowed:
        raise ValueError("gating failed")
    if gate.regime_score > 0.2:
        return "TREND_PULLBACK"
    if gate.regime_score < -0.2:
        return "BALANCE_FADE"
    if features.ts % (60 * 1_000_000) < 5 * 60 * 1_000_000:
        return "ORB"
    return "LATE_PUSH"


def build_signal(ts: int, underlying: str, features: FeaturePacket, gate: GateResult,
                 learner_adjustments: Mapping[str, float], atr: float) -> Mapping[str, object]:
    # Regime-based pick, then bias by bandit weights within plausible set
    playbook = choose_playbook(features, gate)
    weights = learner_adjustments.get("playbook_weights", {}) if isinstance(learner_adjustments, dict) else {}
    if isinstance(weights, dict) and weights:
        # Constrain to regime-consistent candidates
        if gate.regime_score > 0.2:
            candidates = ["TREND_PULLBACK", "LATE_PUSH"]
        elif gate.regime_score < -0.2:
            candidates = ["BALANCE_FADE", "ORB"]
        else:
            candidates = ["ORB", "LATE_PUSH"]
        # Pick the highest-weight candidate
        weighted_choice = max(candidates, key=lambda p: float(weights.get(p, 0.0)))
        if weighted_choice != playbook:
            playbook = weighted_choice
    context = RegimeContext(
        trend_score=gate.regime_score,
        vol_regime="stressed" if features.vol_of_vol > 0.1 else "moderate",
        risk_multiplier=learner_adjustments.get("risk_multiplier", 1.0),
    )
    liquidity = LiquidityContext(
        nbbo_age_ms=features.micro["nbbo_age_ms"],
        spread_pct=features.micro["spread_pct"],
        spread_state=features.micro["spread_state"],
    )
    intent = build_intent(
        playbook,
        ts,
        underlying,
        context,
        liquidity,
        atr,
    )
    # Use learner-provided playbook weights for sizing if available
    weights = learner_adjustments.get("playbook_weights", {})
    try:
        size_bump = float(weights.get(playbook, 1.0)) if isinstance(weights, dict) else 1.0
    except Exception:
        size_bump = 1.0
    intent.size_multiplier *= size_bump
    return intent.to_dict()
