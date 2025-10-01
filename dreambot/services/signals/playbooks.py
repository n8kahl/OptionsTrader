"""Playbook logic for DreamBot."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from .schemas import EntryTrigger, SignalIntent


@dataclass
class RegimeContext:
    trend_score: float
    vol_regime: str
    risk_multiplier: float


@dataclass
class LiquidityContext:
    nbbo_age_ms: float
    spread_pct: float
    spread_state: str

    def score(self) -> float:
        score = 1.0
        if self.nbbo_age_ms > 500:
            score *= 0.5
        if self.spread_pct > 0.005:
            score *= 0.7
        if self.spread_state == "stressed":
            score *= 0.0
        return max(score, 0.0)


def trend_pullback(ts: int, underlying: str, context: RegimeContext, liquidity: LiquidityContext,
                   atr: float) -> SignalIntent:
    trigger = EntryTrigger(
        type="VWAP_BAND_TOUCH",
        band="-1σ" if context.trend_score >= 0 else "+1σ",
        confirmations=["CVD_UP", "ES_AGREE"],
    )
    size_multiplier = context.trend_score * liquidity.score() * context.risk_multiplier
    return SignalIntent(
        ts=ts,
        underlying=underlying,
        side="BUY" if context.trend_score >= 0 else "SELL",
        playbook="TREND_PULLBACK",
        entry_trigger=trigger,
        target_underlying_move=0.7 * atr,
        stop_underlying_move=-0.45 * atr,
        time_stop_secs=240,
        option_filters={"delta": [0.4, 0.55], "dte": [0, 1], "spread_pct_max": 0.01, "quote_age_ms_max": 800},
        size_multiplier=size_multiplier,
    )


def balance_fade(ts: int, underlying: str, context: RegimeContext, liquidity: LiquidityContext,
                 atr: float) -> SignalIntent:
    trigger = EntryTrigger(
        type="VWAP_REVERT",
        band="±2σ",
        confirmations=["BAND_STABLE"],
    )
    size_multiplier = 0.6 * liquidity.score() * context.risk_multiplier
    return SignalIntent(
        ts=ts,
        underlying=underlying,
        side="SELL" if context.trend_score > 0 else "BUY",
        playbook="BALANCE_FADE",
        entry_trigger=trigger,
        target_underlying_move=0.5 * atr,
        stop_underlying_move=-0.35 * atr,
        time_stop_secs=180,
        option_filters={"delta": [0.3, 0.4], "dte": [1, 3], "spread_pct_max": 0.01},
        size_multiplier=size_multiplier,
    )


def opening_range_break(ts: int, underlying: str, context: RegimeContext, liquidity: LiquidityContext,
                         atr: float) -> SignalIntent:
    trigger = EntryTrigger(
        type="OPENING_BREAK",
        band="ORB",
        confirmations=["OPEN_IMBALANCE", "ES_AGREE"],
    )
    size_multiplier = 0.5 * context.risk_multiplier * liquidity.score()
    return SignalIntent(
        ts=ts,
        underlying=underlying,
        side="BUY",
        playbook="ORB",
        entry_trigger=trigger,
        target_underlying_move=0.8 * atr,
        stop_underlying_move=-0.5 * atr,
        time_stop_secs=300,
        option_filters={"delta": [0.45, 0.55], "dte": [0, 1]},
        size_multiplier=size_multiplier,
    )


def late_push(ts: int, underlying: str, context: RegimeContext, liquidity: LiquidityContext,
              atr: float) -> SignalIntent:
    trigger = EntryTrigger(
        type="LATE_PUSH",
        band="VWAP",
        confirmations=["PIN_RISK_OK"],
    )
    size_multiplier = 0.3 * context.risk_multiplier * liquidity.score()
    return SignalIntent(
        ts=ts,
        underlying=underlying,
        side="BUY",
        playbook="LATE_PUSH",
        entry_trigger=trigger,
        target_underlying_move=0.4 * atr,
        stop_underlying_move=-0.25 * atr,
        time_stop_secs=120,
        option_filters={"delta": [0.35, 0.45], "dte": [0, 1], "late_close": True},
        size_multiplier=size_multiplier,
    )


PLAYBOOK_DISPATCH = {
    "TREND_PULLBACK": trend_pullback,
    "BALANCE_FADE": balance_fade,
    "ORB": opening_range_break,
    "LATE_PUSH": late_push,
}


def build_intent(playbook: str, ts: int, underlying: str, context: RegimeContext,
                 liquidity: LiquidityContext, atr: float) -> SignalIntent:
    factory = PLAYBOOK_DISPATCH[playbook]
    return factory(ts, underlying, context, liquidity, atr)
