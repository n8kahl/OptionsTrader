"""Backtest replay harness."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

from ..features.main import FeatureEngine
from ..features.schemas import FeaturePacket
from ..ingest.schemas import Agg1s, Quote
from ..signals.main import SignalEngine
from ..learner.main import LearnerService
from ..risk.main import build_risk_manager
from ..oms.main import OMSConfig, OMSService
from .fill_model import FillInputs, FillModel
from .metrics import BacktestReport, Trade, summarize


@dataclass
class BacktestConfig:
    risk: dict
    gate: dict
    oms: dict


@dataclass
class BacktestResult:
    features: List[FeaturePacket]
    trades: List[Trade]
    report: BacktestReport


class BacktestRunner:
    def __init__(self, feature_engine: FeatureEngine, signal_engine: SignalEngine,
                 learner: LearnerService, fill_model: FillModel, config: BacktestConfig, seed: int = 0):
        self.feature_engine = feature_engine
        self.signal_engine = signal_engine
        self.learner = learner
        self.fill_model = fill_model
        self.risk_manager = build_risk_manager(config.risk)
        self.oms = OMSService(OMSConfig(**config.oms))

    def replay(
        self,
        symbol: str,
        bars: Sequence[Agg1s],
        *,
        decision_symbol: str | None = None,
        decision_bars: Sequence[Agg1s] | None = None,
    ) -> BacktestResult:
        if not bars:
            return BacktestResult(features=[], trades=[], report=summarize([]))

        decision_symbol = decision_symbol or symbol
        src_bars = decision_bars or bars
        length = min(len(bars), len(src_bars))
        if length == 0:
            return BacktestResult(features=[], trades=[], report=summarize([]))

        self.risk_manager.set_session_start(bars[0].ts)
        features: List[FeaturePacket] = []
        trades: List[Trade] = []
        for idx in range(length):
            feature_bar = src_bars[idx]
            actual_bar = bars[idx]
            quote = Quote(
                ts=feature_bar.ts,
                symbol=decision_symbol,
                bid=feature_bar.c - 0.05,
                ask=feature_bar.c + 0.05,
                mid=feature_bar.c,
                bid_size=max(feature_bar.v / 10, 1.0),
                ask_size=max(feature_bar.v / 10, 1.0),
                nbbo_age_ms=10,
            )
            self.feature_engine.update_quote(quote)
            feature = self.feature_engine.compute_features(decision_symbol, feature_bar)
            features.append(feature)
            if idx == length - 1:
                continue
            if not self.risk_manager.entry_allowed(feature_bar.ts, minutes_to_open=60, minutes_to_close=240):
                continue
            try:
                gate_adjustments = {"risk_multiplier": 1.0}
                signal = self.signal_engine.evaluate(
                    feature_bar.ts,
                    decision_symbol,
                    feature,
                    feature.atr_1m,
                    gate_adjustments,
                )
            except RuntimeError:
                continue
            spread = max(actual_bar.h - actual_bar.l, 0.02)
            fill_inputs = FillInputs(
                mid=actual_bar.c,
                spread=spread,
                spread_state=feature.micro["spread_state"],
                event_rate=10,
            )
            fill = self.fill_model.execute(signal["side"], fill_inputs)
            exit_bar = bars[idx + 1]
            direction = 1 if signal["side"].upper() == "BUY" else -1
            raw_move = exit_bar.c - fill.price
            pnl = direction * raw_move
            size_multiplier = float(signal.get("size_multiplier", 1.0) or 1.0)
            pnl *= size_multiplier
            trades.append(
                Trade(
                    entry_ts=actual_bar.ts,
                    exit_ts=exit_bar.ts,
                    symbol=symbol,
                    side=signal["side"],
                    playbook=str(signal.get("playbook", "UNKNOWN")),
                    entry_price=fill.price,
                    exit_price=exit_bar.c,
                    pnl=pnl,
                    size=size_multiplier,
                )
            )
            self.risk_manager.register_position(+1)
            self.risk_manager.register_position(-1)
            self.risk_manager.register_fill(pnl, exit_bar.ts)

        report = summarize(trades)
        return BacktestResult(features=features, trades=trades, report=report)
