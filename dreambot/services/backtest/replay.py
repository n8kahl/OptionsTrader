"""Backtest replay harness."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

import numpy as np

from ..features.main import FeatureEngine
from ..features.schemas import FeaturePacket
from ..ingest.schemas import Agg1s
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
        self.rng = np.random.default_rng(seed)

    def replay(self, symbol: str, bars: Sequence[Agg1s]) -> BacktestResult:
        features: List[FeaturePacket] = []
        trades: List[Trade] = []
        for bar in bars:
            feature = self.feature_engine.compute_features(symbol, bar)
            features.append(feature)
            try:
                gate_adjustments = {"risk_multiplier": 1.0}
                signal = self.signal_engine.evaluate(bar.ts, symbol, feature, feature.atr_1m, gate_adjustments)
            except RuntimeError:
                continue
            fill_inputs = FillInputs(mid=bar.c, spread=0.02, spread_state=feature.micro["spread_state"], event_rate=10)
            fill = self.fill_model.execute(signal["side"], fill_inputs)
            pnl = float(self.rng.normal(0.05, 0.1))
            trades.append(Trade(pnl=pnl - fill.slippage))
        report = summarize(trades)
        return BacktestResult(features=features, trades=trades, report=report)
