"""Learner service entrypoint."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping

import numpy as np
from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import (
    FEATURE_STREAM,
    LEARNER_ADJUSTMENT_STREAM,
    OMS_ORDER_STREAM,
    SIGNAL_STREAM,
)
from ..features.schemas import FeaturePacket
from ..oms.schemas import OrderStatus
from ..signals.schemas import SignalIntent
from .bandit import ContextualBandit
from .calibration_io import apply_calibration, load_calibration, save_calibration
from .changepoint import BayesianChangePoint
from .metalabel import MetaLabeler
from .triple_barrier import triple_barrier_label

_DEFAULT_ADX_THRESHOLD = 20.0


@dataclass
class LearnerState:
    bandit: ContextualBandit
    metalabeler: MetaLabeler
    changepoint: BayesianChangePoint
    calibration: Dict[str, float] = field(default_factory=dict)


class LearnerService:
    def __init__(self, calibration_path: Path):
        self.calibration_path = calibration_path
        self.state = LearnerState(
            bandit=ContextualBandit(["TREND_PULLBACK", "BALANCE_FADE", "ORB", "LATE_PUSH"]),
            metalabeler=MetaLabeler(),
            changepoint=BayesianChangePoint(),
        )
        self.state.calibration.update(load_calibration(calibration_path))

    def calibration_params(self, symbol: str) -> Dict[str, float]:
        calibration = self.state.calibration or {}
        params: Dict[str, float] = {}
        symbols_cfg = calibration.get("symbols", {}) if isinstance(calibration, Mapping) else {}
        symbol_key = symbol.upper()
        if symbol_key in symbols_cfg:
            symbol_params = symbols_cfg[symbol_key].get("params", {})
            if isinstance(symbol_params, Mapping):
                params.update({k: float(v) for k, v in symbol_params.items() if isinstance(v, (int, float))})
        global_params = {}
        if isinstance(calibration.get("global_params"), Mapping):
            global_params = calibration["global_params"]
        default_risk = calibration.get("risk_multiplier") or global_params.get("risk_multiplier", 1.0)
        default_pot = calibration.get("pot_threshold") or global_params.get("pot_threshold", 0.55)
        default_adx = calibration.get("adx_threshold") or global_params.get("adx_threshold", _DEFAULT_ADX_THRESHOLD)
        params.setdefault("risk_multiplier", float(default_risk))
        params.setdefault("pot_threshold", float(default_pot))
        params.setdefault("adx_threshold", float(default_adx))
        return params

    def select_playbook(self, context: Mapping[str, float]) -> str:
        return self.state.bandit.select(context)

    def update_reward(self, playbook: str, reward: float) -> None:
        self.state.bandit.update(playbook, reward)

    def fit_metalabel(self, features: np.ndarray, labels: np.ndarray) -> None:
        self.state.metalabeler.fit(features, labels)

    def score_metalabel(self, features: np.ndarray) -> np.ndarray:
        return self.state.metalabeler.predict_proba(features)

    def detect_change(self, value: float) -> bool:
        return self.state.changepoint.update(value)

    def save_calibration(self) -> None:
        save_calibration(self.calibration_path, self.state.calibration)

    def apply_calibration(self, target: Dict[str, float]) -> None:
        apply_calibration(target, self.state.calibration)

    @staticmethod
    def label_trade(path: np.ndarray, entry: float, up: float, down: float, steps: int) -> int:
        return triple_barrier_label(path.tolist(), entry, up, down, steps)


def _feature_from_payload(payload: Mapping[str, object]) -> FeaturePacket:
    bands = {key: tuple(value) for key, value in payload["vwap_bands"].items()}
    return FeaturePacket(
        ts=int(payload["ts"]),
        symbol=str(payload["symbol"]),
        tf=str(payload["tf"]),
        vwap=float(payload["vwap"]),
        vwap_bands=bands,
        atr_1m=float(payload["atr_1m"]),
        atr_1s=float(payload["atr_1s"]),
        adx_3m=float(payload["adx_3m"]),
        vwap_slope=float(payload["vwap_slope"]),
        rv_5m=float(payload["rv_5m"]),
        rv_15m=float(payload["rv_15m"]),
        iv_9d=float(payload["iv_9d"]),
        iv_30d=float(payload["iv_30d"]),
        iv_60d=float(payload["iv_60d"]),
        skew_25d=float(payload["skew_25d"]),
        vol_of_vol=float(payload["vol_of_vol"]),
        micro=dict(payload["micro"]),
        prob=dict(payload["prob"]),
    )


async def run_learner_stream(service: LearnerService, redis: Redis, stop_event: asyncio.Event | None = None) -> None:
    def should_stop() -> bool:
        return stop_event.is_set() if stop_event else False

    async def handle_feature(payload: Mapping[str, object]) -> None:
        feature = _feature_from_payload(payload)
        spread_pct = float(feature.micro.get("spread_pct", 0.0))
        regime_value = abs(feature.vwap_slope) * 10 + feature.adx_3m / 50
        change = service.detect_change(spread_pct)
        weights = service.state.bandit.weights()
        params = service.calibration_params(feature.symbol)
        base_risk = params.get("risk_multiplier", 1.0)
        risk_multiplier = 0.8 if change else max(0.5, min(1.5, 1.0 / (1.0 + feature.vol_of_vol * 5)))
        risk_multiplier *= base_risk
        base_pot = params.get("pot_threshold", 0.55)
        adjustments = {
            "ts": feature.ts,
            "symbol": feature.symbol,
            "risk_multiplier": risk_multiplier,
            "playbook_weights": weights,
            "pot_threshold": max(0.4, min(0.7, base_pot + min(0.2, regime_value * 0.1))),
            "adx_threshold": params.get("adx_threshold", _DEFAULT_ADX_THRESHOLD),
        }
        await publish_json(redis, LEARNER_ADJUSTMENT_STREAM, adjustments)

    async def handle_order(payload: Mapping[str, object]) -> None:
        status = OrderStatus.from_dict(payload)
        metadata = dict(status.request.get("metadata", {}))
        playbook = str(metadata.get("playbook", "TREND_PULLBACK"))
        state = status.state.lower()
        reward = 0.0
        if state == "filled":
            reward = 0.1
        elif state == "cancelled":
            reward = -0.05
        service.update_reward(playbook, reward)

    async def handle_signal(payload: Mapping[str, object]) -> None:
        signal = SignalIntent.from_dict(dict(payload))
        context = {
            "trend_score": signal.size_multiplier,
            "time_stop": signal.time_stop_secs / 300,
        }
        service.select_playbook(context)

    tasks = [
        asyncio.create_task(consume_stream(redis, FEATURE_STREAM, handle_feature, stop=should_stop)),
        asyncio.create_task(consume_stream(redis, OMS_ORDER_STREAM, handle_order, stop=should_stop)),
        asyncio.create_task(consume_stream(redis, SIGNAL_STREAM, handle_signal, stop=should_stop)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        raise


async def main_async() -> None:
    service = LearnerService(Path("backtests/calibration.json"))
    redis = await create_redis()
    try:
        await run_learner_stream(service, redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
