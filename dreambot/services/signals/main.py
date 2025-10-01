"""Signals service entrypoint."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping

import yaml
from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import FEATURE_STREAM, SIGNAL_STREAM, LEARNER_ADJUSTMENT_STREAM
from ..features.schemas import FeaturePacket
from .gating import evaluate_gates
from .policy import build_signal


def load_gate_config() -> Mapping[str, float]:
    path = Path("config/features.yaml")
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        "nbbo_age_ms_max": config["microstructure"]["nbbo_stale_ms"],
        "spread_pct_max": config["microstructure"]["nbbo_max_spread_pct"] / 100,
        "trend_threshold": -0.2,
        "adx_threshold": 20,
        "pot_threshold": 0.55,
    }


@dataclass
class SignalEngine:
    gate_config: Mapping[str, float]

    def evaluate(
        self,
        ts: int,
        underlying: str,
        features: FeaturePacket,
        atr: float,
        learner_adjustments: Mapping[str, float],
    ) -> Mapping[str, Any]:
        config = dict(self.gate_config)
        pot_override = learner_adjustments.get("pot_threshold")
        if pot_override is not None:
            config["pot_threshold"] = float(pot_override)
        adx_override = learner_adjustments.get("adx_threshold")
        if adx_override is not None:
            config["adx_threshold"] = float(adx_override)
        gate = evaluate_gates(features, config)
        if not gate.allowed:
            raise RuntimeError("Gating prevented entry")
        return build_signal(ts, underlying, features, gate, learner_adjustments, atr)


def _feature_from_payload(payload: Dict[str, Any]) -> FeaturePacket:
    bands = {key: tuple(value) for key, value in payload["vwap_bands"].items()}
    return FeaturePacket(
        ts=payload["ts"],
        symbol=payload["symbol"],
        tf=payload["tf"],
        vwap=payload["vwap"],
        vwap_bands=bands,
        atr_1m=payload["atr_1m"],
        atr_1s=payload["atr_1s"],
        adx_3m=payload["adx_3m"],
        vwap_slope=payload["vwap_slope"],
        rv_5m=payload["rv_5m"],
        rv_15m=payload["rv_15m"],
        iv_9d=payload["iv_9d"],
        iv_30d=payload["iv_30d"],
        iv_60d=payload["iv_60d"],
        skew_25d=payload["skew_25d"],
        vol_of_vol=payload["vol_of_vol"],
        micro=payload["micro"],
        prob=payload["prob"],
    )


async def run_signal_stream(
    engine: SignalEngine,
    redis: Redis,
    *,
    learner_adjustments: Mapping[str, float] | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    default_adjustments = dict(learner_adjustments or {})
    symbol_adjustments: Dict[str, Dict[str, Any]] = {}

    def should_stop() -> bool:
        return stop_event.is_set() if stop_event else False

    async def handle_feature(payload: Dict[str, Any]) -> None:
        packet = _feature_from_payload(payload)
        sym_key = packet.symbol.upper()
        params = dict(default_adjustments)
        if sym_key in symbol_adjustments:
            params.update(symbol_adjustments[sym_key])
        params.pop("symbol", None)
        try:
            signal = engine.evaluate(
                packet.ts,
                packet.symbol,
                packet,
                packet.atr_1m,
                params,
            )
        except RuntimeError:
            return
        await publish_json(redis, SIGNAL_STREAM, signal)

    async def handle_adjustment(payload: Dict[str, Any]) -> None:
        symbol = str(payload.get("symbol", "")).upper()
        data = dict(payload)
        if symbol:
            symbol_adjustments[symbol] = data
        else:
            default_adjustments.update(data)

    tasks = [
        asyncio.create_task(consume_stream(redis, FEATURE_STREAM, handle_feature, stop=should_stop)),
        asyncio.create_task(consume_stream(redis, LEARNER_ADJUSTMENT_STREAM, handle_adjustment, stop=should_stop)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        raise


async def main_async() -> None:
    engine = SignalEngine(load_gate_config())
    redis = await create_redis()
    try:
        await run_signal_stream(engine, redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
