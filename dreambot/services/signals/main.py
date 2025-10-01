"""Signals service entrypoint."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from ..features.schemas import FeaturePacket
from .gating import evaluate_gates
from .policy import build_signal


def load_gate_config() -> Mapping[str, float]:
    path = Path("config/features.yaml")
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        "nbbo_age_ms_max": config["microstructure"]["nbbo_stale_ms"],
        "spread_pct_max": config["microstructure"]["nbbo_max_spread_pct"]/100,
        "trend_threshold": -0.2,
        "adx_threshold": 20,
        "pot_threshold": 0.55,
    }


@dataclass
class SignalEngine:
    gate_config: Mapping[str, float]

    def evaluate(self, ts: int, underlying: str, features: FeaturePacket, atr: float,
                 learner_adjustments: Mapping[str, float]) -> Mapping[str, Any]:
        gate = evaluate_gates(features, self.gate_config)
        if not gate.allowed:
            raise RuntimeError("Gating prevented entry")
        return build_signal(ts, underlying, features, gate, learner_adjustments, atr)


async def main_async() -> None:
    engine = SignalEngine(load_gate_config())
    while True:
        await asyncio.sleep(5)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
