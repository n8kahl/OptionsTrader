"""Learner service entrypoint."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping

import numpy as np

from .bandit import ContextualBandit
from .calibration_io import apply_calibration, load_calibration, save_calibration
from .changepoint import BayesianChangePoint
from .metalabel import MetaLabeler
from .triple_barrier import triple_barrier_label


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


async def main_async() -> None:
    service = LearnerService(Path("backtests/calibration.json"))
    while True:
        await asyncio.sleep(5)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
