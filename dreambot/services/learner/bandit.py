"""Contextual Thompson sampling bandit."""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Dict, Iterable, Mapping


@dataclass
class ArmStats:
    count: int = 0
    sum_rewards: float = 0.0
    sum_sq: float = 0.0

    def mean(self) -> float:
        return self.sum_rewards / self.count if self.count else 0.0

    def variance(self) -> float:
        if self.count < 2:
            return 1.0
        mean = self.mean()
        return max((self.sum_sq / self.count) - mean ** 2, 1e-6)


class ContextualBandit:
    def __init__(self, arms: Iterable[str]):
        self.arms = list(arms)
        self.state: Dict[str, ArmStats] = {arm: ArmStats() for arm in self.arms}

    def select(self, context: Mapping[str, float]) -> str:
        scores = {}
        for arm in self.arms:
            stats = self.state[arm]
            mean = stats.mean()
            variance = stats.variance()
            exploration = random.gauss(mean, math.sqrt(variance / (stats.count + 1)))
            context_score = sum(context.values()) / (len(context) or 1)
            scores[arm] = exploration + 0.1 * context_score
        best = max(scores.items(), key=lambda kv: kv[1])[0]
        return best

    def update(self, arm: str, reward: float) -> None:
        stats = self.state[arm]
        stats.count += 1
        stats.sum_rewards += reward
        stats.sum_sq += reward ** 2

    def weights(self) -> Dict[str, float]:
        totals = {arm: self.state[arm].mean() + 1e-6 for arm in self.arms}
        total = sum(max(v, 0.0) for v in totals.values())
        if total == 0:
            return {arm: 1 / len(self.arms) for arm in self.arms}
        return {arm: max(value, 0.0) / total for arm, value in totals.items()}
