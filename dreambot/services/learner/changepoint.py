"""Change-point detection on microstructure series."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque


@dataclass
class BayesianChangePoint:
    window: int = 120
    threshold: float = 5.0
    history: Deque[float] = field(default_factory=lambda: deque(maxlen=120))

    def update(self, value: float) -> bool:
        self.history.append(value)
        if len(self.history) < self.window:
            return False
        first_half = list(self.history)[: self.window // 2]
        second_half = list(self.history)[self.window // 2 :]
        mean_diff = abs(sum(first_half) / len(first_half) - sum(second_half) / len(second_half))
        return mean_diff >= self.threshold
