"""Microstructure and liquidity analytics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Deque, Iterable
from collections import deque
import math


@dataclass
class SpreadHistory:
    window: int = 300
    values: Deque[float] = field(default_factory=lambda: deque(maxlen=300))

    def add(self, value: float) -> None:
        self.values.append(value)

    def median(self) -> float:
        if not self.values:
            return 0.0
        ordered = sorted(self.values)
        mid = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[mid]
        return 0.5 * (ordered[mid - 1] + ordered[mid])

    def stdev(self) -> float:
        if len(self.values) < 2:
            return 0.0
        mean = sum(self.values) / len(self.values)
        var = sum((x - mean) ** 2 for x in self.values) / (len(self.values) - 1)
        return math.sqrt(var)


def compute_spread_pct(bid: float, ask: float, mid: float) -> float:
    if mid <= 0:
        return 0.0
    return (ask - bid) / mid


def classify_spread(spread_history: SpreadHistory, spread_pct: float, stress_z: float) -> str:
    spread_history.add(spread_pct)
    median = spread_history.median()
    stdev = spread_history.stdev()
    if stdev <= 0:
        return "normal"
    z = (spread_pct - median) / stdev
    if z <= -1:
        return "tight"
    if z >= stress_z:
        return "stressed"
    return "normal"


def nbbo_age(now_ts: int, last_nbbo_ts: int) -> int:
    return max(0, now_ts - last_nbbo_ts)


def nbbo_event_rate(events: Iterable[int], window_secs: int) -> float:
    timestamps = list(events)
    if len(timestamps) < 2:
        return 0.0
    span = timestamps[-1] - timestamps[0]
    if span <= 0:
        return 0.0
    return len(timestamps) / span * window_secs


def cumulative_volume_delta(trades: Iterable[tuple[str, float]]) -> float:
    """Aggregate signed volume. trade tuple: (side, size)."""
    delta = 0.0
    for side, size in trades:
        if side == "buy":
            delta += size
        elif side == "sell":
            delta -= size
    return delta
