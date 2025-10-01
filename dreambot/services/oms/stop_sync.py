"""Stop synchronization with underlying price."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StopSyncConfig:
    modify_on_tick: bool = True
    trail_ratio: float = 0.6


def compute_stop_from_underlying(entry_underlying: float, move: float, direction: str) -> float:
    sign = 1 if direction.upper() == "BUY" else -1
    return entry_underlying + sign * move


def adjust_stop(existing_stop: float, current_underlying: float, direction: str, config: StopSyncConfig) -> float:
    if not config.modify_on_tick:
        return existing_stop
    desired = compute_stop_from_underlying(current_underlying, -config.trail_ratio * abs(current_underlying - existing_stop), direction)
    if direction.upper() == "BUY":
        return max(existing_stop, desired)
    return min(existing_stop, desired)
