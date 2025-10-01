"""Risk scheduler for econ halts and timers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Iterable, List, Tuple


@dataclass
class HaltWindow:
    label: str
    start: datetime
    end: datetime


class EconScheduler:
    def __init__(self, windows: Iterable[Tuple[datetime, datetime, str]]):
        self.windows: List[HaltWindow] = [HaltWindow(label=lab, start=start, end=end) for start, end, lab in windows]

    def is_halted(self, now: datetime) -> bool:
        return any(window.start <= now <= window.end for window in self.windows)

    def minutes_to_next(self, now: datetime) -> int:
        future = [window for window in self.windows if window.start > now]
        if not future:
            return 10_000
        future.sort(key=lambda w: w.start)
        delta = future[0].start - now
        return max(int(delta.total_seconds() // 60), 0)

    @staticmethod
    def build_window(release_time: datetime, padding_minutes: int) -> Tuple[datetime, datetime]:
        start = release_time - timedelta(minutes=padding_minutes)
        end = release_time + timedelta(minutes=padding_minutes)
        return start, end
