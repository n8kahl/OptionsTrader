"""Economic calendar utilities."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List


@dataclass
class EconEvent:
    name: str
    release_time: datetime


class EconCalendar:
    def __init__(self, events: Iterable[EconEvent]):
        self.events: List[EconEvent] = list(events)

    def between(self, start: datetime, end: datetime) -> List[EconEvent]:
        return [event for event in self.events if start <= event.release_time <= end]

    @classmethod
    def from_dicts(cls, entries: Iterable[dict[str, str]]) -> "EconCalendar":
        events = [EconEvent(name=entry["name"], release_time=datetime.fromisoformat(entry["time"])) for entry in entries]
        return cls(events)
