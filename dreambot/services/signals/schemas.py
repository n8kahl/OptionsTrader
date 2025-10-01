"""Signal data contracts."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Dict, List


@dataclass(slots=True)
class EntryTrigger:
    type: str
    band: str
    confirmations: List[str]

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class SignalIntent:
    ts: int
    underlying: str
    side: str
    playbook: str
    entry_trigger: EntryTrigger
    target_underlying_move: float
    stop_underlying_move: float
    time_stop_secs: int
    option_filters: Dict[str, object] = field(default_factory=dict)
    size_multiplier: float = 1.0

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["entry_trigger"] = self.entry_trigger.to_dict()
        return payload
