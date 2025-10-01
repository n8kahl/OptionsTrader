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

    @classmethod
    def from_dict(cls, payload: Dict[str, object]) -> "EntryTrigger":
        return cls(
            type=str(payload["type"]),
            band=str(payload.get("band", "")),
            confirmations=list(payload.get("confirmations", [])),
        )


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

    @classmethod
    def from_dict(cls, payload: Dict[str, object]) -> "SignalIntent":
        trigger = EntryTrigger.from_dict(payload["entry_trigger"])
        return cls(
            ts=int(payload["ts"]),
            underlying=str(payload["underlying"]),
            side=str(payload["side"]),
            playbook=str(payload["playbook"]),
            entry_trigger=trigger,
            target_underlying_move=float(payload["target_underlying_move"]),
            stop_underlying_move=float(payload["stop_underlying_move"]),
            time_stop_secs=int(payload["time_stop_secs"]),
            option_filters=dict(payload.get("option_filters", {})),
            size_multiplier=float(payload.get("size_multiplier", 1.0)),
        )
