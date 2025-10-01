"""OMS message schemas."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Mapping


@dataclass(slots=True)
class OrderRequest:
    ts: int
    underlying: str
    option_symbol: str
    side: str
    quantity: int
    entry_price: float
    target_price: float
    stop_price: float
    time_stop_secs: int
    metadata: Mapping[str, object]

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["metadata"] = dict(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OrderRequest":
        return cls(
            ts=int(payload["ts"]),
            underlying=str(payload["underlying"]),
            option_symbol=str(payload["option_symbol"]),
            side=str(payload["side"]),
            quantity=int(payload["quantity"]),
            entry_price=float(payload["entry_price"]),
            target_price=float(payload["target_price"]),
            stop_price=float(payload["stop_price"]),
            time_stop_secs=int(payload["time_stop_secs"]),
            metadata=dict(payload.get("metadata", {})),
        )


@dataclass(slots=True)
class OrderStatus:
    order_id: str
    state: str
    payload: Mapping[str, object]

    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data["payload"] = dict(self.payload)
        return data
