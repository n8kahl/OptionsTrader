"""OMS message schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Sequence


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
        return {
            "ts": self.ts,
            "underlying": self.underlying,
            "option_symbol": self.option_symbol,
            "side": self.side,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "target_price": self.target_price,
            "stop_price": self.stop_price,
            "time_stop_secs": self.time_stop_secs,
            "metadata": dict(self.metadata),
        }

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
    ts: int
    order_id: str
    state: str
    request: Mapping[str, Any]
    broker_payload: Mapping[str, Any]
    fills: Sequence[Mapping[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "ts": self.ts,
            "order_id": self.order_id,
            "state": self.state,
            "request": dict(self.request),
            "broker_payload": dict(self.broker_payload),
            "fills": [dict(fill) for fill in self.fills],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OrderStatus":
        fills = payload.get("fills", [])
        if isinstance(fills, Mapping):  # single fill dict
            fills = [dict(fills)]
        elif isinstance(fills, Sequence) and not isinstance(fills, (str, bytes)):
            fills = [dict(item) for item in fills]
        else:
            fills = []
        return cls(
            ts=int(payload.get("ts", 0)),
            order_id=str(payload["order_id"]),
            state=str(payload.get("state", "unknown")),
            request=dict(payload.get("request", {})),
            broker_payload=dict(payload.get("broker_payload", {})),
            fills=fills,
        )


@dataclass(slots=True)
class OrderCommand:
    action: str
    client_order_id: str | None = None
    order_id: str | None = None
    stop_price: float | None = None
    target_price: float | None = None

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {"action": self.action}
        if self.client_order_id:
            payload["client_order_id"] = self.client_order_id
        if self.order_id:
            payload["order_id"] = self.order_id
        if self.stop_price is not None:
            payload["stop_price"] = self.stop_price
        if self.target_price is not None:
            payload["target_price"] = self.target_price
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OrderCommand":
        return cls(
            action=str(payload["action"]),
            client_order_id=payload.get("client_order_id"),
            order_id=payload.get("order_id"),
            stop_price=float(payload["stop_price"]) if "stop_price" in payload else None,
            target_price=float(payload["target_price"]) if "target_price" in payload else None,
        )
