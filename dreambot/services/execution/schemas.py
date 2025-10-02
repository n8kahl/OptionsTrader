"""Execution analytics data contracts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional


@dataclass(slots=True)
class ExecutionReport:
    ts: int
    order_id: str
    underlying: str
    option_symbol: str
    side: str
    fill_price: float
    fill_qty: int
    fill_ts: int
    option_mid: Optional[float]
    underlying_mid: Optional[float]
    slippage_bps: Optional[float]
    latency_ms: float
    risk_reward: Optional[float]
    metadata: Mapping[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "order_id": self.order_id,
            "underlying": self.underlying,
            "option_symbol": self.option_symbol,
            "side": self.side,
            "fill_price": self.fill_price,
            "fill_qty": self.fill_qty,
            "fill_ts": self.fill_ts,
            "option_mid": self.option_mid,
            "underlying_mid": self.underlying_mid,
            "slippage_bps": self.slippage_bps,
            "latency_ms": self.latency_ms,
            "risk_reward": self.risk_reward,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ExecutionReport":
        return cls(
            ts=int(payload["ts"]),
            order_id=str(payload["order_id"]),
            underlying=str(payload["underlying"]),
            option_symbol=str(payload["option_symbol"]),
            side=str(payload["side"]),
            fill_price=float(payload["fill_price"]),
            fill_qty=int(payload["fill_qty"]),
            fill_ts=int(payload["fill_ts"]),
            option_mid=payload.get("option_mid"),
            underlying_mid=payload.get("underlying_mid"),
            slippage_bps=payload.get("slippage_bps"),
            latency_ms=float(payload["latency_ms"]),
            risk_reward=payload.get("risk_reward"),
            metadata=dict(payload.get("metadata", {})),
        )
