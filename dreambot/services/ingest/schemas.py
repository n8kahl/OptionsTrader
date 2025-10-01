"""Data contracts for ingest service."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List


@dataclass(slots=True)
class Quote:
    ts: int
    symbol: str
    bid: float
    ask: float
    mid: float
    bid_size: float
    ask_size: float
    nbbo_age_ms: int

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Quote":
        return cls(
            ts=int(payload["ts"]),
            symbol=str(payload["symbol"]),
            bid=float(payload["bid"]),
            ask=float(payload["ask"]),
            mid=float(payload.get("mid", (payload["bid"] + payload["ask"]) / 2)),
            bid_size=float(payload.get("bid_size", 0)),
            ask_size=float(payload.get("ask_size", 0)),
            nbbo_age_ms=int(payload.get("nbbo_age_ms", 0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Agg1s:
    ts: int
    symbol: str
    o: float
    h: float
    l: float
    c: float
    v: float

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Agg1s":
        return cls(
            ts=int(payload["ts"]),
            symbol=str(payload["symbol"]),
            o=float(payload["o"]),
            h=float(payload["h"]),
            l=float(payload["l"]),
            c=float(payload["c"]),
            v=float(payload.get("v", 0.0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OptionMeta:
    ts: int
    underlying: str
    symbol: str
    strike: float
    type: str
    exp: str
    iv: float
    delta: float
    gamma: float
    vega: float
    theta: float
    oi: int
    prev_oi: int

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "OptionMeta":
        return cls(
            ts=int(payload["ts"]),
            underlying=str(payload["underlying"]),
            symbol=str(payload["symbol"]),
            strike=float(payload["strike"]),
            type=str(payload["type"]),
            exp=str(payload["exp"]),
            iv=float(payload.get("iv", 0.0)),
            delta=float(payload.get("delta", 0.0)),
            gamma=float(payload.get("gamma", 0.0)),
            vega=float(payload.get("vega", 0.0)),
            theta=float(payload.get("theta", 0.0)),
            oi=int(payload.get("oi", 0)),
            prev_oi=int(payload.get("prev_oi", 0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class UniverseRotation:
    """Represents a scheduled update to the option universe."""

    ts: int
    underlying: str
    contracts: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"ts": self.ts, "underlying": self.underlying, "contracts": list(self.contracts)}


def is_option(symbol: str) -> bool:
    return len(symbol) > 8 and symbol[-1] in {"C", "P"}
