"""Feature message schema."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any, Mapping


@dataclass(slots=True)
class FeaturePacket:
    ts: int
    symbol: str
    tf: str
    vwap: float
    vwap_bands: Mapping[str, tuple[float, float]]
    atr_1m: float
    atr_1s: float
    adx_3m: float
    vwap_slope: float
    rv_5m: float
    rv_15m: float
    iv_9d: float
    iv_30d: float
    iv_60d: float
    skew_25d: float
    vol_of_vol: float
    micro: Mapping[str, Any]
    prob: Mapping[str, float]

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["vwap_bands"] = {k: list(v) for k, v in payload["vwap_bands"].items()}
        return payload
