"""Order construction utilities."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List


@dataclass
class OrderLeg:
    side: str
    quantity: int
    order_type: str
    limit_price: float | None = None
    stop_price: float | None = None

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class OTOCOOrder:
    symbol: str
    entry: OrderLeg
    take_profit: OrderLeg
    stop: OrderLeg

    def to_payload(self) -> Dict[str, object]:
        return {
            "symbol": self.symbol,
            "type": "OTOCO",
            "legs": [self.entry.to_dict(), self.take_profit.to_dict(), self.stop.to_dict()],
        }


def build_otoco(symbol: str, quantity: int, side: str, entry_price: float,
                target_price: float, stop_price: float, offset_ticks: float) -> OTOCOOrder:
    sign = 1 if side.upper() == "BUY" else -1
    entry_limit = entry_price + sign * offset_ticks
    tp_leg = OrderLeg(side="SELL" if side.upper() == "BUY" else "BUY", quantity=quantity, order_type="limit", limit_price=target_price)
    stop_leg = OrderLeg(side=tp_leg.side, quantity=quantity, order_type="stop", stop_price=stop_price)
    entry_leg = OrderLeg(side=side.upper(), quantity=quantity, order_type="limit", limit_price=entry_limit)
    return OTOCOOrder(symbol=symbol, entry=entry_leg, take_profit=tp_leg, stop=stop_leg)
