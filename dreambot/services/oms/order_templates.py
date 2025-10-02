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

    def to_tradier_payload(
        self,
        *,
        option_symbol: str,
        entry_side: str,
        closing_side: str,
        quantity: int,
        duration: str,
    ) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "class": "option",
            "symbol": option_symbol,
            "option_symbol": option_symbol,
            "side": entry_side,
            "type": self.entry.order_type,
            "duration": duration,
            "quantity": quantity,
            "advanced": "otoco",
        }
        if self.entry.limit_price is not None:
            payload["price"] = f"{self.entry.limit_price:.2f}"
        if self.entry.stop_price is not None:
            payload["stop"] = f"{self.entry.stop_price:.2f}"

        payload.update(
            {
                "orders[0][type]": self.take_profit.order_type,
                "orders[0][side]": closing_side,
                "orders[0][duration]": duration,
                "orders[0][quantity]": quantity,
                "orders[1][type]": self.stop.order_type,
                "orders[1][side]": closing_side,
                "orders[1][duration]": duration,
                "orders[1][quantity]": quantity,
            }
        )
        if self.take_profit.limit_price is not None:
            payload["orders[0][price]"] = f"{self.take_profit.limit_price:.2f}"
        if self.stop.stop_price is not None:
            payload["orders[1][stop]"] = f"{self.stop.stop_price:.2f}"
        if self.stop.limit_price is not None:
            payload["orders[1][price]"] = f"{self.stop.limit_price:.2f}"
        return payload


def build_otoco(symbol: str, quantity: int, side: str, entry_price: float,
                target_price: float, stop_price: float, offset_ticks: float) -> OTOCOOrder:
    sign = 1 if side.upper() == "BUY" else -1
    entry_limit = entry_price + sign * offset_ticks
    tp_leg = OrderLeg(side="SELL" if side.upper() == "BUY" else "BUY", quantity=quantity, order_type="limit", limit_price=target_price)
    stop_leg = OrderLeg(side=tp_leg.side, quantity=quantity, order_type="stop", stop_price=stop_price)
    entry_leg = OrderLeg(side=side.upper(), quantity=quantity, order_type="limit", limit_price=entry_limit)
    return OTOCOOrder(symbol=symbol, entry=entry_leg, take_profit=tp_leg, stop=stop_leg)
