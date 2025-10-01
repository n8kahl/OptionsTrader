"""Fill model used in backtests."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FillInputs:
    mid: float
    spread: float
    spread_state: str
    event_rate: float


@dataclass
class FillResult:
    price: float
    slippage: float


class FillModel:
    def __init__(self, base_slippage: float = 0.01):
        self.base_slippage = base_slippage

    def execute(self, side: str, inputs: FillInputs) -> FillResult:
        direction = 1 if side.upper() == "BUY" else -1
        stress_penalty = 0.0
        if inputs.spread_state == "stressed":
            stress_penalty += 2 * self.base_slippage
        elif inputs.spread_state == "tight":
            stress_penalty -= 0.5 * self.base_slippage
        slippage = self.base_slippage + stress_penalty + 0.001 * inputs.event_rate
        fill_price = inputs.mid + direction * (inputs.spread / 2 + slippage)
        return FillResult(price=fill_price, slippage=slippage)
