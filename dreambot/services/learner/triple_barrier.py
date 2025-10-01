"""Triple barrier labeling."""
from __future__ import annotations

from typing import Iterable


def triple_barrier_label(path: Iterable[float], entry_price: float, up_move: float,
                         down_move: float, max_steps: int) -> int:
    prices = list(path)
    upper = entry_price + up_move
    lower = entry_price + down_move
    for idx, price in enumerate(prices[:max_steps]):
        if price >= upper:
            return 1
        if price <= lower:
            return -1
    return 0
