"""Portfolio/PnL aggregation service."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Mapping

from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import EXECUTION_STREAM, PORTFOLIO_STREAM, QUOTE_STREAM
from ..ingest.schemas import Quote
from ..oms.schemas import OrderStatus
from ..execution.schemas import ExecutionReport  # type: ignore


@dataclass
class Position:
    qty: int = 0
    avg_price: float = 0.0
    last_mid: float = 0.0

    def update_fill(self, side: str, price: float, qty: int) -> float:
        sign = 1 if side.upper() == "BUY" else -1
        realized = 0.0
        incoming = sign * qty
        if self.qty == 0 or (self.qty > 0 and incoming > 0) or (self.qty < 0 and incoming < 0):
            # increasing exposure in same direction → update average
            total_cost = self.avg_price * abs(self.qty) + price * qty
            self.qty += incoming
            if self.qty != 0:
                self.avg_price = total_cost / abs(self.qty)
            else:
                self.avg_price = 0.0
        else:
            # reducing or flipping exposure → realize PnL up to flatten
            closing = min(abs(self.qty), qty)
            realized += (price - self.avg_price) * (closing if self.qty > 0 else -closing)
            new_qty = self.qty + incoming
            if self.qty + incoming == 0:
                self.qty = 0
                self.avg_price = 0.0
            elif (self.qty > 0 and new_qty < 0) or (self.qty < 0 and new_qty > 0):
                # flipped: remaining open at fill price
                self.qty = new_qty
                self.avg_price = price
            else:
                # reduced but same direction remains
                self.qty = new_qty
        return realized

    def unrealized(self) -> float:
        if self.qty == 0:
            return 0.0
        return (self.last_mid - self.avg_price) * self.qty


@dataclass
class PortfolioState:
    positions: Dict[str, Position] = field(default_factory=dict)
    realized_pnl: float = 0.0

    def mark_quote(self, quote: Quote) -> None:
        pos = self.positions.get(quote.symbol)
        if pos:
            pos.last_mid = quote.mid

    def apply_fill(self, symbol: str, side: str, price: float, qty: int) -> None:
        pos = self.positions.setdefault(symbol, Position())
        self.realized_pnl += pos.update_fill(side, price, qty)

    def snapshot(self) -> Dict[str, object]:
        unrealized = sum(p.unrealized() for p in self.positions.values())
        open_positions = [
            {
                "symbol": sym,
                "qty": pos.qty,
                "avg_price": round(pos.avg_price, 6),
                "mid": round(pos.last_mid, 6),
                "unrealized": round(pos.unrealized(), 6),
            }
            for sym, pos in self.positions.items()
            if pos.qty != 0
        ]
        return {
            "ts": None,
            "realized_pnl": round(self.realized_pnl, 6),
            "unrealized_pnl": round(unrealized, 6),
            "total_pnl": round(self.realized_pnl + unrealized, 6),
            "positions": open_positions,
        }


async def run_portfolio(redis: Redis, *, stop_event: asyncio.Event | None = None) -> None:
    state = PortfolioState()

    def should_stop() -> bool:
        return stop_event.is_set() if stop_event else False

    async def handle_quote_async(payload: Mapping[str, object]) -> None:
        state.mark_quote(Quote.from_dict(dict(payload)))
        await publish_json(redis, PORTFOLIO_STREAM, state.snapshot())

    async def handle_exec(payload: Mapping[str, object]) -> None:
        # Accept both ExecutionReport and OrderStatus with fills
        symbol = payload.get("option_symbol") or payload.get("symbol")
        side = payload.get("side") or payload.get("request", {}).get("side")
        fill_price = payload.get("fill_price")
        fill_qty = payload.get("fill_qty")
        if not (symbol and side and fill_price is not None and fill_qty is not None):
            # Try OrderStatus.fill shape
            fills = payload.get("fills", [])
            if fills:
                f0 = fills[0]
                fill_price = f0.get("price")
                fill_qty = f0.get("qty")
                side = payload.get("request", {}).get("side", side)
        try:
            qty = int(float(fill_qty))
            price = float(fill_price)
        except Exception:
            return
        state.apply_fill(str(symbol), str(side), price, qty)
        await publish_json(redis, PORTFOLIO_STREAM, state.snapshot())

    tasks = [
        asyncio.create_task(consume_stream(redis, QUOTE_STREAM, handle_quote_async, stop=should_stop)),
        asyncio.create_task(consume_stream(redis, EXECUTION_STREAM, handle_exec, stop=should_stop)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise


async def main_async() -> None:
    redis = await create_redis()
    try:
        await run_portfolio(redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
