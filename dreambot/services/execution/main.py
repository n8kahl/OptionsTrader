"""Execution analytics service."""
from __future__ import annotations

import asyncio
from typing import Dict, Mapping, Optional

from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import EXECUTION_STREAM, OMS_ORDER_STREAM, QUOTE_STREAM
from ..ingest.schemas import Quote, is_option
from ..oms.schemas import OrderStatus
from .schemas import ExecutionReport


class ExecutionAnalyticsService:
    def __init__(self) -> None:
        self._option_quotes: Dict[str, Quote] = {}
        self._underlying_quotes: Dict[str, Quote] = {}

    def update_quote(self, quote: Quote) -> None:
        if is_option(quote.symbol):
            self._option_quotes[quote.symbol] = quote
        else:
            self._underlying_quotes[quote.symbol] = quote

    def build_report(self, status: OrderStatus) -> Optional[ExecutionReport]:
        if status.state.lower() != "filled":
            return None
        request = status.request
        option_symbol = str(request.get("option_symbol", ""))
        underlying = str(request.get("underlying", ""))
        fills = list(status.fills)
        fill = fills[0] if fills else {}
        fill_price = float(fill.get("price", request.get("entry_price", 0.0)))
        fill_qty = int(fill.get("qty", request.get("quantity", 0)))
        fill_ts = int(fill.get("ts", status.ts))
        option_quote = self._option_quotes.get(option_symbol)
        underlying_quote = self._underlying_quotes.get(underlying)
        option_mid = option_quote.mid if option_quote else None
        underlying_mid = underlying_quote.mid if underlying_quote else None
        slippage_bps: Optional[float] = None
        if option_mid and option_mid > 0:
            slippage_bps = (fill_price - option_mid) / option_mid * 10_000
        request_ts = int(request.get("ts", status.ts))
        latency_ms = max((fill_ts - request_ts) / 1000.0, 0.0)
        side = str(request.get("side", "BUY")).upper()
        target_price = float(request.get("target_price", fill_price))
        stop_price = float(request.get("stop_price", fill_price))
        reward = None
        risk = None
        if side == "BUY":
            reward = target_price - fill_price
            risk = fill_price - stop_price
        else:
            reward = fill_price - target_price
            risk = stop_price - fill_price
        risk_reward: Optional[float] = None
        if risk is not None and abs(risk) > 1e-9:
            risk_reward = reward / abs(risk) if reward is not None else None
        metadata = dict(request.get("metadata", {}))
        return ExecutionReport(
            ts=status.ts,
            order_id=status.order_id,
            underlying=underlying,
            option_symbol=option_symbol,
            side=side,
            fill_price=fill_price,
            fill_qty=fill_qty,
            fill_ts=fill_ts,
            option_mid=option_mid,
            underlying_mid=underlying_mid,
            slippage_bps=slippage_bps,
            latency_ms=latency_ms,
            risk_reward=risk_reward,
            metadata=metadata,
        )


async def run_execution_stream(
    service: ExecutionAnalyticsService,
    redis: Redis,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    def should_stop() -> bool:
        return stop_event.is_set() if stop_event else False

    def handle_quote(payload: Mapping[str, object]) -> None:
        service.update_quote(Quote.from_dict(payload))

    async def handle_status(payload: Mapping[str, object]) -> None:
        status = OrderStatus.from_dict(payload)
        report = service.build_report(status)
        if report is None:
            return
        await publish_json(redis, EXECUTION_STREAM, report.to_dict())

    tasks = [
        asyncio.create_task(consume_stream(redis, QUOTE_STREAM, handle_quote, stop=should_stop)),
        asyncio.create_task(consume_stream(redis, OMS_ORDER_STREAM, handle_status, stop=should_stop)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        raise


async def main_async() -> None:
    service = ExecutionAnalyticsService()
    redis = await create_redis()
    try:
        await run_execution_stream(service, redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
