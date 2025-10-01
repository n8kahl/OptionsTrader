import asyncio

import pytest
from fakeredis import aioredis as fakeredis

from services.common.redis import publish_json
from services.common.streams import EXECUTION_STREAM, PORTFOLIO_STREAM, QUOTE_STREAM
from services.portfolio.main import run_portfolio


@pytest.mark.asyncio
async def test_portfolio_updates_with_fills_and_quotes():
    redis = fakeredis.FakeRedis(decode_responses=True)
    stop = asyncio.Event()
    task = asyncio.create_task(run_portfolio(redis, stop_event=stop))

    try:
        # Simulate a buy fill and a quote
        await publish_json(
            redis,
            EXECUTION_STREAM,
            {
                "option_symbol": "SPY_OPT",
                "side": "BUY",
                "fill_price": 1.0,
                "fill_qty": 1,
            },
        )
        await publish_json(
            redis,
            QUOTE_STREAM,
            {
                "ts": 1,
                "symbol": "SPY_OPT",
                "bid": 1.05,
                "ask": 1.15,
                "mid": 1.10,
                "bid_size": 1,
                "ask_size": 1,
                "nbbo_age_ms": 10,
            },
        )
        await asyncio.sleep(0.1)
        # Publish another quote to ensure mark-to-market runs after fills
        await publish_json(
            redis,
            QUOTE_STREAM,
            {
                "ts": 2,
                "symbol": "SPY_OPT",
                "bid": 1.05,
                "ask": 1.15,
                "mid": 1.10,
                "bid_size": 1,
                "ask_size": 1,
                "nbbo_age_ms": 10,
            },
        )
        await asyncio.sleep(0.1)
        entries = await redis.xread({PORTFOLIO_STREAM: "0-0"}, count=10, block=100)
        assert entries, "no portfolio snapshot published"
        payload = entries[0][1][-1][1]["data"]
        import json

        snap = json.loads(payload)
        assert snap["unrealized_pnl"] > 0
        assert snap["realized_pnl"] == 0
    finally:
        stop.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
