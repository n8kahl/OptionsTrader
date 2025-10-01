"""Async Redis utilities for stream publishing/consumption."""
from __future__ import annotations

import json
import os
from typing import Any, Awaitable, Callable, Dict, Mapping

from redis.asyncio import Redis

DEFAULT_REDIS_URL = "redis://localhost:6379/0"


async def create_redis(url: str | None = None) -> Redis:
    """Create a Redis client with decoded responses."""
    redis_url = url or os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)
    client = Redis.from_url(redis_url, decode_responses=True)
    await client.ping()
    return client


async def close_redis(client: Redis) -> None:
    """Close Redis connection gracefully."""
    await client.close()
    await client.wait_closed()


async def publish_json(client: Redis, stream: str, payload: Mapping[str, Any], *, maxlen: int = 1_000) -> str:
    """Publish a JSON payload to a Redis Stream."""
    data = json.dumps(payload, separators=(",", ":"))
    return await client.xadd(stream, {"data": data}, maxlen=maxlen, approximate=True)


async def consume_stream(
    client: Redis,
    stream: str,
    handler: Callable[[Dict[str, Any]], Any] | Callable[[Dict[str, Any]], Awaitable[Any]],
    *,
    start: str = "0-0",
    block_ms: int = 1_000,
    batch_size: int = 100,
    stop: Callable[[], bool] | None = None,
) -> None:
    """Consume a Redis Stream and dispatch parsed JSON to handler."""
    from asyncio import iscoroutinefunction

    last_id = start
    async_mode = iscoroutinefunction(handler)
    while stop is None or not stop():
        response = await client.xread({stream: last_id}, count=batch_size, block=block_ms)
        if not response:
            continue
        for _, entries in response:
            for entry_id, fields in entries:
                last_id = entry_id
                raw = fields.get("data", "{}")
                payload = json.loads(raw)
                if async_mode:
                    await handler(payload)
                else:
                    handler(payload)
