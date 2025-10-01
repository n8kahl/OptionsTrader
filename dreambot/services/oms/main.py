"""OMS service entrypoint."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Mapping, Optional

import yaml
from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import OMS_ORDER_STREAM, RISK_ORDER_STREAM
from .order_templates import build_otoco
from .schemas import OrderRequest, OrderStatus
from .stop_sync import StopSyncConfig, adjust_stop
from .tradier_api import InMemoryBroker, TradierClient, TradierConfig


def load_broker_config() -> Mapping[str, object]:
    return yaml.safe_load(open("config/broker.yaml", "r", encoding="utf-8").read())


@dataclass
class OMSConfig:
    paper: bool
    order_type: str
    use_otoco: bool
    default_limit_offset_ticks: float
    modify_stop_on_underlying: bool


class OMSService:
    def __init__(self, config: OMSConfig, broker: Optional[object] = None):
        self.config = config
        self.broker = broker or InMemoryBroker()
        self.stop_config = StopSyncConfig(modify_on_tick=config.modify_stop_on_underlying)

    async def route_order(self, request: OrderRequest) -> OrderStatus:
        if not self.config.use_otoco:
            raise NotImplementedError("DreamBot only routes OTOCO orders per spec")
        order = build_otoco(
            symbol=request.option_symbol,
            quantity=request.quantity,
            side=request.side,
            entry_price=request.entry_price,
            target_price=request.target_price,
            stop_price=request.stop_price,
            offset_ticks=self.config.default_limit_offset_ticks,
        )
        response = await self.broker.place_order(order.to_payload())
        return OrderStatus(order_id=response["id"], state=response.get("status", "unknown"), payload=response)

    def sync_stop(self, existing_stop: float, underlying_price: float, direction: str) -> float:
        return adjust_stop(existing_stop, underlying_price, direction, self.stop_config)


def build_tradier_client(env: Mapping[str, str]) -> TradierClient:
    cfg = TradierConfig(token=env["TRADIER_ACCESS_TOKEN"], account_id=env["TRADIER_ACCOUNT_ID"])
    return TradierClient(cfg)


async def run_oms_stream(service: OMSService, redis: Redis, *, stop_event: asyncio.Event | None = None) -> None:
    def should_stop() -> bool:
        return stop_event.is_set() if stop_event else False

    async def handle_order(payload: Mapping[str, object]) -> None:
        request = OrderRequest.from_dict(dict(payload))
        status = await service.route_order(request)
        await publish_json(redis, OMS_ORDER_STREAM, status.to_dict())

    task = asyncio.create_task(
        consume_stream(redis, RISK_ORDER_STREAM, handle_order, stop=should_stop)
    )
    try:
        await task
    except asyncio.CancelledError:
        task.cancel()
        raise


async def main_async() -> None:
    config_map = load_broker_config()
    config = OMSConfig(**config_map)
    service = OMSService(config)
    redis = await create_redis()
    try:
        await run_oms_stream(service, redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
