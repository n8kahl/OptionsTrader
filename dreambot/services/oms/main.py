"""OMS service entrypoint."""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import yaml

from .order_templates import build_otoco
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

    async def route_signal(self, signal: Mapping[str, Any], option_symbol: str,
                           option_price: float, target_price: float, stop_price: float, quantity: int) -> Mapping[str, Any]:
        if not self.config.use_otoco:
            raise NotImplementedError("DreamBot only routes OTOCO orders per spec")
        order = build_otoco(
            symbol=option_symbol,
            quantity=quantity,
            side=signal["side"],
            entry_price=option_price,
            target_price=target_price,
            stop_price=stop_price,
            offset_ticks=self.config.default_limit_offset_ticks,
        )
        response = await self.broker.place_order(order.to_payload())
        return response

    def sync_stop(self, existing_stop: float, underlying_price: float, direction: str) -> float:
        return adjust_stop(existing_stop, underlying_price, direction, self.stop_config)


def build_tradier_client(env: Mapping[str, str]) -> TradierClient:
    cfg = TradierConfig(token=env["TRADIER_ACCESS_TOKEN"], account_id=env["TRADIER_ACCOUNT_ID"])
    return TradierClient(cfg)


async def main_async() -> None:
    config_map = load_broker_config()
    config = OMSConfig(**config_map)
    service = OMSService(config)
    while True:
        await asyncio.sleep(5)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
