"""Ingest service entrypoint."""
from __future__ import annotations

import asyncio
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

import yaml
from redis.asyncio import Redis

from ..common.redis import close_redis, create_redis, publish_json
from ..common.streams import AGG_STREAM, OPTION_META_STREAM, QUOTE_STREAM
from .chain_snapshot import ChainSnapshotClient
from .polygon_ws import OptionUniverseManager, PolygonWebSocketClient, WebSocketConfig, rotate_universe
from .schemas import Agg1s, OptionMeta, Quote


@dataclass
class IngestConfig:
    api_key: str
    symbols: Mapping[str, List[str]]
    option_rotate_secs: int
    max_contracts: int
    strikes_around_atm: int


class IngestService:
    def __init__(self, config: IngestConfig):
        self.config = config
        self.universe_manager = OptionUniverseManager(
            config.max_contracts, config.strikes_around_atm, config.option_rotate_secs
        )
        self.snapshot_client = ChainSnapshotClient(config.api_key) if config.api_key else None
        self.ws_config = WebSocketConfig(
            api_key=config.api_key,
            symbols=config.symbols,
            max_contracts_per_conn=config.max_contracts,
        )
        all_symbols = set()
        for bucket in config.symbols.values():
            all_symbols.update(bucket)
        self._synthetic_prices: Dict[str, float] = {symbol: 400.0 for symbol in all_symbols}

    async def rotate(self, underlying: str, ts: int) -> List[str]:
        if not self.snapshot_client:
            return []
        chain = await self.snapshot_client.fetch_chain(underlying)
        rotation = await rotate_universe(self.universe_manager, underlying, chain, ts)
        return rotation.contracts

    async def ensure_capacity(self, contracts: Iterable[str]) -> None:
        contract_list = list(contracts)
        if len(contract_list) > self.config.max_contracts:
            raise RuntimeError("Option universe exceeds websocket capacity constraint")

    async def close(self) -> None:
        if self.snapshot_client:
            await self.snapshot_client.close()

    async def publish_synthetic_batch(self, redis: Redis, ts: int) -> None:
        """Generate deterministic synthetic data for development & tests."""
        for symbol in self.config.symbols.get("stocks", []) + self.config.symbols.get("indices", []):
            price = self._step_price(symbol)
            bid = round(price - 0.05, 2)
            ask = round(price + 0.05, 2)
            quote = Quote(
                ts=ts,
                symbol=symbol,
                bid=bid,
                ask=ask,
                mid=round((bid + ask) / 2, 2),
                bid_size=100,
                ask_size=100,
                nbbo_age_ms=10,
            )
            await publish_json(redis, QUOTE_STREAM, quote.to_dict())

            agg = Agg1s(
                ts=ts,
                symbol=symbol,
                o=price,
                h=price + 0.2,
                l=price - 0.2,
                c=price,
                v=150_000,
            )
            await publish_json(redis, AGG_STREAM, agg.to_dict())

            option_meta = OptionMeta(
                ts=ts,
                underlying=symbol,
                symbol=f"{symbol}0000C00",
                strike=round(price, 2),
                type="C",
                exp=datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%d"),
                iv=0.22,
                delta=0.5,
                gamma=0.1,
                vega=0.05,
                theta=-0.12,
                oi=25_000,
                prev_oi=24_500,
            )
            await publish_json(redis, OPTION_META_STREAM, option_meta.to_dict())

    def _step_price(self, symbol: str) -> float:
        base = self._synthetic_prices.setdefault(symbol, 400.0)
        jitter = math.sin(time.time()) * 0.2 + random.uniform(-0.1, 0.1)
        base = max(base + jitter, 1.0)
        self._synthetic_prices[symbol] = base
        return round(base, 2)


async def run_ingest(service: IngestService, redis: Redis) -> None:
    """Main ingest loop. Real Polygon hooks will replace synthetic generator."""
    if not service.config.api_key:
        while True:
            ts = int(time.time() * 1_000_000)
            await service.publish_synthetic_batch(redis, ts)
            await asyncio.sleep(1)

    ws_client = PolygonWebSocketClient(service.ws_config)
    try:
        async with ws_client:
            while True:
                now = datetime.now(tz=timezone.utc)
                ts = int(now.timestamp() * 1_000_000)
                await service.publish_synthetic_batch(redis, ts)
                for symbol in service.config.symbols.get("indices", []):
                    try:
                        contracts = await service.rotate(symbol.replace("I:", ""), ts)
                        await service.ensure_capacity(contracts)
                    except Exception:
                        await asyncio.sleep(1)
                await asyncio.sleep(service.config.option_rotate_secs)
    finally:
        await service.close()


def load_ingest_config() -> IngestConfig:
    data_cfg = yaml.safe_load(Path("config/data.yaml").read_text(encoding="utf-8"))
    symbols_cfg = yaml.safe_load(Path("config/symbols.yaml").read_text(encoding="utf-8"))
    return IngestConfig(
        api_key=os.environ.get("POLYGON_API_KEY", ""),
        symbols=data_cfg["polygon"]["symbols"],
        option_rotate_secs=data_cfg["polygon"]["option_chain_snapshot_secs"],
        max_contracts=data_cfg["polygon"]["max_contracts_per_conn"],
        strikes_around_atm=symbols_cfg["strikes_around_atm"],
    )


async def main_async() -> None:
    config = load_ingest_config()
    service = IngestService(config)
    redis = await create_redis()
    try:
        await run_ingest(service, redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
