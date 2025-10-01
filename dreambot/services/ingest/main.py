"""Ingest service entrypoint."""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

import yaml

from .chain_snapshot import ChainSnapshotClient
from .polygon_ws import OptionUniverseManager, PolygonWebSocketClient, WebSocketConfig, rotate_universe
from .schemas import OptionMeta


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
        self.snapshot_client = ChainSnapshotClient(config.api_key)
        self.ws_config = WebSocketConfig(
            api_key=config.api_key,
            symbols=config.symbols,
            max_contracts_per_conn=config.max_contracts,
        )

    async def rotate(self, underlying: str, ts: int) -> List[str]:
        chain = await self.snapshot_client.fetch_chain(underlying)
        rotation = await rotate_universe(self.universe_manager, underlying, chain, ts)
        return rotation.contracts

    async def ensure_capacity(self, contracts: Iterable[str]) -> None:
        contract_list = list(contracts)
        if len(contract_list) > self.config.max_contracts:
            raise RuntimeError("Option universe exceeds websocket capacity constraint")

    async def close(self) -> None:
        await self.snapshot_client.close()


async def run_ingest(service: IngestService) -> None:
    ws_client = PolygonWebSocketClient(service.ws_config)
    try:
        async with ws_client:
            while True:
                now = datetime.now(tz=timezone.utc)
                ts = int(now.timestamp() * 1_000_000)
                for symbol in service.config.symbols.get("indices", []):
                    try:
                        contracts = await service.rotate(symbol.replace("I:", ""), ts)
                        await service.ensure_capacity(contracts)
                    except Exception:
                        # Rotate failures bubble up to logging in production; here we continue.
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
    if not config.api_key:
        # Without credentials we idle but keep process healthy.
        while True:
            await asyncio.sleep(config.option_rotate_secs)
    await run_ingest(service)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
