"""Polygon websocket client wrapper."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncIterator, Dict, Iterable, List, Mapping

try:
    import websockets
except ImportError:  # pragma: no cover - optional for tests
    websockets = None

from .schemas import OptionMeta, UniverseRotation


@dataclass
class WebSocketConfig:
    api_key: str
    symbols: Mapping[str, List[str]]
    max_contracts_per_conn: int


class OptionUniverseManager:
    def __init__(self, max_contracts: int, strikes_around_atm: int, rotate_secs: int):
        self.max_contracts = max_contracts
        self.strikes_around_atm = strikes_around_atm
        self.rotate_secs = rotate_secs
        self._last_rotation_ts: int = 0
        self._universe: Dict[str, List[str]] = {}

    def build_universe(self, underlying: str, chain: Iterable[OptionMeta], ts: int) -> UniverseRotation:
        if ts - self._last_rotation_ts < self.rotate_secs:
            return UniverseRotation(ts=ts, underlying=underlying, contracts=self._universe.get(underlying, []))
        sorted_chain = sorted(chain, key=lambda opt: (abs(opt.delta - 0.5), abs(opt.strike)))
        selection = [opt.symbol for opt in sorted_chain[: self.max_contracts]]
        self._universe[underlying] = selection
        self._last_rotation_ts = ts
        return UniverseRotation(ts=ts, underlying=underlying, contracts=selection)

    def contracts(self, underlying: str) -> List[str]:
        return self._universe.get(underlying, [])


class PolygonWebSocketClient:
    def __init__(self, config: WebSocketConfig):
        self.config = config
        self.connection = None

    async def __aenter__(self) -> "PolygonWebSocketClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def connect(self) -> None:
        if websockets is None:
            return
        url = "wss://socket.polygon.io/options"
        self.connection = await websockets.connect(url)
        await self.connection.send(f"{{\"action\":\"auth\",\"params\":\"{self.config.api_key}\"}}")

    async def close(self) -> None:
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def subscribe(self, channels: Iterable[str]) -> None:
        if not self.connection:
            return
        channel_list = ",".join(channels)
        await self.connection.send(f"{{\"action\":\"subscribe\",\"params\":\"{channel_list}\"}}")

    async def listen(self) -> AsyncIterator[str]:
        if not self.connection:
            return
        while True:
            message = await self.connection.recv()
            yield message


async def rotate_universe(manager: OptionUniverseManager, underlying: str,
                          options: Iterable[OptionMeta], ts: int) -> UniverseRotation:
    rotation = manager.build_universe(underlying, options, ts)
    if len(rotation.contracts) > manager.max_contracts:
        rotation.contracts = rotation.contracts[: manager.max_contracts]
    return rotation
