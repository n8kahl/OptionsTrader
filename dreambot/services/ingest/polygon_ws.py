"""Polygon websocket client wrapper."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, Iterable, List, Mapping, Tuple

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
    def __init__(
        self,
        max_contracts: int,
        strikes_around_atm: int,
        rotate_secs: int,
        *,
        delta_range: Tuple[float, float] = (0.0, 1.0),
        dte_range: Tuple[int, int] = (0, 365),
    ):
        self.max_contracts = max_contracts
        self.strikes_around_atm = strikes_around_atm
        self.rotate_secs = rotate_secs
        self.delta_range = delta_range
        self.dte_range = dte_range
        self._last_rotation_ts: Dict[str, int] = {}
        self._universe: Dict[str, List[str]] = {}

    def build_universe(self, underlying: str, chain: Iterable[OptionMeta], ts: int) -> UniverseRotation:
        contracts = self._universe.get(underlying, [])
        rotate_interval_us = max(self.rotate_secs, 0) * 1_000_000
        last_rotation = self._last_rotation_ts.get(underlying, 0)
        if rotate_interval_us and ts - last_rotation < rotate_interval_us:
            return UniverseRotation(ts=ts, underlying=underlying, contracts=contracts)

        filtered = self._filter_chain(chain, ts)
        if not filtered:
            return UniverseRotation(ts=ts, underlying=underlying, contracts=contracts)

        sorted_chain = sorted(filtered, key=self._sort_key)
        selection = [opt.symbol for opt in sorted_chain[: self.max_contracts]]
        self._universe[underlying] = selection
        self._last_rotation_ts[underlying] = ts
        return UniverseRotation(ts=ts, underlying=underlying, contracts=selection)

    def contracts(self, underlying: str) -> List[str]:
        return self._universe.get(underlying, [])

    def _filter_chain(self, chain: Iterable[OptionMeta], ts: int) -> List[OptionMeta]:
        now = self._ts_to_datetime(ts)
        results: List[OptionMeta] = []
        delta_low, delta_high = self.delta_range
        dte_low, dte_high = self.dte_range
        for option in chain:
            if not option.symbol:
                continue
            delta = abs(getattr(option, "delta", 0.0) or 0.0)
            if delta_low <= delta <= delta_high:
                dte = self._calculate_dte(option.exp, now)
                if dte is None:
                    continue
                if dte_low <= dte <= dte_high:
                    results.append(option)
        return results

    @staticmethod
    def _ts_to_datetime(ts: int) -> datetime:
        if ts <= 0:
            return datetime.now(tz=timezone.utc)
        seconds = ts / 1_000_000
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    @staticmethod
    def _calculate_dte(expiry: str, now: datetime) -> float | None:
        if not expiry:
            return None
        try:
            if "T" in expiry:
                exp_dt = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            else:
                exp_dt = datetime.strptime(expiry, "%Y-%m-%d")
        except ValueError:
            return None
        if exp_dt.tzinfo is None:
            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
        else:
            exp_dt = exp_dt.astimezone(timezone.utc)
        dte = (exp_dt - now).total_seconds() / 86_400
        return dte

    @staticmethod
    def _sort_key(option: OptionMeta) -> Tuple[float, float, float]:
        delta_dev = abs(abs(getattr(option, "delta", 0.0) or 0.0) - 0.5)
        oi_rank = -float(getattr(option, "oi", 0))
        strike_rank = abs(float(getattr(option, "strike", 0.0)))
        return (delta_dev, oi_rank, strike_rank)


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
