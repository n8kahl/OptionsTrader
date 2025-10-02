"""Ingest service entrypoint."""
from __future__ import annotations

import asyncio
import json
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, Set, Tuple

import yaml
from redis.asyncio import Redis

try:  # pragma: no cover - optional in tests
    import websockets
except ImportError:  # pragma: no cover - optional in tests
    websockets = None

from ..common.redis import close_redis, create_redis, publish_json
from ..common.streams import AGG_STREAM, INGEST_HEARTBEAT_STREAM, OPTION_META_STREAM, QUOTE_STREAM
from .chain_snapshot import ChainSnapshotClient
from .recording import SnapshotRecorder
from .polygon_ws import OptionUniverseManager
from .schemas import Agg1s, OptionMeta, Quote, UniverseRotation

POLYGON_WS_URL = "wss://socket.polygon.io"


@dataclass
class IngestConfig:
    api_key: str
    symbols: Mapping[str, List[str]]
    option_rotate_secs: int
    max_contracts: int
    strikes_around_atm: int
    delta_range: Tuple[float, float]
    dte_range: Tuple[int, int]
    enable_stocks_ws: bool
    enable_indices_ws: bool
    enable_options_ws: bool
    heartbeat_secs: int
    snapshot_path: str | None
    snapshot_rotate_bytes: int


class IngestService:
    def __init__(self, config: IngestConfig):
        self.config = config
        self.universe_manager = OptionUniverseManager(
            max_contracts=config.max_contracts,
            strikes_around_atm=config.strikes_around_atm,
            rotate_secs=config.option_rotate_secs,
            delta_range=config.delta_range,
            dte_range=config.dte_range,
        )
        self.snapshot_client = ChainSnapshotClient(config.api_key) if config.api_key else None
        self._option_universe: Dict[str, List[str]] = {}
        self._current_option_contracts: Set[str] = set()
        self.option_rotation_queue: asyncio.Queue[Set[str]] = asyncio.Queue()
        snapshot_path = config.snapshot_path
        self.snapshot_recorder = SnapshotRecorder(snapshot_path, config.snapshot_rotate_bytes) if snapshot_path else None
        self.heartbeat_secs = max(config.heartbeat_secs, 1)
        self._stats = {
            "quotes": {"count": 0, "last_ts": 0.0},
            "aggs": {"count": 0, "last_ts": 0.0},
            "option_meta": {"count": 0, "last_ts": 0.0},
        }
        all_symbols = set()
        for bucket in config.symbols.values():
            all_symbols.update(bucket)
        self._synthetic_prices: Dict[str, float] = {symbol: 400.0 for symbol in all_symbols}

    def tracked_symbols(self, bucket: str) -> List[str]:
        return self.config.symbols.get(bucket, [])

    async def rotate(self, underlying: str, ts: int, chain: Sequence[OptionMeta]) -> List[str]:
        rotation = await rotate_universe(self.universe_manager, underlying, chain, ts)
        await self.update_option_universe(underlying, rotation.contracts)
        return rotation.contracts

    async def ensure_capacity(self, contracts: Iterable[str]) -> None:
        unique_contracts = {contract for contract in contracts if contract}
        if len(unique_contracts) > self.config.max_contracts:
            raise RuntimeError("Option universe exceeds websocket capacity constraint")

    async def update_option_universe(self, underlying: str, contracts: Sequence[str]) -> None:
        self._option_universe[underlying] = [c for c in dict.fromkeys(contracts) if c]
        if not self.config.enable_options_ws:
            return
        union_set: Set[str] = set()
        for bucket_contracts in self._option_universe.values():
            union_set.update(bucket_contracts)
        await self.ensure_capacity(union_set)
        if union_set != self._current_option_contracts:
            new_snapshot = set(union_set)
            self._current_option_contracts = new_snapshot
            await self.option_rotation_queue.put(set(new_snapshot))

    async def close(self) -> None:
        if self.snapshot_client:
            await self.snapshot_client.close()

    async def publish_synthetic_batch(self, redis: Redis, ts: int) -> None:
        for symbol in self.tracked_symbols("stocks") + self.tracked_symbols("indices"):
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
            self.record_quote(quote)

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
            self.record_agg(agg)

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
            self.record_option_meta(option_meta)

    def _step_price(self, symbol: str) -> float:
        base = self._synthetic_prices.setdefault(symbol, 400.0)
        jitter = math.sin(time.time()) * 0.2 + random.uniform(-0.1, 0.1)
        base = max(base + jitter, 1.0)
        self._synthetic_prices[symbol] = base
        return round(base, 2)

    def record_quote(self, quote: Quote) -> None:
        self._stats["quotes"]["count"] += 1
        self._stats["quotes"]["last_ts"] = max(self._stats["quotes"]["last_ts"], quote.ts / 1_000_000.0)

    def record_agg(self, agg: Agg1s) -> None:
        self._stats["aggs"]["count"] += 1
        self._stats["aggs"]["last_ts"] = max(self._stats["aggs"]["last_ts"], agg.ts / 1_000_000.0)

    def record_option_meta(self, option: OptionMeta) -> None:
        self._stats["option_meta"]["count"] += 1
        self._stats["option_meta"]["last_ts"] = max(
            self._stats["option_meta"]["last_ts"], option.ts / 1_000_000.0
        )

    async def record_snapshot(self, raw_message: str) -> None:
        if not raw_message or self.snapshot_recorder is None:
            return
        await self.snapshot_recorder.write(raw_message)

    def heartbeat_payload(self) -> Dict[str, object]:
        now = time.time()
        def _age(stat: Dict[str, float]) -> float | None:
            last = stat.get("last_ts", 0.0)
            if not last:
                return None
            return max((now - last) * 1_000.0, 0.0)

        return {
            "ts": int(now * 1_000_000),
            "mode": "live" if self.config.api_key else "synthetic",
            "quotes": {
                "count": self._stats["quotes"]["count"],
                "age_ms": _age(self._stats["quotes"]),
            },
            "aggs": {
                "count": self._stats["aggs"]["count"],
                "age_ms": _age(self._stats["aggs"]),
            },
            "option_meta": {
                "count": self._stats["option_meta"]["count"],
                "age_ms": _age(self._stats["option_meta"]),
            },
        }


async def rotate_universe(
    manager: OptionUniverseManager,
    underlying: str,
    options: Iterable[OptionMeta],
    ts: int,
) -> UniverseRotation:
    rotation = manager.build_universe(underlying, options, ts)
    if len(rotation.contracts) > manager.max_contracts:
        rotation.contracts = rotation.contracts[: manager.max_contracts]
    return rotation


def _quote_from_polygon(entry: Mapping[str, object]) -> Quote | None:
    ev = entry.get("ev") or entry.get("eventType")
    if ev not in {"Q", "Iq", "Cx", "XQ"}:
        return None
    symbol = str(entry.get("sym"))
    bid = float(entry.get("bp", entry.get("bidPrice", 0.0)))
    ask = float(entry.get("ap", entry.get("askPrice", 0.0)))
    if bid <= 0 or ask <= 0:
        return None
    ts_raw = int(entry.get("t", entry.get("timestamp", 0)))
    ts = ts_raw * 1000 if ts_raw < 10**15 else ts_raw  # convert ms → µs if needed
    return Quote(
        ts=ts,
        symbol=symbol,
        bid=bid,
        ask=ask,
        mid=round((bid + ask) / 2, 5),
        bid_size=float(entry.get("bs", entry.get("bidSize", 0.0))),
        ask_size=float(entry.get("as", entry.get("askSize", 0.0))),
        nbbo_age_ms=entry.get("participants", 0) if isinstance(entry.get("participants", 0), int) else 0,
    )


def _agg_from_polygon(entry: Mapping[str, object]) -> Agg1s | None:
    ev = entry.get("ev")
    if ev not in {"A", "AM", "XA"}:
        return None
    symbol = str(entry.get("sym"))
    o = float(entry.get("o", entry.get("open", 0.0)))
    c = float(entry.get("c", entry.get("close", 0.0)))
    h = float(entry.get("h", entry.get("high", max(o, c))))
    l = float(entry.get("l", entry.get("low", min(o, c))))
    v = float(entry.get("v", entry.get("volume", 0.0)))
    ts_raw = int(entry.get("s", entry.get("t", 0)))
    ts = ts_raw * 1000 if ts_raw < 10**15 else ts_raw
    return Agg1s(ts=ts, symbol=symbol, o=o, h=h, l=l, c=c, v=v)


def _channels_for_symbols(symbols: Iterable[str]) -> List[str]:
    channels: List[str] = []
    for symbol in symbols:
        if not symbol:
            continue
        channels.append(f"Q.{symbol}")
        channels.append(f"A.{symbol}")
    return channels


async def _handle_polygon_message(service: IngestService, redis: Redis, message: str) -> None:
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return
    if isinstance(payload, dict):
        payload = [payload]
    for entry in payload:
        await _process_polygon_entry(service, redis, entry)


async def _process_polygon_entry(
    service: IngestService,
    redis: Redis,
    entry: Mapping[str, object],
) -> None:
    quote = _quote_from_polygon(entry)
    if quote:
        await publish_json(redis, QUOTE_STREAM, quote.to_dict())
        service.record_quote(quote)
    agg = _agg_from_polygon(entry)
    if agg:
        await publish_json(redis, AGG_STREAM, agg.to_dict())
        service.record_agg(agg)


async def _stream_polygon_cluster(
    cluster: str,
    api_key: str,
    symbols: Sequence[str],
    redis: Redis,
    service: IngestService,
) -> None:
    if not symbols:
        return
    if websockets is None:
        return
    channels = _channels_for_symbols(symbols)
    if not channels:
        return
    url = f"{POLYGON_WS_URL}/{cluster}"
    backoff = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"action": "auth", "params": api_key}))
                await ws.send(json.dumps({"action": "subscribe", "params": ",".join(channels)}))
                backoff = 1
                async for message in ws:
                    await service.record_snapshot(message)
                    await _handle_polygon_message(service, redis, message)
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def _stream_polygon_options(service: IngestService, redis: Redis) -> None:
    if not service.config.enable_options_ws:
        return
    if websockets is None:
        return
    if not service.config.api_key:
        return
    url = f"{POLYGON_WS_URL}/options"
    backoff = 1
    current: Set[str] = set()
    queue = service.option_rotation_queue
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"action": "auth", "params": service.config.api_key}))
                if current:
                    channels = _channels_for_symbols(sorted(current))
                    if channels:
                        await ws.send(json.dumps({"action": "subscribe", "params": ",".join(channels)}))
                backoff = 1
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        message = None
                    while True:
                        try:
                            new_symbols = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        else:
                            queue.task_done()
                            target = set(new_symbols)
                            additions = target - current
                            removals = current - target
                            if additions:
                                add_channels = _channels_for_symbols(sorted(additions))
                                if add_channels:
                                    await ws.send(
                                        json.dumps({
                                            "action": "subscribe",
                                            "params": ",".join(add_channels),
                                        })
                                    )
                            if removals:
                                remove_channels = _channels_for_symbols(sorted(removals))
                                if remove_channels:
                                    await ws.send(
                                        json.dumps({
                                            "action": "unsubscribe",
                                            "params": ",".join(remove_channels),
                                        })
                                    )
                            current = target
                    if message is None:
                        continue
                    await service.record_snapshot(message)
                    await _handle_polygon_message(service, redis, message)
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def _option_snapshot_loop(service: IngestService, redis: Redis) -> None:
    if not service.snapshot_client:
        return
    while True:
        ts = int(time.time() * 1_000_000)
        for underlying in service.tracked_symbols("stocks") + service.tracked_symbols("indices"):
            try:
                chain = await service.snapshot_client.fetch_chain(
                    underlying,
                    min_dte=service.config.dte_range[0],
                    max_dte=service.config.dte_range[1],
                    max_options=service.config.max_contracts,
                )
                for option in chain:
                    await publish_json(redis, OPTION_META_STREAM, option.to_dict())
                await service.rotate(underlying, ts, chain)
            except Exception:
                continue
        await asyncio.sleep(service.config.option_rotate_secs)


async def publish_heartbeat(service: IngestService, redis: Redis) -> None:
    payload = service.heartbeat_payload()
    await publish_json(redis, INGEST_HEARTBEAT_STREAM, payload)


async def _heartbeat_loop(service: IngestService, redis: Redis) -> None:
    interval = service.heartbeat_secs
    while True:
        await publish_heartbeat(service, redis)
        await asyncio.sleep(interval)


async def replay_messages(
    service: IngestService, redis: Redis, messages: Iterable[str]
) -> None:
    for message in messages:
        await _handle_polygon_message(service, redis, message)


async def replay_snapshot_file(service: IngestService, redis: Redis, path: Path) -> None:
    lines = (line.strip() for line in path.read_text(encoding="utf-8").splitlines())
    to_replay = [line for line in lines if line]
    await replay_messages(service, redis, to_replay)


async def run_ingest(service: IngestService, redis: Redis) -> None:
    if not service.config.api_key or websockets is None:
        while True:
            ts = int(time.time() * 1_000_000)
            await service.publish_synthetic_batch(redis, ts)
            await publish_heartbeat(service, redis)
            await asyncio.sleep(service.heartbeat_secs)
        return

    stream_tasks: List[asyncio.Task] = []
    if service.config.enable_stocks_ws:
        symbols = service.tracked_symbols("stocks")
        if symbols:
            stream_tasks.append(
                asyncio.create_task(
                    _stream_polygon_cluster(
                        "stocks", service.config.api_key, symbols, redis, service
                    )
                )
            )
    if service.config.enable_indices_ws:
        symbols = service.tracked_symbols("indices")
        if symbols:
            stream_tasks.append(
                asyncio.create_task(
                    _stream_polygon_cluster(
                        "indices", service.config.api_key, symbols, redis, service
                    )
                )
            )
    if service.config.enable_options_ws:
        stream_tasks.append(asyncio.create_task(_stream_polygon_options(service, redis)))

    option_snapshot_task = asyncio.create_task(_option_snapshot_loop(service, redis))
    heartbeat_task = asyncio.create_task(_heartbeat_loop(service, redis))
    tasks = [option_snapshot_task, heartbeat_task, *stream_tasks]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def load_ingest_config() -> IngestConfig:
    data_cfg = yaml.safe_load(Path("config/data.yaml").read_text(encoding="utf-8"))
    symbols_cfg = yaml.safe_load(Path("config/symbols.yaml").read_text(encoding="utf-8"))
    polygon_cfg = data_cfg["polygon"]
    delta_range_cfg = symbols_cfg.get("delta_range", [0.0, 1.0])
    delta_low, delta_high = (float(delta_range_cfg[0]), float(delta_range_cfg[1]))
    dte_range_cfg = symbols_cfg.get("dte_range", [0, 365])
    dte_low, dte_high = (int(dte_range_cfg[0]), int(dte_range_cfg[1]))
    heartbeat_secs = int(os.environ.get("POLYGON_HEARTBEAT_SECS", polygon_cfg.get("heartbeat_secs", 5)))
    snapshot_path = os.environ.get("POLYGON_SNAPSHOT_PATH", polygon_cfg.get("snapshot_path"))
    rotate_mb = float(
        os.environ.get(
            "POLYGON_SNAPSHOT_ROTATE_MB",
            polygon_cfg.get("snapshot_rotate_mb", 50),
        )
    )
    snapshot_rotate_bytes = int(max(rotate_mb, 0.0) * 1_048_576)
    if snapshot_path:
        snapshot_path = str(Path(snapshot_path))
    return IngestConfig(
        api_key=os.environ.get("POLYGON_API_KEY", ""),
        symbols=polygon_cfg["symbols"],
        option_rotate_secs=polygon_cfg["option_chain_snapshot_secs"],
        max_contracts=polygon_cfg["max_contracts_per_conn"],
        strikes_around_atm=symbols_cfg["strikes_around_atm"],
        delta_range=(delta_low, delta_high),
        dte_range=(dte_low, dte_high),
        enable_stocks_ws=polygon_cfg.get("stocks_ws", True),
        enable_indices_ws=polygon_cfg.get("indices_ws", True),
        enable_options_ws=polygon_cfg.get("options_ws", True),
        heartbeat_secs=heartbeat_secs,
        snapshot_path=snapshot_path,
        snapshot_rotate_bytes=snapshot_rotate_bytes,
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
