import json
from pathlib import Path

import pytest
from fakeredis import aioredis as fakeredis

from services.common.streams import AGG_STREAM, INGEST_HEARTBEAT_STREAM, QUOTE_STREAM
from services.ingest.main import (
    IngestConfig,
    IngestService,
    publish_heartbeat,
    replay_messages,
)
from services.ingest.schemas import Quote


def _build_config(tmp_path: Path | None = None) -> IngestConfig:
    snapshot_path = str(tmp_path) if tmp_path else None
    return IngestConfig(
        api_key="",
        symbols={"stocks": ["SPY"], "indices": [], "options": []},
        option_rotate_secs=30,
        max_contracts=50,
        strikes_around_atm=3,
        delta_range=(0.3, 0.6),
        dte_range=(0, 5),
        enable_stocks_ws=True,
        enable_indices_ws=False,
        enable_options_ws=False,
        heartbeat_secs=1,
        snapshot_path=snapshot_path,
        snapshot_rotate_bytes=1024,
    )


@pytest.mark.asyncio
async def test_publish_heartbeat_and_snapshot(tmp_path: Path):
    config = _build_config(tmp_path)
    service = IngestService(config)
    redis = fakeredis.FakeRedis(decode_responses=True)

    quote = Quote(
        ts=1700000000000000,
        symbol="SPY",
        bid=449.9,
        ask=450.1,
        mid=450.0,
        bid_size=10,
        ask_size=12,
        nbbo_age_ms=5,
    )
    service.record_quote(quote)
    await publish_heartbeat(service, redis)
    await service.record_snapshot('[{"ev":"Q","sym":"SPY"}]')

    entries = await redis.xread({INGEST_HEARTBEAT_STREAM: "0-0"}, count=1, block=1000)
    assert entries, "heartbeat stream should receive entry"
    heartbeat = entries[0][1][0][1]
    payload = json.loads(heartbeat["data"])
    assert "quotes" in payload

    snapshot_file = tmp_path / "polygon_messages.jsonl"
    assert snapshot_file.exists()
    content = snapshot_file.read_text(encoding="utf-8").strip().splitlines()
    assert content and "SPY" in content[0]


@pytest.mark.asyncio
async def test_replay_messages_populates_streams():
    config = _build_config()
    service = IngestService(config)
    redis = fakeredis.FakeRedis(decode_responses=True)
    quote_msg = '[{"ev":"Q","sym":"SPY","bp":449.9,"ap":450.1,"bs":10,"as":12,"t":1700000000000}]'
    agg_msg = '{"ev":"A","sym":"SPY","o":449.5,"c":450.2,"h":450.5,"l":449.0,"v":12000,"s":1700000000000}'
    await replay_messages(service, redis, [quote_msg, agg_msg])

    quote_entries = await redis.xread({QUOTE_STREAM: "0-0"}, count=1, block=1000)
    assert quote_entries, "Quote stream remained empty after replay"
    agg_entries = await redis.xread({AGG_STREAM: "0-0"}, count=1, block=1000)
    assert agg_entries, "Agg stream remained empty after replay"
