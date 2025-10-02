import asyncio
from pathlib import Path

import pytest
from fakeredis import aioredis as fakeredis

from services.common.audit import StreamAuditConfig, configure_auditor
from services.common.redis import publish_json


@pytest.mark.asyncio
async def test_publish_json_writes_audit(tmp_path: Path):
    configure_auditor(StreamAuditConfig(root=tmp_path))
    redis = fakeredis.FakeRedis(decode_responses=True)
    try:
        await publish_json(redis, "dreambot:test", {"foo": "bar"})
        await asyncio.sleep(0.05)
        audit_file = tmp_path / "dreambot_test.jsonl"
        assert audit_file.exists(), "audit log file missing"
        lines = audit_file.read_text(encoding="utf-8").strip().splitlines()
        assert lines, "audit log empty"
    finally:
        configure_auditor(None)
        await redis.aclose()
