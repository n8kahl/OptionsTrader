import asyncio

import pytest
from fakeredis import aioredis as fakeredis

from services.common.streams import OMS_METRIC_STREAM
from services.oms.main import OMSConfig, OMSService
from services.oms.schemas import OrderCommand, OrderRequest


class DummyTradierBroker:
    def __init__(self):
        self.payload = None

    async def place_order(self, payload):
        self.payload = payload
        return {"order": {"id": "T123", "status": "open"}}


def test_otoco_lifecycle_and_stop_sync():
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    service = OMSService(config)
    request = OrderRequest(
        ts=1700000000000000,
        underlying="SPY",
        option_symbol="SPY2410C00450000",
        side="BUY",
        quantity=1,
        entry_price=1.5,
        target_price=1.7,
        stop_price=1.3,
        time_stop_secs=240,
        metadata={"playbook": "TREND_PULLBACK"},
    )
    status = asyncio.run(service.route_order(request))
    assert status.state == "filled"
    assert status.broker_payload["payload"]["legs"][0]["order_type"] == "limit"

    adjusted = service.sync_stop(1.3, 452.0, "BUY")
    assert adjusted >= 1.3


def test_tradier_payload_and_status_shape():
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
        provider="tradier",
        tradier_duration="day",
    )
    broker = DummyTradierBroker()
    service = OMSService(config, broker=broker)
    request = OrderRequest(
        ts=1700000000001234,
        underlying="QQQ",
        option_symbol="QQQ2410C00450000",
        side="BUY",
        quantity=2,
        entry_price=1.55,
        target_price=1.8,
        stop_price=1.35,
        time_stop_secs=180,
        metadata={"playbook": "BREAKOUT", "client_order_id": "demo-1"},
    )
    status = asyncio.run(service.route_order(request))
    payload = broker.payload
    assert payload["advanced"] == "otoco"
    assert payload["side"] == "buy_to_open"
    assert payload["orders[0][side]"] == "sell_to_close"
    assert payload["orders[0][price]"] == "1.80"
    assert payload["orders[1][stop]"] == "1.35"
    assert payload["client_order_id"] == "demo-1"
    assert status.order_id == "T123"
    assert status.state == "open"


@pytest.mark.asyncio
async def test_oms_handles_cancel_command():
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    service = OMSService(config)
    redis = fakeredis.FakeRedis(decode_responses=True)
    request = OrderRequest(
        ts=1700000000000001,
        underlying="SPY",
        option_symbol="SPY2410C00450000",
        side="BUY",
        quantity=1,
        entry_price=1.5,
        target_price=1.7,
        stop_price=1.3,
        time_stop_secs=60,
        metadata={"client_order_id": "cancel-1"},
    )
    status = await service.route_order(request)
    command = OrderCommand(action="cancel", client_order_id="cancel-1", order_id=status.order_id)
    cancelled = await service.handle_command(command, redis)
    assert cancelled is not None
    assert cancelled.state.lower() == "cancelled"


@pytest.mark.asyncio
async def test_oms_modify_updates_request():
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    service = OMSService(config)
    redis = fakeredis.FakeRedis(decode_responses=True)
    request = OrderRequest(
        ts=1700000001111111,
        underlying="QQQ",
        option_symbol="QQQ2410C00450000",
        side="BUY",
        quantity=1,
        entry_price=1.6,
        target_price=1.9,
        stop_price=1.4,
        time_stop_secs=120,
        metadata={"client_order_id": "modify-1"},
    )
    status = await service.route_order(request)
    command = OrderCommand(
        action="modify",
        client_order_id="modify-1",
        order_id=status.order_id,
        stop_price=1.45,
        target_price=1.95,
    )
    modified = await service.handle_command(command, redis)
    assert modified is not None
    assert modified.request["stop_price"] == 1.45
    assert service._order_requests["modify-1"].stop_price == 1.45


@pytest.mark.asyncio
async def test_oms_metrics_written():
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    service = OMSService(config)
    redis = fakeredis.FakeRedis(decode_responses=True)
    request = OrderRequest(
        ts=1700000002222222,
        underlying="SPY",
        option_symbol="SPY2410C00450000",
        side="BUY",
        quantity=1,
        entry_price=1.5,
        target_price=1.7,
        stop_price=1.3,
        time_stop_secs=0,
        metadata={"client_order_id": "metrics-1"},
    )
    status = await service.route_order(request)
    await service.record_status(status, redis)
    entries = await redis.xread({OMS_METRIC_STREAM: "0-0"}, count=1, block=1000)
    assert entries, "expected metrics entry"


@pytest.mark.asyncio
async def test_oms_audit_writer(tmp_path, monkeypatch):
    audit_file = tmp_path / "oms_audit.jsonl"
    monkeypatch.setenv("OMS_AUDIT_PATH", str(audit_file))
    monkeypatch.setenv("OMS_AUDIT_ROTATE_MB", "0")
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    service = OMSService(config)
    status = await service.route_order(
        OrderRequest(
            ts=1700000003333333,
            underlying="QQQ",
            option_symbol="QQQ2410C00450000",
            side="BUY",
            quantity=1,
            entry_price=1.6,
            target_price=1.9,
            stop_price=1.4,
            time_stop_secs=0,
            metadata={"client_order_id": "audit-1"},
        )
    )
    await service.record_status(status, None)
    assert audit_file.exists()
    contents = audit_file.read_text(encoding="utf-8").strip().splitlines()
    assert contents and '"order_id"' in contents[0]
