import asyncio
import json

import pytest
from fakeredis import aioredis as fakeredis

from services.common.streams import RISK_COMMAND_STREAM
from services.risk.main import RiskService, build_risk_manager, default_scheduler, load_risk_config
from services.risk.econ_calendar import EconCalendar
from services.signals.schemas import EntryTrigger, SignalIntent
from services.oms.schemas import OrderStatus


@pytest.mark.asyncio
async def test_risk_time_stop_emits_cancel_command():
    redis = fakeredis.FakeRedis(decode_responses=True)
    config = load_risk_config()
    service = RiskService(
        manager=build_risk_manager(config),
        calendar=EconCalendar([]),
        scheduler=default_scheduler(),
    )
    signal = SignalIntent(
        ts=1700000000000000,
        underlying="SPY",
        side="BUY",
        playbook="TEST",
        entry_trigger=EntryTrigger(type="VWAP", band="MID", confirmations=[]),
        target_underlying_move=0.2,
        stop_underlying_move=-0.1,
        time_stop_secs=1,
        option_filters={},
    )
    order = await service.submit_signal(redis, signal)
    assert order is not None
    status = OrderStatus(
        ts=order.ts,
        order_id="ORD123",
        state="open",
        request=order.to_dict(),
        broker_payload={},
        fills=[],
    )
    await service.process_status(redis, status)
    await asyncio.sleep(1.05)
    entries = await redis.xread({RISK_COMMAND_STREAM: "0-0"}, count=1, block=1000)
    assert entries, "expected cancel command"
    payload = json.loads(entries[0][1][0][1]["data"])
    assert payload["action"].lower() == "cancel"
    assert payload["client_order_id"] == dict(order.metadata)["client_order_id"]


@pytest.mark.asyncio
async def test_risk_partial_fill_triggers_modify():
    redis = fakeredis.FakeRedis(decode_responses=True)
    config = load_risk_config()
    service = RiskService(
        manager=build_risk_manager(config),
        calendar=EconCalendar([]),
        scheduler=default_scheduler(),
    )
    signal = SignalIntent(
        ts=1700000000001111,
        underlying="QQQ",
        side="BUY",
        playbook="TEST",
        entry_trigger=EntryTrigger(type="VWAP", band="MID", confirmations=[]),
        target_underlying_move=0.2,
        stop_underlying_move=-0.1,
        time_stop_secs=10,
        option_filters={},
    )
    order = await service.submit_signal(redis, signal)
    assert order is not None
    client_id = dict(order.metadata)["client_order_id"]
    service.pending_orders[client_id].request.quantity = 2
    status = OrderStatus(
        ts=order.ts,
        order_id="ORD789",
        state="open",
        request=order.to_dict(),
        broker_payload={},
        fills=[{"qty": 1}],
    )
    await service.process_status(redis, status)
    entries = await redis.xread({RISK_COMMAND_STREAM: "0-0"}, count=1, block=1000)
    assert entries, "expected modify command"
    payload = json.loads(entries[0][1][0][1]["data"])
    assert payload["action"].lower() == "modify"
    assert "stop_price" in payload
