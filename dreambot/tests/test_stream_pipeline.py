import asyncio
from contextlib import suppress

import pytest
from fakeredis import aioredis as fakeredis

from services.common.redis import publish_json
from services.common.streams import (
    AGG_STREAM,
    OMS_ORDER_STREAM,
    OPTION_META_STREAM,
    QUOTE_STREAM,
    RISK_ORDER_STREAM,
    SIGNAL_STREAM,
)
from services.features.main import FeatureEngine, load_feature_config, run_feature_stream
from services.ingest.schemas import Agg1s, OptionMeta, Quote
from services.signals.main import SignalEngine, run_signal_stream
from services.risk.main import (
    RiskService,
    build_risk_manager,
    default_scheduler,
    load_risk_config,
    run_risk_stream,
)
from services.risk.econ_calendar import EconCalendar
from services.oms.main import OMSConfig, OMSService, run_oms_stream


@pytest.fixture(scope="module")
def feature_config():
    return load_feature_config()


@pytest.mark.asyncio
async def test_stream_pipeline_emits_signal(feature_config):
    redis = fakeredis.FakeRedis(decode_responses=True)

    feature_engine = FeatureEngine(feature_config)
    feature_stop = asyncio.Event()
    feature_task = asyncio.create_task(run_feature_stream(feature_engine, redis, stop_event=feature_stop))

    gate_config = {
        "nbbo_age_ms_max": 10_000,
        "spread_pct_max": 1.0,
        "trend_threshold": -1.0,
        "adx_threshold": 0.0,
        "pot_threshold": 0.1,
    }
    signal_engine = SignalEngine(gate_config)
    signal_stop = asyncio.Event()
    signal_task = asyncio.create_task(
        run_signal_stream(signal_engine, redis, learner_adjustments={}, stop_event=signal_stop)
    )

    risk_config = load_risk_config()
    risk_service = RiskService(
        manager=build_risk_manager(risk_config),
        calendar=EconCalendar([]),
        scheduler=default_scheduler(),
    )
    risk_service.manager.set_session_start(1700000000000000 - 180_000_000)
    risk_stop = asyncio.Event()
    risk_task = asyncio.create_task(run_risk_stream(risk_service, redis, stop_event=risk_stop))

    oms_config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    oms_service = OMSService(oms_config)
    oms_stop = asyncio.Event()
    oms_task = asyncio.create_task(run_oms_stream(oms_service, redis, stop_event=oms_stop))

    try:
        ts = 1700000000000000
        quote = Quote(
            ts=ts,
            symbol="SPY",
            bid=449.95,
            ask=450.05,
            mid=450.0,
            bid_size=100,
            ask_size=100,
            nbbo_age_ms=5,
        )
        await publish_json(redis, QUOTE_STREAM, quote.to_dict())

        option = OptionMeta(
            ts=ts,
            underlying="SPY",
            symbol="SPYTESTC",
            strike=450.0,
            type="C",
            exp="2024-12-20",
            iv=0.22,
            delta=0.5,
            gamma=0.1,
            vega=0.05,
            theta=-0.12,
            oi=20000,
            prev_oi=19500,
        )
        await publish_json(redis, OPTION_META_STREAM, option.to_dict())

        agg = Agg1s(ts=ts, symbol="SPY", o=450.0, h=450.2, l=449.8, c=450.1, v=120000)
        await publish_json(redis, AGG_STREAM, agg.to_dict())

        await asyncio.sleep(0.2)
        signals = await redis.xread({SIGNAL_STREAM: "0-0"}, count=1, block=1000)
        assert signals, "signal stream remained empty"

        risk_entries = await redis.xread({RISK_ORDER_STREAM: "0-0"}, count=1, block=1000)
        assert risk_entries, "risk stream remained empty"

        oms_entries = await redis.xread({OMS_ORDER_STREAM: "0-0"}, count=1, block=1000)
        assert oms_entries, "oms stream remained empty"
    finally:
        feature_stop.set()
        signal_stop.set()
        risk_stop.set()
        oms_stop.set()
        for task in (feature_task, signal_task, risk_task, oms_task):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
