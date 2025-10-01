import asyncio
from datetime import datetime, timezone

from services.ingest.polygon_ws import OptionUniverseManager, rotate_universe
from services.ingest.schemas import OptionMeta


def test_universe_capped_under_limit():
    manager = OptionUniverseManager(max_contracts=1000, strikes_around_atm=5, rotate_secs=0)
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000)
    chain = [
        OptionMeta(
            ts=now,
            underlying="SPY",
            symbol=f"SPY2412{i:03d}C00450000",
            strike=450 + i,
            type="C",
            exp="2024-12-20",
            iv=0.2,
            delta=0.5,
            gamma=0.1,
            vega=0.1,
            theta=-0.1,
            oi=1000,
            prev_oi=900,
        )
        for i in range(1500)
    ]
    rotation = asyncio.run(rotate_universe(manager, "SPY", chain, now))
    assert len(rotation.contracts) <= 1000


def test_ingest_capacity_guard():
    manager = OptionUniverseManager(max_contracts=5, strikes_around_atm=5, rotate_secs=0)
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000)
    chain = [
        OptionMeta(
            ts=now,
            underlying="SPY",
            symbol=f"SPY2412{i:03d}C00450000",
            strike=450 + i,
            type="C",
            exp="2024-12-20",
            iv=0.2,
            delta=0.5,
            gamma=0.1,
            vega=0.1,
            theta=-0.1,
            oi=1000,
            prev_oi=900,
        )
        for i in range(10)
    ]
    rotation = asyncio.run(rotate_universe(manager, "SPY", chain, now))
    assert len(rotation.contracts) <= 5
