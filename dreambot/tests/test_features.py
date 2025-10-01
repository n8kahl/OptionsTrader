import math
from pathlib import Path

import numpy as np
import pytest
import yaml

from services.features.main import FeatureEngine
from services.ingest.schemas import Agg1s
from services.backtest.replay import BacktestConfig, BacktestRunner
from services.signals.main import SignalEngine
from services.learner.main import LearnerService
from services.backtest.fill_model import FillModel


@pytest.fixture(scope="module")
def feature_config():
    with Path("config/features.yaml").open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


@pytest.fixture(scope="module")
def gate_config():
    return {
        "nbbo_age_ms_max": 800,
        "spread_pct_max": 0.01,
        "trend_threshold": -0.2,
        "adx_threshold": 20,
        "pot_threshold": 0.55,
    }


def generate_bars(count: int, start_price: float = 450.0) -> list[Agg1s]:
    bars: list[Agg1s] = []
    price = start_price
    rng = np.random.default_rng(0)
    for i in range(count):
        ts = 1700000000000000 + i * 1_000_000
        move = rng.normal(0, 0.2)
        price = max(price + move, 1)
        high = price + rng.uniform(0, 0.4)
        low = price - rng.uniform(0, 0.4)
        volume = rng.uniform(1_000, 5_000)
        bars.append(Agg1s(ts=ts, symbol="SPY", o=price, h=high, l=low, c=price, v=volume))
    return bars


def test_feature_parity(feature_config, gate_config, tmp_path):
    bars = generate_bars(180)
    live_engine = FeatureEngine(feature_config)
    live_features = [live_engine.compute_features("SPY", bar) for bar in bars]

    backtest_engine = FeatureEngine(feature_config)
    learner = LearnerService(tmp_path / "calibration.json")
    runner = BacktestRunner(
        feature_engine=backtest_engine,
        signal_engine=SignalEngine(gate_config),
        learner=learner,
        fill_model=FillModel(),
        config=BacktestConfig(
            risk={
                "daily_loss_cap": -500,
                "per_trade_max_risk_pct": 0.7,
                "max_concurrent_positions": 2,
                "no_trade_first_seconds": 90,
                "econ_halt_minutes_pre_post": 3,
                "force_flat_before_close_secs": 180,
                "defensive_mode": {"slippage_z": 2.0, "spread_z": 2.0},
            },
            gate=gate_config,
            oms={
                "paper": True,
                "order_type": "marketable_limit",
                "use_otoco": True,
                "default_limit_offset_ticks": 1,
                "modify_stop_on_underlying": True,
            },
        ),
        seed=42,
    )
    replay_result = runner.replay("SPY", bars)

    assert len(live_features) == len(replay_result.features)
    for live, replay in zip(live_features[-50:], replay_result.features[-50:]):
        assert math.isclose(live.vwap, replay.vwap, rel_tol=1e-5)
        assert math.isclose(live.atr_1m, replay.atr_1m, rel_tol=1e-5)
        assert math.isclose(live.rv_5m, replay.rv_5m, rel_tol=1e-5)
