from pathlib import Path

import yaml

from services.backtest.fill_model import FillModel
from services.backtest.replay import BacktestConfig, BacktestRunner
from services.features.main import FeatureEngine
from services.ingest.schemas import Agg1s
from services.learner.main import LearnerService
from services.signals.main import SignalEngine


def generate_bars():
    bars = []
    for i in range(120):
        ts = 1700000000000000 + i * 1_000_000
        price = 450 + i * 0.01
        bars.append(Agg1s(ts=ts, symbol="SPY", o=price, h=price + 0.2, l=price - 0.2, c=price, v=2000 + i))
    return bars


def make_runner(seed: int):
    with Path("config/features.yaml").open("r", encoding="utf-8") as handle:
        feature_config = yaml.safe_load(handle)
    gate_config = {
        "nbbo_age_ms_max": 800,
        "spread_pct_max": 0.01,
        "trend_threshold": -0.2,
        "adx_threshold": 20,
        "pot_threshold": 0.55,
    }
    return BacktestRunner(
        feature_engine=FeatureEngine(feature_config),
        signal_engine=SignalEngine(gate_config),
        learner=LearnerService(Path("backtests/calibration.json")),
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
        seed=seed,
    )


def test_backtest_determinism():
    bars = generate_bars()
    runner_a = make_runner(seed=123)
    runner_b = make_runner(seed=123)
    result_a = runner_a.replay("SPY", bars)
    result_b = runner_b.replay("SPY", bars)
    assert [t.pnl for t in result_a.trades] == [t.pnl for t in result_b.trades]
