"""Backtest CLI entry."""
from __future__ import annotations

import argparse
from pathlib import Path

from ..features.main import FeatureEngine
from ..signals.main import SignalEngine
from ..learner.main import LearnerService
from ..oms.main import OMSConfig
from .fill_model import FillModel
from .metrics import summarize
from .replay import BacktestConfig, BacktestRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DreamBot backtest runner")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--seed", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    feature_config = {
        "vwap": {"bands_sigmas": [1, 2, 3], "band_stdev_window_secs": 600},
        "atr": {"fast_secs": 10, "min_lookback": 14},
        "adx": {"tf_minutes": 3},
        "microstructure": {"spread_stress_z": 1.25, "es_nq_lead_confirm_secs": 10},
        "vol_surface": {"term_days": [9, 30, 60], "skew_delta": 0.25},
    }
    engine = FeatureEngine(feature_config)
    gate_config = {
        "nbbo_age_ms_max": 800,
        "spread_pct_max": 0.01,
        "trend_threshold": -0.2,
        "adx_threshold": 20,
        "pot_threshold": 0.55,
    }
    signal_engine = SignalEngine(gate_config)
    learner = LearnerService(Path("backtests/calibration.json"))
    fill_model = FillModel()
    runner = BacktestRunner(
        feature_engine=engine,
        signal_engine=signal_engine,
        learner=learner,
        fill_model=fill_model,
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
        seed=args.seed,
    )
    # Placeholder: real implementation would load bars from DuckDB and run replay
    print(f"Backtest stub for {args.symbol} complete.")


if __name__ == "__main__":
    main()
