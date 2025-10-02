"""Backtest CLI entry."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..features.main import FeatureEngine, load_feature_config
from ..signals.main import SignalEngine, load_gate_config
from ..learner.main import LearnerService
from .data_loader import load_bars
from .fill_model import FillModel
from .replay import BacktestConfig, BacktestRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DreamBot backtest runner")
    parser.add_argument("--symbol", default="SPY", help="Underlying symbol to backtest")
    parser.add_argument("--data", help="Path to CSV with ts,o,h,l,c,v columns", default=None)
    parser.add_argument("--limit", type=int, help="Optional limit on number of bars", default=None)
    parser.add_argument("--table", help="Table name when reading DuckDB sources", default=None)
    parser.add_argument("--output", help="Optional JSON summary output path", default=None)
    parser.add_argument(
        "--trades-output",
        help="Optional JSON file capturing individual trade results",
        default=None,
    )
    parser.add_argument("--seed", type=int, default=0, help="Reserved for reproducibility")
    return parser.parse_args()


def build_runner(seed: int) -> BacktestRunner:
    feature_config = load_feature_config()
    gate_config = load_gate_config()
    learner = LearnerService(Path("backtests/calibration.json"))
    fill_model = FillModel()
    config = BacktestConfig(
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
    )
    return BacktestRunner(
        feature_engine=FeatureEngine(feature_config),
        signal_engine=SignalEngine(gate_config),
        learner=learner,
        fill_model=fill_model,
        config=config,
        seed=seed,
    )


def main() -> None:
    args = parse_args()
    bars = load_bars(args.symbol, data_path=args.data, limit=args.limit, table=args.table)
    if not bars:
        raise SystemExit("No bars available for backtest")
    runner = build_runner(args.seed)
    result = runner.replay(args.symbol.upper(), bars)
    report = result.report
    summary = {
        "symbol": args.symbol.upper(),
        "trades": len(result.trades),
        "expectancy": round(report.expectancy, 6),
        "win_rate": round(report.win_rate, 4),
        "avg_win": round(report.avg_win, 6),
        "avg_loss": round(report.avg_loss, 6),
        "max_drawdown": round(report.max_drawdown, 6),
    }
    print(json.dumps(summary, indent=2))
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.trades_output:
        trades_path = Path(args.trades_output)
        trades_path.parent.mkdir(parents=True, exist_ok=True)
        trades_payload = [trade.to_dict() for trade in result.trades]
        trades_path.write_text(json.dumps(trades_payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
