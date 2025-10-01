"""Backtest calibration CLI."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

from ..features.main import FeatureEngine, load_feature_config
from ..signals.main import SignalEngine, load_gate_config
from ..learner.main import LearnerService
from .data_loader import load_bars
from .fill_model import FillModel
from .metrics import Trade
from .replay import BacktestConfig, BacktestRunner


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


def aggregate_playbook_metrics(trades: Iterable[Trade]) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "pnl": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
    })
    for trade in trades:
        payload = stats[trade.playbook]
        payload["trades"] += 1
        payload["pnl"] += trade.pnl
        if trade.pnl > 0:
            payload["wins"] += 1
            payload["avg_win"] += trade.pnl
        else:
            payload["losses"] += 1
            payload["avg_loss"] += trade.pnl
    for data in stats.values():
        if data["wins"]:
            data["avg_win"] /= data["wins"]
        if data["losses"]:
            data["avg_loss"] /= data["losses"]
    return stats


def compute_global_metrics(trades: List[Trade]) -> Dict[str, float]:
    if not trades:
        return {"trades": 0, "expectancy": 0.0, "win_rate": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "pnl": 0.0}
    pnl_values = [trade.pnl for trade in trades]
    wins = [p for p in pnl_values if p > 0]
    losses = [p for p in pnl_values if p <= 0]
    return {
        "trades": len(trades),
        "expectancy": sum(pnl_values) / len(trades),
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "avg_win": sum(wins) / len(wins) if wins else 0.0,
        "avg_loss": sum(losses) / len(losses) if losses else 0.0,
        "pnl": sum(pnl_values),
    }


def derive_calibration(global_metrics: Dict[str, float]) -> Dict[str, float]:
    expectancy = global_metrics.get("expectancy", 0.0)
    win_rate = global_metrics.get("win_rate", 0.0)
    risk_multiplier = max(0.5, min(1.5, 1.0 + expectancy))
    pot_adjust = (0.55 - win_rate) * 0.2
    pot_threshold = max(0.45, min(0.65, 0.55 + pot_adjust))
    return {
        "risk_multiplier": round(risk_multiplier, 4),
        "pot_threshold": round(pot_threshold, 4),
    }


def calibrate(
    symbols: List[str],
    data: str | None,
    table: str | None,
    limit: int | None,
    seed: int,
) -> tuple[dict, List[Trade]]:
    runner = build_runner(seed)
    all_trades: List[Trade] = []
    symbol_results: Dict[str, dict] = {}
    for symbol in symbols:
        bars = load_bars(symbol, data_path=data, limit=limit, table=table)
        if not bars:
            continue
        result = runner.replay(symbol.upper(), bars)
        all_trades.extend(result.trades)
        symbol_results[symbol.upper()] = {
            "metrics": compute_global_metrics(result.trades),
            "playbooks": aggregate_playbook_metrics(result.trades),
        }
    global_metrics = compute_global_metrics(all_trades)
    calibration = derive_calibration(global_metrics)
    summary = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "symbols": symbol_results,
        "global": global_metrics,
        "playbooks": aggregate_playbook_metrics(all_trades),
        **calibration,
    }
    return summary, all_trades


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate calibration from backtests")
    parser.add_argument("--symbols", nargs="+", default=["SPY"], help="Symbols to backtest")
    parser.add_argument("--data", help="Optional data file (CSV/DuckDB)", default=None)
    parser.add_argument("--table", help="DuckDB table name", default=None)
    parser.add_argument("--limit", type=int, default=None, help="Max number of bars")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--output", default="backtests/calibration.json", help="Calibration JSON output path")
    parser.add_argument("--trades-output", help="Optional trades JSON dump for auditing", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary, trades = calibrate(args.symbols, args.data, args.table, args.limit, args.seed)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if args.trades_output:
        trades_path = Path(args.trades_output)
        trades_path.parent.mkdir(parents=True, exist_ok=True)
        trades_path.write_text(json.dumps([trade.to_dict() for trade in trades], indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
