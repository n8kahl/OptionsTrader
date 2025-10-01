"""Backtest calibration CLI with per-symbol optimization."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence

from ..features.main import FeatureEngine, load_feature_config
from ..signals.main import SignalEngine, load_gate_config
from ..learner.main import LearnerService
from .data_loader import load_bars
from .fill_model import FillModel
from .metrics import Trade
from .replay import BacktestConfig, BacktestRunner, BacktestResult

# Base configuration cache
_FEATURE_CONFIG = load_feature_config()
_BASE_GATE_CONFIG = load_gate_config()
_BASE_CONFIG = BacktestConfig(
    risk={
        "daily_loss_cap": -500,
        "per_trade_max_risk_pct": 0.7,
        "max_concurrent_positions": 2,
        "no_trade_first_seconds": 90,
        "econ_halt_minutes_pre_post": 3,
        "force_flat_before_close_secs": 180,
        "defensive_mode": {"slippage_z": 2.0, "spread_z": 2.0},
    },
    gate=_BASE_GATE_CONFIG,
    oms={
        "paper": True,
        "order_type": "marketable_limit",
        "use_otoco": True,
        "default_limit_offset_ticks": 1,
        "modify_stop_on_underlying": True,
    },
)


def aggregate_playbook_metrics(trades: Iterable[Trade]) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "avg_win": 0.0, "avg_loss": 0.0}
    )
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


def compute_metrics(trades: Sequence[Trade]) -> Dict[str, float]:
    if not trades:
        return {
            "trades": 0,
            "expectancy": 0.0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "pnl": 0.0,
        }
    pnl_values = [trade.pnl for trade in trades]
    wins = [p for p in pnl_values if p > 0]
    losses = [p for p in pnl_values if p <= 0]
    return {
        "trades": len(trades),
        "expectancy": sum(pnl_values) / len(trades),
        "win_rate": len(wins) / len(trades),
        "avg_win": sum(wins) / len(wins) if wins else 0.0,
        "avg_loss": sum(losses) / len(losses) if losses else 0.0,
        "pnl": sum(pnl_values),
    }


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def derive_risk_multiplier(expectancy: float) -> float:
    return round(clamp(1.0 + expectancy, 0.5, 1.5), 4)


def create_runner(pot_threshold: float, adx_threshold: float, seed: int) -> BacktestRunner:
    gate_config = dict(_BASE_GATE_CONFIG)
    if pot_threshold is not None:
        gate_config["pot_threshold"] = pot_threshold
    if adx_threshold is not None:
        gate_config["adx_threshold"] = adx_threshold
    return BacktestRunner(
        feature_engine=FeatureEngine(_FEATURE_CONFIG),
        signal_engine=SignalEngine(gate_config),
        learner=LearnerService(Path("backtests/calibration.json")),
        fill_model=FillModel(),
        config=_BASE_CONFIG,
        seed=seed,
    )


def run_backtest(
    symbol: str,
    bars: Sequence,
    *,
    decision_symbol: str | None = None,
    decision_bars: Sequence | None = None,
    pot_threshold: float,
    adx_threshold: float,
    seed: int,
) -> BacktestResult:
    runner = create_runner(pot_threshold, adx_threshold, seed)
    return runner.replay(
        symbol.upper(),
        bars,
        decision_symbol=decision_symbol.upper() if decision_symbol else None,
        decision_bars=decision_bars,
    )


def optimize_symbol(
    symbol: str,
    bars: Sequence,
    *,
    decision_symbol: str,
    decision_bars: Sequence,
    pot_grid: Sequence[float],
    adx_grid: Sequence[float],
    min_win_rate: float,
    min_trades: int,
    seed: int,
) -> tuple[Dict[str, float], List[Trade], Dict[str, float]]:
    best_metrics: Dict[str, float] | None = None
    best_trades: List[Trade] = []
    best_params: Dict[str, float] = {}

    for pot, adx in product(pot_grid, adx_grid):
        result = run_backtest(
            symbol,
            bars,
            decision_symbol=decision_symbol,
            decision_bars=decision_bars,
            pot_threshold=pot,
            adx_threshold=adx,
            seed=seed,
        )
        metrics = compute_metrics(result.trades)
        if metrics["trades"] < min_trades or metrics["win_rate"] < min_win_rate:
            continue
        if best_metrics is None or metrics["expectancy"] > best_metrics["expectancy"]:
            best_metrics = metrics
            best_trades = list(result.trades)
            best_params = {
                "pot_threshold": round(pot, 4),
                "adx_threshold": round(adx, 2),
                "risk_multiplier": derive_risk_multiplier(metrics["expectancy"]),
            }
    if best_metrics is None:
        # fallback: use default config even if constraints fail
        default_pot = pot_grid[0]
        default_adx = adx_grid[0]
        result = run_backtest(
            symbol,
            bars,
            decision_symbol=decision_symbol,
            decision_bars=decision_bars,
            pot_threshold=default_pot,
            adx_threshold=default_adx,
            seed=seed,
        )
        best_trades = list(result.trades)
        best_metrics = compute_metrics(result.trades)
        best_params = {
            "pot_threshold": round(default_pot, 4),
            "adx_threshold": round(default_adx, 2),
            "risk_multiplier": derive_risk_multiplier(best_metrics["expectancy"]),
        }
    return best_metrics, best_trades, best_params


def parse_float_list(values: str, default: Sequence[float]) -> List[float]:
    if not values:
        return list(default)
    return [float(v.strip()) for v in values.split(",") if v.strip()]


def parse_decision_map(raw: str | None) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not raw:
        return mapping
    pairs = [p.strip() for p in raw.split(",") if p.strip()]
    for pair in pairs:
        if "=" not in pair:
            continue
        target, source = pair.split("=", 1)
        mapping[target.strip().upper()] = source.strip().upper()
    return mapping


def calibrate(
    symbols: List[str],
    data: str | None,
    table: str | None,
    limit: int | None,
    seed: int,
    *,
    optimize: bool = False,
    pot_grid: Sequence[float] | None = None,
    adx_grid: Sequence[float] | None = None,
    min_win_rate: float = 0.2,
    min_trades: int = 50,
    decision_map: Mapping[str, str] | None = None,
) -> tuple[dict, List[Trade]]:
    decision_map = {k.upper(): v.upper() for k, v in (decision_map or {}).items()}
    pot_grid = list(pot_grid or [0.52, 0.55, 0.58, 0.6, 0.62])
    adx_grid = list(adx_grid or [12, 15, 18, 20, 25])

    symbol_results: Dict[str, dict] = {}
    all_trades: List[Trade] = []

    bars_cache: Dict[str, Sequence] = {}
    for sym in set(symbols) | set(decision_map.values()):
        bars_cache[sym.upper()] = load_bars(sym, data_path=data, limit=limit, table=table)

    total = len(symbols)
    for idx, symbol in enumerate(symbols, start=1):
        sym_key = symbol.upper()
        bars = bars_cache.get(sym_key, [])
        if not bars:
            print(f"[{idx}/{total}] No bars found for {sym_key}, skipping", flush=True)
            continue
        decision_symbol = decision_map.get(sym_key, sym_key)
        decision_bars = bars_cache.get(decision_symbol) or bars
        if optimize:
            metrics, trades, params = optimize_symbol(
                sym_key,
                bars,
                decision_symbol=decision_symbol,
                decision_bars=decision_bars,
                pot_grid=pot_grid,
                adx_grid=adx_grid,
                min_win_rate=min_win_rate,
                min_trades=min_trades,
                seed=seed,
            )
        else:
            default_pot = pot_grid[0]
            default_adx = adx_grid[0]
            result = run_backtest(
                sym_key,
                bars,
                decision_symbol=decision_symbol,
                decision_bars=decision_bars,
                pot_threshold=default_pot,
                adx_threshold=default_adx,
                seed=seed,
            )
            trades = list(result.trades)
            metrics = compute_metrics(trades)
            params = {
                "pot_threshold": round(default_pot, 4),
                "adx_threshold": round(default_adx, 2),
                "risk_multiplier": derive_risk_multiplier(metrics["expectancy"]),
            }
        all_trades.extend(trades)
        symbol_results[sym_key] = {
            "metrics": metrics,
            "playbooks": aggregate_playbook_metrics(trades),
            "params": {
                **params,
                "decision_symbol": decision_symbol,
            },
        }
        print(
            f"[{idx}/{total}] {sym_key} -> trades={metrics['trades']} expectancy={metrics['expectancy']:.4f} win_rate={metrics['win_rate']:.2%}",
            flush=True,
        )

    global_metrics = compute_metrics(all_trades)
    global_params = {
        "pot_threshold": pot_grid[0],
        "adx_threshold": adx_grid[0],
        "risk_multiplier": derive_risk_multiplier(global_metrics["expectancy"]),
    }
    global_params = {k: (round(v, 4) if isinstance(v, float) else v) for k, v in global_params.items()}
    summary = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "symbols": symbol_results,
        "global": global_metrics,
        "playbooks": aggregate_playbook_metrics(all_trades),
        "global_params": global_params,
        "risk_multiplier": global_params["risk_multiplier"],
        "pot_threshold": global_params["pot_threshold"],
        "adx_threshold": global_params["adx_threshold"],
    }
    return summary, all_trades


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate calibration from backtests")
    parser.add_argument("--symbols", nargs="+", default=["SPY"], help="Symbols to backtest")
    parser.add_argument("--data", help="Optional data source (directory, CSV, DuckDB)", default=None)
    parser.add_argument("--table", help="DuckDB table name", default=None)
    parser.add_argument("--limit", type=int, default=None, help="Max number of bars")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--output", default="backtests/calibration.json", help="Calibration JSON output path")
    parser.add_argument("--trades-output", help="Optional trades JSON dump for auditing", default=None)
    parser.add_argument("--optimize", action="store_true", help="Run grid search for thresholds")
    parser.add_argument("--pot-grid", default="", help="Comma-separated pot thresholds (e.g., 0.52,0.55,0.58)")
    parser.add_argument("--adx-grid", default="", help="Comma-separated ADX thresholds (e.g., 12,15,20)")
    parser.add_argument("--min-win-rate", type=float, default=0.2, help="Minimum acceptable win rate")
    parser.add_argument("--min-trades", type=int, default=50, help="Minimum trades required per symbol")
    parser.add_argument(
        "--decision-map",
        default="",
        help="Optional mapping target=source (e.g., I:SPX=SPY,I:NDX=QQQ)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary, trades = calibrate(
        args.symbols,
        args.data,
        args.table,
        args.limit,
        args.seed,
        optimize=args.optimize,
        pot_grid=parse_float_list(args.pot_grid, [0.52, 0.55, 0.58, 0.6, 0.62]),
        adx_grid=parse_float_list(args.adx_grid, [12, 15, 18, 20, 25]),
        min_win_rate=args.min_win_rate,
        min_trades=args.min_trades,
        decision_map=parse_decision_map(args.decision_map),
    )
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
