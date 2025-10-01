"""Sync last 30 days of data for SPY/QQQ/SPX/NDX and run calibration."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _ensure_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    dreambot_root = root / "dreambot"
    os.chdir(dreambot_root)
    return dreambot_root


def main() -> None:
    _ensure_path()
    symbols = ["SPY", "QQQ", "I:SPX", "I:NDX"]
    days = int(os.environ.get("POLYGON_SYNC_DAYS", 30))
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise SystemExit("POLYGON_API_KEY must be set")

    from dreambot.services.backtest.polygon_rest_sync import sync_rest_aggregates
    from dreambot.services.backtest.calibrate import calibrate
    import json

    dest_dir = Path("data/flatfiles")
    dest_dir.mkdir(parents=True, exist_ok=True)

    print(f"Syncing {days} days via REST for {symbols}", flush=True)
    sync_rest_aggregates(api_key, symbols, dest_dir, days=days, adjusted=True)

    print("Starting calibration run...", flush=True)
    def _parse_list(value: str, default: list[float]) -> list[float]:
        if not value:
            return default
        return [float(v.strip()) for v in value.split(",") if v.strip()]

    pot_grid = _parse_list(os.environ.get("CAL_POT_GRID", ""), [0.52, 0.55, 0.58, 0.6, 0.62])
    adx_grid = _parse_list(os.environ.get("CAL_ADX_GRID", ""), [12, 15, 18, 20, 25])
    min_win = float(os.environ.get("CAL_MIN_WIN_RATE", 0.25))
    min_trades = int(os.environ.get("CAL_MIN_TRADES", 200))
    decision_map = {"I:SPX": "SPY", "I:NDX": "QQQ"}

    summary, trades = calibrate(
        symbols,
        data=str(dest_dir),
        table=None,
        limit=None,
        seed=0,
        optimize=True,
        pot_grid=pot_grid,
        adx_grid=adx_grid,
        min_win_rate=min_win,
        min_trades=min_trades,
        decision_map=decision_map,
    )
    output_path = Path("backtests/calibration.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    trades_path = Path("backtests/trades.json")
    trades_path.write_text(json.dumps([t.to_dict() for t in trades], indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
