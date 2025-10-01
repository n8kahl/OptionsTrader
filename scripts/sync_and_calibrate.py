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

    print(f"Syncing {days} days via REST for {symbols}")
    sync_rest_aggregates(api_key, symbols, dest_dir, days=days, adjusted=True)

    summary, trades = calibrate(symbols, data=str(dest_dir), table=None, limit=None, seed=0)
    output_path = Path("backtests/calibration.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    trades_path = Path("backtests/trades.json")
    trades_path.write_text(json.dumps([t.to_dict() for t in trades], indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
