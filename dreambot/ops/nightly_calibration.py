"""Nightly calibration pipeline: sync Polygon flat files and update calibration JSON."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from services.backtest.calibrate import calibrate
from services.backtest.polygon_sync import sync_flatfiles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run nightly Polygon sync + calibration")
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ", "SPX", "NDX"], help="Symbols to process")
    parser.add_argument("--days", type=int, default=60, help="Trailing days of flat files to sync")
    parser.add_argument("--flatfile-prefix", default="us_stocks_sip/minute_aggs_v1", help="Polygon S3 prefix")
    parser.add_argument("--flatfile-dest", default="data/flatfiles", help="Local destination for flat files")
    parser.add_argument("--calibration-output", default="backtests/calibration.json", help="Calibration JSON path")
    parser.add_argument("--trades-output", default="backtests/trades.json", help="Optional trades log")
    parser.add_argument("--limit", type=int, default=None, help="Optional bar limit per symbol during calibration")
    parser.add_argument("--seed", type=int, default=0, help="Random seed for replay determinism")
    parser.add_argument("--sync-method", choices=["s3", "rest", "none"], default="rest", help="Data sync strategy")
    parser.add_argument("--adjusted", action="store_true", help="Use adjusted prices when syncing via REST")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dest_dir = Path(args.flatfile_dest)
    dest_dir.mkdir(parents=True, exist_ok=True)
    api_key = os.environ.get("POLYGON_API_KEY")

    if args.sync_method == "s3":
        if not (
            os.environ.get("POLYGON_S3_ACCESS_KEY")
            and os.environ.get("POLYGON_S3_SECRET_KEY")
        ) and not api_key:
            raise SystemExit(
                "Set POLYGON_S3_ACCESS_KEY/POLYGON_S3_SECRET_KEY or POLYGON_API_KEY for S3 sync"
            )
        sync_flatfiles(
            api_key,
            args.symbols,
            dest_dir,
            days=args.days,
            prefix=args.flatfile_prefix.strip("/"),
        )
    elif args.sync_method == "rest":
        if not api_key:
            raise SystemExit("POLYGON_API_KEY is required for REST sync")
        from services.backtest.polygon_rest_sync import sync_rest_aggregates

        sync_rest_aggregates(
            api_key,
            args.symbols,
            dest_dir,
            days=args.days,
            adjusted=args.adjusted,
        )
    else:
        print("Skipping data sync (sync method set to none)")

    summary, trades = calibrate(
        args.symbols,
        data=str(dest_dir),
        table=None,
        limit=args.limit,
        seed=args.seed,
    )

    output_path = Path(args.calibration_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if args.trades_output:
        trades_path = Path(args.trades_output)
        trades_path.parent.mkdir(parents=True, exist_ok=True)
        trades_payload = [trade.to_dict() for trade in trades]
        trades_path.write_text(json.dumps(trades_payload, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
