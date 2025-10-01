"""Fetch Polygon minute aggregates via REST API into local CSV files."""
from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List

import requests

DEFAULT_RANGE = 60


def daterange(days: int, *, end: date | None = None) -> List[date]:
    end_date = end or date.today()
    return [end_date - timedelta(days=offset) for offset in range(days)]


def fetch_day(symbol: str, day: date, api_key: str, adjusted: bool = True) -> List[dict]:
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol.upper()}/range/1/minute/{day}/{day}"
    params = {
        "adjusted": "true" if adjusted else "false",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }
    response = requests.get(url, params=params, timeout=30)
    if response.status_code == 429:
        time.sleep(1)
        response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    return payload.get("results", [])


def write_day(symbol: str, day: date, results: List[dict], dest_dir: Path) -> Path:
    symbol_dir = dest_dir / symbol.upper()
    symbol_dir.mkdir(parents=True, exist_ok=True)
    path = symbol_dir / f"{day.isoformat()}.csv"
    if not results:
        return path
    with path.open("w", encoding="utf-8") as handle:
        handle.write("ts,o,h,l,c,v\n")
        for item in results:
            ts_micro = int(item["t"]) * 1000
            o = item.get("o", 0.0)
            h = item.get("h", o)
            l = item.get("l", o)
            c = item.get("c", o)
            v = item.get("v", 0.0)
            handle.write(f"{ts_micro},{o},{h},{l},{c},{v}\n")
    return path


def sync_rest_aggregates(
    api_key: str,
    symbols: Iterable[str],
    dest_dir: Path,
    *,
    days: int = DEFAULT_RANGE,
    adjusted: bool = True,
) -> List[Path]:
    downloaded: List[Path] = []
    for day in daterange(days):
        for symbol in symbols:
            try:
                results = fetch_day(symbol, day, api_key, adjusted=adjusted)
                path = write_day(symbol, day, results, dest_dir)
                if results:
                    downloaded.append(path)
                    print(f"Saved {symbol} {day} -> {path}")
            except Exception as exc:  # pragma: no cover - network calls
                print(f"Warning: failed to fetch {symbol} {day}: {exc}")
                continue
    return downloaded
