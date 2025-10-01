"""Data loading utilities for backtests."""
from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Iterable, List, Sequence

try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None

from ..ingest.schemas import Agg1s


def _candidate_paths(symbol: str, data_path: str | None) -> Sequence[Path]:
    if data_path:
        return [Path(data_path)]
    slug = symbol.lower()
    candidates = [
        Path(f"data/backtests/{slug}.csv"),
        Path(f"data/backtests/{slug}_1m.csv"),
        Path(f"data/{slug}.csv"),
        Path(f"data/{slug}_1m.csv"),
        Path(f"data/{symbol.upper()}_1m.csv"),
    ]
    return candidates


def _parse_csv(path: Path, symbol: str, limit: int | None = None) -> List[Agg1s]:
    bars: List[Agg1s] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                ts = int(row["ts"])
                o = float(row["o"])
                h = float(row["h"])
                l = float(row["l"])
                c = float(row["c"])
                v = float(row.get("v", 0.0))
            except (KeyError, ValueError) as exc:
                raise ValueError(f"Malformed row in {path}: {row}") from exc
            bars.append(Agg1s(ts=ts, symbol=symbol.upper(), o=o, h=h, l=l, c=c, v=v))
            if limit and len(bars) >= limit:
                break
    return bars


def _load_duckdb(path: Path, symbol: str, limit: int | None, table: str | None) -> List[Agg1s]:
    if duckdb is None:
        raise RuntimeError("duckdb package not installed")
    table_name = table or "bars"
    conn = duckdb.connect(str(path))
    try:
        query = f"SELECT ts, o, h, l, c, COALESCE(v, 0) as v FROM {table_name} WHERE symbol = ? ORDER BY ts"
        if limit:
            query += f" LIMIT {int(limit)}"
        rows = conn.execute(query, [symbol.upper()]).fetchall()
    finally:
        conn.close()
    bars: List[Agg1s] = []
    for row in rows:
        ts, o, h, l, c, v = row
        bars.append(Agg1s(ts=int(ts), symbol=symbol.upper(), o=float(o), h=float(h), l=float(l), c=float(c), v=float(v)))
    return bars


def load_bars(
    symbol: str,
    data_path: str | None = None,
    limit: int | None = None,
    *,
    table: str | None = None,
) -> List[Agg1s]:
    """Load aggregated bars for the given symbol, falling back to synthetic data."""
    for candidate in _candidate_paths(symbol, data_path):
        if candidate.exists():
            if candidate.is_dir():
                bars: List[Agg1s] = []
                search_dirs = [candidate]
                upper = candidate / symbol.upper()
                lower = candidate / symbol.lower()
                if upper.exists():
                    search_dirs.append(upper)
                elif lower.exists():
                    search_dirs.append(lower)
                for directory in search_dirs:
                    files = sorted(directory.glob("*.csv"))
                    for file in files:
                        remaining = None if limit is None else max(limit - len(bars), 0)
                        if remaining == 0:
                            break
                        per_file = None if remaining is None or remaining == 0 else remaining
                        bars.extend(_parse_csv(file, symbol, per_file))
                    if limit and len(bars) >= limit:
                        return bars[:limit]
                return bars
            if candidate.suffix.lower() in {".duckdb", ".db"}:
                return _load_duckdb(candidate, symbol, limit, table)
            return _parse_csv(candidate, symbol, limit)
    return generate_synthetic_bars(symbol.upper(), count=limit or 500)


def generate_synthetic_bars(symbol: str, *, count: int = 500) -> List[Agg1s]:
    base_price = 400.0
    bars: List[Agg1s] = []
    for idx in range(count):
        ts = 1700000000000000 + idx * 60 * 1_000_000
        drift = math.sin(idx / 25) * 1.5
        price = base_price + idx * 0.05 + drift
        o = price - 0.1
        c = price + 0.1
        h = max(o, c) + 0.3
        l = min(o, c) - 0.3
        v = 10_000 + idx * 50
        bars.append(Agg1s(ts=ts, symbol=symbol, o=o, h=h, l=l, c=c, v=v))
    return bars
