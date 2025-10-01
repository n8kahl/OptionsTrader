"""DuckDB I/O helpers for backtesting."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import duckdb
import pandas as pd


@dataclass
class DuckDBConfig:
    data_dir: Path


def load_flat_files(paths: Iterable[Path]) -> pd.DataFrame:
    files = [str(path) for path in paths]
    if not files:
        return pd.DataFrame()
    query = " UNION ALL ".join(f"SELECT * FROM read_parquet('{file}')" for file in files)
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(query).fetchdf()
    finally:
        con.close()


def load_range(config: DuckDBConfig, pattern: str) -> pd.DataFrame:
    matches = list(config.data_dir.glob(pattern))
    return load_flat_files(matches)
