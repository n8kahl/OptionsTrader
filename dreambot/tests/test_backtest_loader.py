from pathlib import Path

import pytest

from services.backtest.data_loader import generate_synthetic_bars, load_bars


def test_load_bars_from_csv(tmp_path: Path):
    csv_path = tmp_path / "spy.csv"
    csv_path.write_text("ts,o,h,l,c,v\n1,1,2,0,1.5,100\n2,1.6,2.2,1.2,1.8,120\n", encoding="utf-8")
    bars = load_bars("SPY", data_path=str(csv_path))
    assert len(bars) == 2
    assert bars[0].symbol == "SPY"


def test_generate_synthetic_default_count():
    bars = generate_synthetic_bars("SPY", count=10)
    assert len(bars) == 10
    assert all(bar.symbol == "SPY" for bar in bars)


def test_load_bars_from_duckdb(tmp_path: Path):
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "bars.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE bars (ts BIGINT, symbol VARCHAR, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE)")
    conn.execute(
        "INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, "SPY", 1.0, 1.2, 0.9, 1.1, 100.0),
    )
    conn.execute(
        "INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, ?)",
        (2, "SPY", 1.1, 1.3, 1.0, 1.2, 120.0),
    )
    conn.close()
    bars = load_bars("SPY", data_path=str(path), table="bars")
    assert len(bars) == 2
    assert bars[0].symbol == "SPY"


def test_load_bars_from_directory(tmp_path: Path):
    data_dir = tmp_path / "csvs"
    data_dir.mkdir()
    (data_dir / "part1.csv").write_text("ts,o,h,l,c,v\n1,1,2,0,1,100\n", encoding="utf-8")
    spy_dir = data_dir / "SPY"
    spy_dir.mkdir()
    (spy_dir / "part2.csv").write_text("ts,o,h,l,c,v\n2,1,2,0,1,120\n", encoding="utf-8")
    bars = load_bars("SPY", data_path=str(data_dir))
    assert len(bars) == 2
