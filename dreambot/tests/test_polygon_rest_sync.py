from datetime import date
from pathlib import Path

import pytest

from services.backtest.polygon_rest_sync import daterange, sync_rest_aggregates


def test_daterange_order():
    days = daterange(3, end=date(2024, 1, 5))
    assert days == [date(2024, 1, 5), date(2024, 1, 4), date(2024, 1, 3)]


def test_sync_rest(monkeypatch, tmp_path):
    calls = []

    def fake_fetch_day(symbol, day, api_key, adjusted=True):
        calls.append((symbol, day))
        return [
            {"t": 1700000000000, "o": 1.0, "h": 1.1, "l": 0.9, "c": 1.05, "v": 1000}
        ]

    monkeypatch.setattr("services.backtest.polygon_rest_sync.fetch_day", fake_fetch_day)
    paths = sync_rest_aggregates("KEY", ["SPY"], tmp_path, days=1)
    assert len(paths) == 1
    csv = paths[0]
    assert csv.exists()
    content = csv.read_text(encoding="utf-8").strip().splitlines()
    assert content[0] == "ts,o,h,l,c,v"
    assert content[1].startswith("1700000000000000,1.0")
    assert calls
