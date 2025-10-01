from datetime import date

from services.backtest.polygon_sync import generate_dates


def test_generate_dates_order():
    today = date(2024, 1, 10)
    dates = generate_dates(3, end=today)
    assert dates == ["2024-01-10", "2024-01-09", "2024-01-08"]
