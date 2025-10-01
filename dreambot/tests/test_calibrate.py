import json
from pathlib import Path

from services.backtest.calibrate import calibrate


def test_calibrate_produces_metrics(tmp_path):
    summary, trades = calibrate(["SPY"], data=None, table=None, limit=50, seed=0)
    assert "global" in summary
    assert "risk_multiplier" in summary
    assert trades, "expected trades from calibration"
    output = tmp_path / "calibration.json"
    output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    assert output.exists()
