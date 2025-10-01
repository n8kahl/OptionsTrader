from pathlib import Path

from services.learner.calibration_io import apply_calibration, load_calibration, save_calibration


def test_calibration_round_trip(tmp_path):
    path = tmp_path / "calibration.json"
    payload = {"pot_offset_trend": 0.05, "spread_z_threshold": 1.1}
    save_calibration(path, payload)
    loaded = load_calibration(path)
    target = {"pot_offset_trend": 0.0}
    apply_calibration(target, loaded)
    assert target["pot_offset_trend"] == 0.05
    assert target["spread_z_threshold"] == 1.1
