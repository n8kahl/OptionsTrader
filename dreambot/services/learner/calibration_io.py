"""Calibration persistence."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, MutableMapping


def load_calibration(path: Path) -> Mapping[str, float]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_calibration(path: Path, payload: Mapping[str, float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def apply_calibration(target: MutableMapping[str, float], calibration: Mapping[str, float]) -> None:
    target.update(calibration)
