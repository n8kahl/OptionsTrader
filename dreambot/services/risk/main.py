"""Risk service entrypoint."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

import yaml

from .econ_calendar import EconCalendar
from .rules import RiskConfig, RiskManager
from .scheduler import EconScheduler


def build_risk_manager(config: Mapping[str, object]) -> RiskManager:
    risk_config = RiskConfig(
        daily_loss_cap=config["daily_loss_cap"],
        per_trade_max_risk_pct=config["per_trade_max_risk_pct"],
        max_concurrent_positions=config["max_concurrent_positions"],
        no_trade_first_seconds=config["no_trade_first_seconds"],
        econ_halt_minutes_pre_post=config["econ_halt_minutes_pre_post"],
        force_flat_before_close_secs=config["force_flat_before_close_secs"],
        defensive_mode=config["defensive_mode"],
    )
    return RiskManager(risk_config)


def load_risk_config() -> Mapping[str, object]:
    return yaml.safe_load(Path("config/risk.yaml").read_text(encoding="utf-8"))


def load_calendar() -> EconCalendar:
    calendar_path = Path("storage/parquet/econ_calendar.json")
    if calendar_path.exists():
        import json
        entries = json.loads(calendar_path.read_text(encoding="utf-8"))
        return EconCalendar.from_dicts(entries)
    return EconCalendar([])


def default_scheduler(config: Mapping[str, object]) -> EconScheduler:
    return EconScheduler([])


@dataclass
class RiskService:
    manager: RiskManager
    calendar: EconCalendar
    scheduler: EconScheduler

    def evaluate_entry(self, now_ts: int, minutes_to_open: int, minutes_to_close: int) -> bool:
        now = datetime.fromtimestamp(now_ts / 1e6, tz=timezone.utc)
        if self.scheduler.is_halted(now):
            return False
        return self.manager.entry_allowed(now_ts, minutes_to_open, minutes_to_close)

    def handle_slippage(self, slippage_z: float, spread_z: float) -> None:
        self.manager.update_defensive(slippage_z, spread_z)


async def main_async() -> None:
    config = load_risk_config()
    service = RiskService(
        manager=build_risk_manager(config),
        calendar=load_calendar(),
        scheduler=default_scheduler(config),
    )
    while True:
        await asyncio.sleep(5)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
