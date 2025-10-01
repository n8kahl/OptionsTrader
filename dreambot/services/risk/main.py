"""Risk service entrypoint."""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

import yaml
from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import RISK_ORDER_STREAM, SIGNAL_STREAM
from ..signals.schemas import SignalIntent
from ..oms.schemas import OrderRequest
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


def default_scheduler() -> EconScheduler:
    return EconScheduler([])


@dataclass
class RiskService:
    manager: RiskManager
    calendar: EconCalendar
    scheduler: EconScheduler
    account_equity: float = 10_000.0

    def evaluate_entry(self, signal_ts: int, minutes_to_open: int, minutes_to_close: int) -> bool:
        now = datetime.fromtimestamp(signal_ts / 1_000_000, tz=timezone.utc)
        if self.scheduler.is_halted(now):
            return False
        return self.manager.entry_allowed(signal_ts, minutes_to_open, minutes_to_close)

    def build_order(self, signal: SignalIntent) -> OrderRequest:
        quantity = 1  # TODO: integrate learner sizing and risk budget
        entry_price = 1.0
        target_price = entry_price + max(signal.target_underlying_move, 0.1)
        stop_price = max(entry_price + signal.stop_underlying_move, 0.1)
        metadata = {
            "playbook": signal.playbook,
            "filters": signal.option_filters,
        }
        option_symbol = f"{signal.underlying}_AUTO"
        return OrderRequest(
            ts=signal.ts,
            underlying=signal.underlying,
            option_symbol=option_symbol,
            side=signal.side,
            quantity=quantity,
            entry_price=entry_price,
            target_price=target_price,
            stop_price=stop_price,
            time_stop_secs=signal.time_stop_secs,
            metadata=metadata,
        )


async def run_risk_stream(
    service: RiskService,
    redis: Redis,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    def should_stop() -> bool:
        return stop_event.is_set() if stop_event else False

    async def handle_signal(payload: Mapping[str, object]) -> None:
        signal = SignalIntent.from_dict(dict(payload))
        allowed = service.evaluate_entry(signal.ts, minutes_to_open=10, minutes_to_close=60)
        if not allowed:
            return
        order = service.build_order(signal)
        await publish_json(redis, RISK_ORDER_STREAM, order.to_dict())

    task = asyncio.create_task(
        consume_stream(redis, SIGNAL_STREAM, handle_signal, stop=should_stop)
    )
    try:
        await task
    except asyncio.CancelledError:
        task.cancel()
        raise


async def main_async() -> None:
    config = load_risk_config()
    service = RiskService(
        manager=build_risk_manager(config),
        calendar=load_calendar(),
        scheduler=default_scheduler(),
        account_equity=float(os.environ.get("ACCOUNT_EQUITY", 10_000)),
    )
    redis = await create_redis()
    try:
        await run_risk_stream(service, redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
