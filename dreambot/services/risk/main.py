"""Risk service entrypoint."""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Mapping

import yaml
from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import OMS_ORDER_STREAM, RISK_COMMAND_STREAM, RISK_ORDER_STREAM, SIGNAL_STREAM
from ..signals.schemas import SignalIntent
from ..oms.schemas import OrderCommand, OrderRequest, OrderStatus
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
class PendingOrder:
    client_id: str
    request: OrderRequest
    time_stop_secs: int
    order_id: str | None = None
    order_id_event: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_task: asyncio.Task | None = None
    partial_adjusted: bool = False


@dataclass
class RiskService:
    manager: RiskManager
    calendar: EconCalendar
    scheduler: EconScheduler
    account_equity: float = 10_000.0
    pending_orders: Dict[str, "PendingOrder"] = field(default_factory=dict)

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
        client_order_id = f"{signal.underlying}-{signal.ts}"
        metadata["client_order_id"] = client_order_id
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

    async def submit_signal(self, redis: Redis, signal: SignalIntent) -> OrderRequest | None:
        allowed = self.evaluate_entry(signal.ts, minutes_to_open=10, minutes_to_close=60)
        if not allowed:
            return None
        order = self.build_order(signal)
        client_id = self._client_id_from_order(order)
        await publish_json(redis, RISK_ORDER_STREAM, order.to_dict())
        self._register_pending(order, redis, client_id=client_id)
        return order

    async def process_status(self, redis: Redis, status: OrderStatus) -> None:
        client_id = self._client_id_from_status(status)
        if not client_id:
            return
        pending = self.pending_orders.get(client_id)
        if pending:
            if status.order_id and status.order_id != pending.order_id:
                pending.order_id = status.order_id
                pending.order_id_event.set()
        if status.state.lower() in {"filled", "cancelled", "rejected"}:
            self._finalize_pending(client_id)
            return
        if pending and self._should_modify(status, pending):
            command = self._build_partial_fill_command(status, pending)
            await publish_json(redis, RISK_COMMAND_STREAM, command.to_dict())
            pending.partial_adjusted = True

    def _register_pending(self, order: OrderRequest, redis: Redis, *, client_id: str | None = None) -> None:
        if order.time_stop_secs <= 0:
            return
        client_id = client_id or self._client_id_from_order(order)
        if client_id in self.pending_orders:
            return
        pending = PendingOrder(client_id=client_id, request=order, time_stop_secs=order.time_stop_secs)
        pending.cancel_task = asyncio.create_task(self._time_stop_worker(pending, redis))
        self.pending_orders[client_id] = pending

    async def _time_stop_worker(self, pending: "PendingOrder", redis: Redis) -> None:
        try:
            await pending.order_id_event.wait()
            await asyncio.sleep(max(pending.time_stop_secs, 0))
            if pending.client_id not in self.pending_orders:
                return
            command = OrderCommand(
                action="cancel",
                client_order_id=pending.client_id,
                order_id=pending.order_id,
            )
            await publish_json(redis, RISK_COMMAND_STREAM, command.to_dict())
        except asyncio.CancelledError:  # pragma: no cover - task cleanup
            return

    def _finalize_pending(self, client_id: str) -> None:
        pending = self.pending_orders.pop(client_id, None)
        if pending and pending.cancel_task:
            pending.cancel_task.cancel()

    @staticmethod
    def _client_id_from_order(order: OrderRequest) -> str:
        metadata = dict(order.metadata)
        client_id = str(metadata.get("client_order_id", f"{order.underlying}-{order.ts}"))
        if "client_order_id" not in metadata:
            metadata["client_order_id"] = client_id
        order.metadata = metadata
        return client_id

    @staticmethod
    def _client_id_from_status(status: OrderStatus) -> str:
        metadata = status.request.get("metadata", {})
        if isinstance(metadata, Mapping) and metadata.get("client_order_id"):
            return str(metadata["client_order_id"])
        return str(status.order_id)

    @staticmethod
    def _filled_quantity(status: OrderStatus) -> int:
        total = 0
        for fill in status.fills:
            try:
                total += int(float(fill.get("qty", 0)))
            except (ValueError, TypeError):
                continue
        return total

    def _should_modify(self, status: OrderStatus, pending: "PendingOrder") -> bool:
        if pending.partial_adjusted:
            return False
        requested = pending.request.quantity
        filled = self._filled_quantity(status)
        return 0 < filled < requested

    def _build_partial_fill_command(
        self,
        status: OrderStatus,
        pending: "PendingOrder",
    ) -> OrderCommand:
        request = pending.request
        if request.side.upper() == "BUY":
            new_stop = max(min(request.entry_price, request.stop_price) - 0.05, 0.01)
        else:
            new_stop = max(request.stop_price, request.entry_price + 0.05)
        new_target = request.target_price
        pending.request = OrderRequest.from_dict(
            {
                **request.to_dict(),
                "stop_price": new_stop,
                "target_price": new_target,
            }
        )
        return OrderCommand(
            action="modify",
            client_order_id=pending.client_id,
            order_id=pending.order_id,
            stop_price=new_stop,
            target_price=new_target,
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
        await service.submit_signal(redis, signal)

    async def handle_status(payload: Mapping[str, object]) -> None:
        status = OrderStatus.from_dict(dict(payload))
        await service.process_status(redis, status)

    tasks = [
        asyncio.create_task(consume_stream(redis, SIGNAL_STREAM, handle_signal, stop=should_stop)),
        asyncio.create_task(consume_stream(redis, OMS_ORDER_STREAM, handle_status, stop=should_stop)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
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
