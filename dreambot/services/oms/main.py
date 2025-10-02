"""OMS service entrypoint."""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml
from redis.asyncio import Redis

from ..common.redis import close_redis, consume_stream, create_redis, publish_json
from ..common.streams import (
    OMS_METRIC_STREAM,
    OMS_ORDER_STREAM,
    RISK_COMMAND_STREAM,
    RISK_ORDER_STREAM,
)
from .audit import OrderAuditRecorder
from .order_templates import OTOCOOrder, build_otoco
from .schemas import OrderCommand, OrderRequest, OrderStatus
from .stop_sync import StopSyncConfig, adjust_stop
from .tradier_api import InMemoryBroker, TradierClient, TradierConfig


def load_broker_config() -> Mapping[str, object]:
    cfg_path = Path("config/broker.yaml")
    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    tradier_cfg = data.pop("tradier", {}) or {}
    for key, value in tradier_cfg.items():
        data[f"tradier_{key}"] = value
    return data


@dataclass
class OMSConfig:
    paper: bool
    order_type: str
    use_otoco: bool
    default_limit_offset_ticks: float
    modify_stop_on_underlying: bool
    provider: str = "in_memory"
    tradier_duration: str = "day"
    tradier_access_token_env: str = "TRADIER_ACCESS_TOKEN"
    tradier_account_id_env: str = "TRADIER_ACCOUNT_ID"
    tradier_live_base_url: str = "https://api.tradier.com/v1"
    tradier_paper_base_url: str = "https://sandbox.tradier.com/v1"
    tradier_max_retries: int = 5
    tradier_retry_backoff_secs: float = 1.0
    tradier_request_timeout_secs: float = 5.0
    tradier_poll_interval_secs: float = 1.0
    tradier_status_timeout_secs: float = 60.0


class OMSService:
    def __init__(self, config: OMSConfig, broker: Optional[object] = None):
        self.config = config
        self.broker = broker or self._init_broker()
        self.stop_config = StopSyncConfig(modify_on_tick=config.modify_stop_on_underlying)
        self._order_requests: Dict[str, OrderRequest] = {}
        self._client_to_order_id: Dict[str, str] = {}
        self._active_pollers: set[asyncio.Task] = set()
        audit_path = os.environ.get("OMS_AUDIT_PATH")
        rotate_mb = float(os.environ.get("OMS_AUDIT_ROTATE_MB", "0"))
        rotate_bytes = int(max(rotate_mb, 0.0) * 1_048_576)
        self._audit_recorder = (
            OrderAuditRecorder(audit_path, rotate_bytes) if audit_path else None
        )
        self.metrics_stream = os.environ.get("OMS_METRIC_STREAM", OMS_METRIC_STREAM)

    async def route_order(self, request: OrderRequest) -> OrderStatus:
        if not self.config.use_otoco:
            raise NotImplementedError("DreamBot only routes OTOCO orders per spec")
        client_id = self._client_id_from_request(request)
        self._order_requests[client_id] = request
        order = build_otoco(
            symbol=request.option_symbol,
            quantity=request.quantity,
            side=request.side,
            entry_price=request.entry_price,
            target_price=request.target_price,
            stop_price=request.stop_price,
            offset_ticks=self.config.default_limit_offset_ticks,
        )
        response = await self._place_order(order, request)
        status = self._status_from_response(response, request)
        self._register_status(status)
        return status

    def sync_stop(self, existing_stop: float, underlying_price: float, direction: str) -> float:
        return adjust_stop(existing_stop, underlying_price, direction, self.stop_config)

    def _init_broker(self) -> object:
        provider = self.config.provider.lower()
        if provider == "tradier":
            token_env = self.config.tradier_access_token_env
            account_env = self.config.tradier_account_id_env
            token = os.environ.get(token_env)
            account_id = os.environ.get(account_env)
            if not token or not account_id:
                raise RuntimeError(
                    "Tradier credentials missing. Set env variables "
                    f"{token_env} and {account_env}."
                )
            base_url = (
                self.config.tradier_paper_base_url
                if self.config.paper
                else self.config.tradier_live_base_url
            )
            cfg = TradierConfig(
                token=token,
                account_id=account_id,
                base_url=base_url,
                request_timeout=self.config.tradier_request_timeout_secs,
                max_retries=self.config.tradier_max_retries,
                retry_backoff_secs=self.config.tradier_retry_backoff_secs,
                poll_interval_secs=self.config.tradier_poll_interval_secs,
                status_timeout_secs=self.config.tradier_status_timeout_secs,
            )
            return TradierClient(cfg)
        return InMemoryBroker()

    def _client_id_from_request(self, request: OrderRequest) -> str:
        metadata = dict(request.metadata or {})
        client_id = str(metadata.get("client_order_id", f"dreambot-{request.ts}"))
        if metadata.get("client_order_id") != client_id:
            metadata["client_order_id"] = client_id
            request.metadata = metadata
        return client_id

    @staticmethod
    def _client_id_from_status(status: OrderStatus) -> str:
        metadata = status.request.get("metadata", {})
        if isinstance(metadata, Mapping) and metadata.get("client_order_id"):
            return str(metadata["client_order_id"])
        return str(status.order_id)

    async def _place_order(self, order: OTOCOOrder, request: OrderRequest) -> Mapping[str, object]:
        if self.config.provider.lower() == "tradier":
            payload = self._build_tradier_payload(order, request)
            return await self.broker.place_order(payload)
        return await self.broker.place_order(order.to_payload())

    def _build_tradier_payload(self, order: OTOCOOrder, request: OrderRequest) -> Mapping[str, object]:
        side = request.side.upper()
        if side == "BUY":
            entry_side = "buy_to_open"
            closing_side = "sell_to_close"
        else:
            entry_side = "sell_to_open"
            closing_side = "buy_to_close"
        payload = order.to_tradier_payload(
            option_symbol=request.option_symbol,
            entry_side=entry_side,
            closing_side=closing_side,
            quantity=request.quantity,
            duration=self.config.tradier_duration,
        )
        metadata = dict(request.metadata or {})
        tag = metadata.get("tag") or metadata.get("playbook")
        if tag:
            payload["tag"] = str(tag)
        client_order_id = metadata.get("client_order_id") or f"dreambot-{request.ts}"
        payload.setdefault("client_order_id", str(client_order_id))
        return payload

    def _status_from_response(self, response: Mapping[str, Any], request: OrderRequest) -> OrderStatus:
        status_ts = int(time.time() * 1_000_000)
        order_id, state, fills, broker_payload = self._extract_status_fields(response)
        status = OrderStatus(
            ts=status_ts,
            order_id=order_id,
            state=state,
            request=request.to_dict(),
            broker_payload=broker_payload,
            fills=fills,
        )
        return status

    def _register_status(self, status: OrderStatus) -> None:
        client_id = self._client_id_from_status(status)
        self._client_to_order_id[client_id] = status.order_id
        if client_id not in self._order_requests:
            self._order_requests[client_id] = OrderRequest.from_dict(status.request)

    def _order_request_for(self, client_id: str) -> OrderRequest | None:
        return self._order_requests.get(client_id)

    @staticmethod
    def _filled_quantity(status: OrderStatus) -> int:
        total = 0
        for fill in status.fills:
            try:
                total += int(float(fill.get("qty", 0)))
            except (TypeError, ValueError):
                continue
        return total

    def _metrics_payload(self, status: OrderStatus) -> Dict[str, object]:
        request = status.request
        metadata = request.get("metadata") if isinstance(request.get("metadata"), Mapping) else {}
        client_id = metadata.get("client_order_id") if isinstance(metadata, Mapping) else None
        request_ts = int(request.get("ts", status.ts))
        latency_ms = max((status.ts - request_ts) / 1_000.0, 0.0)
        quantity = int(request.get("quantity", 0))
        filled = self._filled_quantity(status)
        avg_fill = None
        total_price = 0.0
        for fill in status.fills:
            try:
                price = float(fill.get("price", 0.0))
                qty = float(fill.get("qty", 0.0))
            except (TypeError, ValueError):
                continue
            total_price += price * qty
        if filled > 0 and total_price > 0:
            avg_fill = total_price / filled
        return {
            "ts": status.ts,
            "order_id": status.order_id,
            "client_order_id": client_id,
            "state": status.state.lower(),
            "side": request.get("side"),
            "quantity": quantity,
            "filled_qty": filled,
            "latency_ms": latency_ms,
            "avg_fill_price": avg_fill,
        }

    async def record_status(self, status: OrderStatus, redis: Optional[Redis]) -> None:
        if self._audit_recorder is not None:
            await self._audit_recorder.write(status)
        if redis is None:
            return
        payload = self._metrics_payload(status)
        await publish_json(redis, self.metrics_stream, payload)

    async def handle_command(self, command: OrderCommand, redis: Redis) -> Optional[OrderStatus]:
        client_id = command.client_order_id or ""
        if not client_id and command.order_id:
            client_id = next((cid for cid, oid in self._client_to_order_id.items() if oid == command.order_id), "")
        order_id = command.order_id or self._client_to_order_id.get(client_id, "")
        if not order_id:
            return None
        request = self._order_request_for(client_id)
        if request is None:
            return None

        action = command.action.upper()
        if action == "CANCEL":
            response = await self.broker.cancel_order(order_id)
            status = self._status_from_response(response, request)
            self._register_status(status)
            return status
        if action == "MODIFY":
            if command.stop_price is not None:
                request.stop_price = command.stop_price
            if command.target_price is not None:
                request.target_price = command.target_price
            self._order_requests[client_id] = request
            order = build_otoco(
                symbol=request.option_symbol,
                quantity=request.quantity,
                side=request.side,
                entry_price=request.entry_price,
                target_price=request.target_price,
                stop_price=request.stop_price,
                offset_ticks=self.config.default_limit_offset_ticks,
            )
            if self.config.provider.lower() == "tradier":
                payload = self._build_tradier_payload(order, request)
                response = await self.broker.modify_order(order_id, payload)
            else:
                response = await self.broker.modify_order(order_id, order.to_payload())
            status = self._status_from_response(response, request)
            self._register_status(status)
            return status
        return None

    async def monitor_order(self, redis: Redis, status: OrderStatus) -> None:
        if self.config.provider.lower() != "tradier":
            return
        client_id = self._client_id_from_status(status)
        request = self._order_request_for(client_id) or OrderRequest.from_dict(status.request)
        self._order_requests.setdefault(client_id, request)
        poll_interval = max(self.config.tradier_poll_interval_secs, 0.5)
        timeout = max(self.config.tradier_status_timeout_secs, poll_interval)
        elapsed = 0.0
        last_state = status.state.lower()
        last_fills = [dict(fill) for fill in status.fills]
        try:
            while elapsed < timeout:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval
                order_id = self._client_to_order_id.get(client_id, status.order_id)
                if not order_id:
                    continue
                response = await self.broker.get_order(order_id)
                next_status = self._status_from_response(response, request)
                self._register_status(next_status)
                state_changed = next_status.state.lower() != last_state
                fills_changed = next_status.fills != last_fills
                if state_changed or fills_changed:
                    await publish_json(redis, OMS_ORDER_STREAM, next_status.to_dict())
                    await self.record_status(next_status, redis)
                    last_state = next_status.state.lower()
                    last_fills = [dict(fill) for fill in next_status.fills]
                if next_status.state.lower() in {"filled", "cancelled", "rejected"}:
                    break
        except asyncio.CancelledError:  # pragma: no cover - shutdown
            return
        finally:
            self._active_pollers.discard(asyncio.current_task())

    def start_status_monitor(self, redis: Redis, status: OrderStatus) -> None:
        if self.config.provider.lower() != "tradier":
            return
        task = asyncio.create_task(self.monitor_order(redis, status))
        self._active_pollers.add(task)

    @staticmethod
    def _extract_status_fields(response: Mapping[str, object]) -> tuple[str, str, list[Mapping[str, object]], Mapping[str, object]]:
        broker_payload = dict(response)
        if "order" in broker_payload and isinstance(broker_payload["order"], Mapping):
            primary = broker_payload["order"]
        else:
            primary = broker_payload

        order_id = str(primary.get("id", broker_payload.get("id", "")))
        state = str(primary.get("status", broker_payload.get("status", "unknown")))

        raw_fills: object = (
            primary.get("executions")
            or broker_payload.get("executions")
            or broker_payload.get("fills")
        )
        if isinstance(raw_fills, Mapping) and "execution" in raw_fills:
            raw_fills = raw_fills["execution"]

        fills: list[Mapping[str, object]]
        if isinstance(raw_fills, Mapping):
            fills = [dict(raw_fills)]
        elif isinstance(raw_fills, list):
            fills = [dict(item) for item in raw_fills]
        else:
            fills = []

        return order_id, state, fills, broker_payload


async def run_oms_stream(service: OMSService, redis: Redis, *, stop_event: asyncio.Event | None = None) -> None:
    def should_stop() -> bool:
        return stop_event.is_set() if stop_event else False

    async def handle_order(payload: Mapping[str, object]) -> None:
        request = OrderRequest.from_dict(dict(payload))
        status = await service.route_order(request)
        await publish_json(redis, OMS_ORDER_STREAM, status.to_dict())
        await service.record_status(status, redis)
        service.start_status_monitor(redis, status)

    async def handle_command(payload: Mapping[str, object]) -> None:
        command = OrderCommand.from_dict(dict(payload))
        status = await service.handle_command(command, redis)
        if status is not None:
            await publish_json(redis, OMS_ORDER_STREAM, status.to_dict())
            await service.record_status(status, redis)

    tasks = [
        asyncio.create_task(consume_stream(redis, RISK_ORDER_STREAM, handle_order, stop=should_stop)),
        asyncio.create_task(consume_stream(redis, RISK_COMMAND_STREAM, handle_command, stop=should_stop)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        raise
    finally:
        pollers = list(service._active_pollers)
        for poller in pollers:
            poller.cancel()
        if pollers:
            await asyncio.gather(*pollers, return_exceptions=True)


async def main_async() -> None:
    config_map = load_broker_config()
    config = OMSConfig(**config_map)
    service = OMSService(config)
    redis = await create_redis()
    try:
        await run_oms_stream(service, redis)
    finally:
        await close_redis(redis)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
