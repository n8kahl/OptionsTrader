"""Tradier REST client wrapper."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:  # pragma: no cover - optional dependency for offline tests
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None


@dataclass
class TradierConfig:
    token: str
    account_id: str
    base_url: str = "https://api.tradier.com/v1"
    request_timeout: float = 5.0
    max_retries: int = 5
    retry_backoff_secs: float = 1.0
    poll_interval_secs: float = 1.0
    status_timeout_secs: float = 60.0


class TradierAPIError(RuntimeError):
    """Raised when Tradier returns an error response."""


class TradierClient:
    def __init__(
        self,
        config: TradierConfig,
        *,
        session: Optional[object] = None,
    ):
        self.config = config
        if aiohttp is None:
            raise RuntimeError("aiohttp is required for TradierClient but is not installed")
        if session is not None:
            self._session = session
            self._external_session = True
        else:
            timeout = aiohttp.ClientTimeout(total=config.request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._external_session = False
        self._max_retries = max(config.max_retries, 1)
        self._backoff_secs = max(config.retry_backoff_secs, 0.1)

    async def close(self) -> None:
        if aiohttp is None:
            return
        if not self._external_session:
            await self._session.close()

    async def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.config.token}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        attempt = 0
        backoff = self._backoff_secs
        data = {k: str(v) for k, v in (payload or {}).items() if v is not None}
        while True:
            try:
                async with self._session.request(method, url, headers=headers, data=data) as resp:
                    text = await resp.text()
                    if resp.status >= 400:
                        raise TradierAPIError(f"Tradier error {resp.status}: {text}")
                    if not text:
                        return {}
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        return {"raw": text}
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:  # pragma: no cover - network retries
                attempt += 1
                if attempt >= self._max_retries:
                    raise TradierAPIError("Tradier request failed after retries") from exc
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._request("POST", f"/accounts/{self.config.account_id}/orders", payload)

    async def modify_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._request("PUT", f"/accounts/{self.config.account_id}/orders/{order_id}", payload)

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return await self._request("DELETE", f"/accounts/{self.config.account_id}/orders/{order_id}")

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/accounts/{self.config.account_id}/orders/{order_id}")


class InMemoryBroker:
    """Sandbox broker used in tests and backtests."""

    def __init__(self):
        self.orders: Dict[str, Dict[str, Any]] = {}
        self._counter = 0

    async def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._counter += 1
        order_id = str(self._counter)
        legs = payload.get("legs", [])
        primary_leg = legs[0] if legs else {}
        fill_price = primary_leg.get("limit_price") or primary_leg.get("stop_price") or 0.0
        fill_qty = primary_leg.get("quantity", 0)
        fill_ts = int(time.time() * 1_000_000)
        record = {
            "id": order_id,
            "payload": payload,
            "status": "filled",
            "fills": [
                {
                    "price": fill_price,
                    "qty": fill_qty,
                    "ts": fill_ts,
                }
            ],
        }
        self.orders[order_id] = record
        return record

    async def modify_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        order = self.orders[order_id]
        order["payload"].update(payload)
        return order

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        order = self.orders.pop(order_id, None)
        if order is not None:
            order["status"] = "cancelled"
            return order
        return {"id": order_id, "status": "cancelled"}

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        return self.orders.get(order_id, {"id": order_id, "status": "unknown"})
