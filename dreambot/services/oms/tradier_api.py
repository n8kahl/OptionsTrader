"""Tradier REST client wrapper."""
from __future__ import annotations

import asyncio
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


class TradierClient:
    def __init__(self, config: TradierConfig, session: Optional[object] = None):
        self.config = config
        if aiohttp is None:
            raise RuntimeError("aiohttp is required for TradierClient but is not installed")
        self._session = session or aiohttp.ClientSession()
        self._external_session = session is not None

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
        }
        async with self._session.request(method, url, headers=headers, data=payload) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._request("POST", f"/accounts/{self.config.account_id}/orders", payload)

    async def modify_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._request("PUT", f"/accounts/{self.config.account_id}/orders/{order_id}", payload)

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return await self._request("DELETE", f"/accounts/{self.config.account_id}/orders/{order_id}")


class InMemoryBroker:
    """Sandbox broker used in tests and backtests."""

    def __init__(self):
        self.orders: Dict[str, Dict[str, Any]] = {}
        self._counter = 0

    async def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._counter += 1
        order_id = str(self._counter)
        record = {"id": order_id, "payload": payload, "status": "filled"}
        self.orders[order_id] = record
        return record

    async def modify_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        order = self.orders[order_id]
        order["payload"].update(payload)
        return order

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        order = self.orders.pop(order_id, None)
        return order or {"id": order_id, "status": "cancelled"}
