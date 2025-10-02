"""Option chain snapshot fetcher."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

import aiohttp

from .schemas import OptionMeta


class ChainSnapshotClient:
    def __init__(self, api_key: str, *, request_timeout: float = 5.0):
        self.api_key = api_key
        timeout = aiohttp.ClientTimeout(total=request_timeout)
        self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        await self._session.close()

    async def fetch_chain(
        self,
        underlying: str,
        *,
        min_dte: Optional[int] = None,
        max_dte: Optional[int] = None,
        max_options: int = 500,
    ) -> List[OptionMeta]:
        base_url = "https://api.polygon.io/v3/reference/options/contracts"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        params = {
            "underlying_ticker": underlying,
            "limit": 100,
            "order": "asc",
        }
        today = datetime.now(tz=timezone.utc)
        if min_dte is not None:
            params["expiration_date.gte"] = (today + timedelta(days=min_dte)).strftime("%Y-%m-%d")
        if max_dte is not None:
            params["expiration_date.lte"] = (today + timedelta(days=max_dte)).strftime("%Y-%m-%d")

        results: List[OptionMeta] = []
        url = base_url
        backoff = 1.0
        while url and len(results) < max_options:
            try:
                async with self._session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30.0)
                        continue
                    resp.raise_for_status()
                    payload = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue

            backoff = 1.0
            params = None
            next_url = payload.get("next_url")
            url = next_url if next_url else None
            for item in payload.get("results", []):
                option = OptionMeta.from_dict(
                    {
                        "ts": item.get("updated", 0),
                        "underlying": underlying,
                        "symbol": item.get("ticker", ""),
                        "strike": item.get("strike_price", 0.0),
                        "type": "C" if item.get("contract_type") == "call" else "P",
                        "exp": item.get("expiration_date", "1970-01-01"),
                        "iv": item.get("implied_volatility", 0.0),
                        "delta": item.get("greeks", {}).get("delta", 0.0),
                        "gamma": item.get("greeks", {}).get("gamma", 0.0),
                        "vega": item.get("greeks", {}).get("vega", 0.0),
                        "theta": item.get("greeks", {}).get("theta", 0.0),
                        "oi": item.get("open_interest", 0),
                        "prev_oi": item.get("previous_day_open_interest", 0),
                    }
                )
                results.append(option)
                if len(results) >= max_options:
                    break
        return results
