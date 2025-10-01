"""Option chain snapshot fetcher."""
from __future__ import annotations

from typing import Iterable, List

import aiohttp

from .schemas import OptionMeta


class ChainSnapshotClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._session = aiohttp.ClientSession()

    async def close(self) -> None:
        await self._session.close()

    async def fetch_chain(self, underlying: str) -> List[OptionMeta]:
        url = f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={underlying}&limit=100"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        async with self._session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            payload = await resp.json()
        results = []
        for item in payload.get("results", []):
            results.append(OptionMeta.from_dict({
                "ts": item.get("updated", 0),
                "underlying": underlying,
                "symbol": item["ticker"],
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
            }))
        return results
