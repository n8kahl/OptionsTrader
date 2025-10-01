"""Parsing helpers for dashboard metrics."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Mapping


@dataclass(slots=True)
class HeartbeatSnapshot:
    ts: int
    mode: str
    quotes_age_ms: float | None
    aggs_age_ms: float | None
    option_meta_age_ms: float | None
    quote_count: int
    agg_count: int
    option_meta_count: int

    @property
    def delay_ms(self) -> float:
        parts = [age for age in (self.quotes_age_ms, self.aggs_age_ms, self.option_meta_age_ms) if age is not None]
        return max(parts) if parts else 0.0

    def to_row(self) -> Mapping[str, object]:
        return {
            "timestamp": datetime.fromtimestamp(self.ts / 1_000_000, tz=timezone.utc).isoformat(),
            "mode": self.mode,
            "delay_ms": round(self.delay_ms, 2),
            "quotes_age_ms": round(self.quotes_age_ms or 0.0, 2),
            "aggs_age_ms": round(self.aggs_age_ms or 0.0, 2),
            "option_meta_age_ms": round(self.option_meta_age_ms or 0.0, 2),
            "quote_count": self.quote_count,
            "agg_count": self.agg_count,
            "option_meta_count": self.option_meta_count,
        }


@dataclass(slots=True)
class OMSMetric:
    ts: int
    order_id: str
    client_order_id: str | None
    state: str
    latency_ms: float
    quantity: int
    filled_qty: int
    avg_fill_price: float | None

    def to_row(self) -> Mapping[str, object]:
        return {
            "timestamp": datetime.fromtimestamp(self.ts / 1_000_000, tz=timezone.utc).isoformat(),
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "state": self.state,
            "latency_ms": round(self.latency_ms, 2),
            "quantity": self.quantity,
            "filled_qty": self.filled_qty,
            "avg_fill_price": round(self.avg_fill_price, 4) if self.avg_fill_price is not None else None,
        }


def parse_heartbeat(payload: Mapping[str, object]) -> HeartbeatSnapshot:
    mode = str(payload.get("mode", "synthetic"))
    quotes = payload.get("quotes", {}) if isinstance(payload.get("quotes"), Mapping) else {}
    aggs = payload.get("aggs", {}) if isinstance(payload.get("aggs"), Mapping) else {}
    option_meta = payload.get("option_meta", {}) if isinstance(payload.get("option_meta"), Mapping) else {}
    return HeartbeatSnapshot(
        ts=int(payload.get("ts", 0)),
        mode=mode,
        quotes_age_ms=float(quotes.get("age_ms")) if quotes.get("age_ms") is not None else None,
        aggs_age_ms=float(aggs.get("age_ms")) if aggs.get("age_ms") is not None else None,
        option_meta_age_ms=float(option_meta.get("age_ms")) if option_meta.get("age_ms") is not None else None,
        quote_count=int(quotes.get("count", 0)),
        agg_count=int(aggs.get("count", 0)),
        option_meta_count=int(option_meta.get("count", 0)),
    )


def parse_oms_metric(payload: Mapping[str, object]) -> OMSMetric:
    return OMSMetric(
        ts=int(payload.get("ts", 0)),
        order_id=str(payload.get("order_id", "")),
        client_order_id=payload.get("client_order_id"),
        state=str(payload.get("state", "unknown")),
        latency_ms=float(payload.get("latency_ms", 0.0)),
        quantity=int(payload.get("quantity", 0)),
        filled_qty=int(payload.get("filled_qty", 0)),
        avg_fill_price=float(payload["avg_fill_price"]) if payload.get("avg_fill_price") is not None else None,
    )


def summarize_oms(metrics: Iterable[OMSMetric]) -> Mapping[str, object]:
    metrics_list: List[OMSMetric] = list(metrics)
    total = len(metrics_list)
    filled = sum(1 for m in metrics_list if m.state.lower() == "filled")
    cancelled = sum(1 for m in metrics_list if m.state.lower() == "cancelled")
    avg_latency = (
        sum(m.latency_ms for m in metrics_list) / total if total else 0.0
    )
    return {
        "total": total,
        "filled": filled,
        "cancelled": cancelled,
        "avg_latency_ms": round(avg_latency, 2),
    }
