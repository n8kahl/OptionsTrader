"""Streamlit dashboard for DreamBot."""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import List

import pandas as pd
import redis
import streamlit as st

from services.common.streams import (
    INGEST_HEARTBEAT_STREAM,
    OMS_METRIC_STREAM,
    PORTFOLIO_STREAM,
    OMS_ORDER_STREAM,
    SIGNAL_STREAM,
)

from .charts import render_underlying_panel
from .heatmap import render_option_heatmap
from .metrics import parse_heartbeat, parse_oms_metric, parse_portfolio, summarize_oms

st.set_page_config(page_title="DreamBot Dashboard", layout="wide")
st.title("DreamBot Monitoring")


@lru_cache(maxsize=1)
def get_redis_client() -> redis.Redis | None:
    url = os.environ.get("DASHBOARD_REDIS_URL", "redis://localhost:6379/0")
    try:
        client = redis.Redis.from_url(url, decode_responses=True)
        client.ping()
        return client
    except Exception:
        return None


def load_status_snapshot() -> dict:
    status_path = Path("storage/logs/status.json")
    if status_path.exists():
        with status_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return {
        "regime": {"playbook": "TREND_PULLBACK", "bandit": {"TREND_PULLBACK": 0.4, "BALANCE_FADE": 0.3}},
        "risk": {"pnl": 0.0, "positions": []},
        "health": {"feeds": "unknown"},
    }


def fetch_latest_heartbeat(client: redis.Redis | None):
    if client is None:
        return None
    try:
        entries = client.xrevrange(INGEST_HEARTBEAT_STREAM, count=1)
    except Exception:
        return None
    if not entries:
        return None
    _, data = entries[0]
    payload = json.loads(data.get("data", "{}")) if isinstance(data.get("data"), str) else data
    return parse_heartbeat(payload)


def fetch_recent_oms_metrics(client: redis.Redis | None, limit: int = 20):
    if client is None:
        return []
    stream_name = os.environ.get("OMS_METRIC_STREAM", OMS_METRIC_STREAM)
    try:
        entries = client.xrevrange(stream_name, count=limit)
    except Exception:
        return []
    metrics: List[object] = []
    for _, data in entries:
        payload = json.loads(data.get("data", "{}")) if isinstance(data.get("data"), str) else data
        metrics.append(parse_oms_metric(payload))
    metrics.reverse()
    return metrics


def fetch_latest_portfolio(client: redis.Redis | None):
    if client is None:
        return None
    try:
        entries = client.xrevrange(PORTFOLIO_STREAM, count=1)
    except Exception:
        return None
    if not entries:
        return None
    _, data = entries[0]
    payload = json.loads(data.get("data", "{}")) if isinstance(data.get("data"), str) else data
    return parse_portfolio(payload)


def fetch_recent_stream(client: redis.Redis | None, stream: str, limit: int = 20):
    if client is None:
        return []
    try:
        entries = client.xrevrange(stream, count=limit)
    except Exception:
        return []
    rows = []
    for _, data in entries:
        payload = json.loads(data.get("data", "{}")) if isinstance(data.get("data"), str) else data
        rows.append(payload)
    rows.reverse()
    return rows


def render_observability_panels(redis_client: redis.Redis | None) -> None:
    st.subheader("Ingest Heartbeat")
    heartbeat = fetch_latest_heartbeat(redis_client)
    if heartbeat:
        hb_row = heartbeat.to_row()
        st.metric(
            label="Max Delay (ms)",
            value=hb_row["delay_ms"],
            help="Worst-case component lag from latest heartbeat",
        )
        st.caption(
            f"Mode: {hb_row['mode']} • Quotes: {hb_row['quote_count']} • Aggs: {hb_row['agg_count']} • Options: {hb_row['option_meta_count']}"
        )
    else:
        st.info("No heartbeat samples available")

    st.subheader("OMS Metrics")
    metrics = fetch_recent_oms_metrics(redis_client)
    if metrics:
        summary = summarize_oms(metrics)
        met_col1, met_col2, met_col3, met_col4 = st.columns(4)
        met_col1.metric("Orders", summary["total"])
        met_col2.metric("Filled", summary["filled"])
        met_col3.metric("Cancelled", summary["cancelled"])
        met_col4.metric("Avg Latency (ms)", summary["avg_latency_ms"])
        df = pd.DataFrame([metric.to_row() for metric in metrics])
        st.dataframe(df, use_container_width=True)
    else:
        st.write("No OMS metrics recorded yet")


status = load_status_snapshot()
redis_client = get_redis_client()

with st.sidebar:
    st.header("Connections")
    if redis_client is None:
        st.error("Redis unavailable — showing cached data only")
    else:
        st.success("Redis connected")

col1, col2 = st.columns((2, 1))
with col1:
    render_underlying_panel(status)
with col2:
    render_option_heatmap(
        pd.DataFrame(
            [
                {"strike": 450, "delta": 0.45, "iv": 0.22, "pot": 0.6, "spread_pct": 0.005},
                {"strike": 452, "delta": 0.50, "iv": 0.24, "pot": 0.62, "spread_pct": 0.007},
            ]
        )
    )

render_observability_panels(redis_client)

st.subheader("Portfolio & PnL")
portfolio = fetch_latest_portfolio(redis_client)
if portfolio:
    met1, met2, met3 = st.columns(3)
    met1.metric("Realized PnL", portfolio["realized_pnl"])
    met2.metric("Unrealized PnL", portfolio["unrealized_pnl"])
    met3.metric("Total PnL", portfolio["total_pnl"])
    if portfolio["positions"]:
        st.dataframe(pd.DataFrame(portfolio["positions"]), use_container_width=True)
else:
    st.info("No portfolio snapshot yet")

st.subheader("Recent Signals")
signals = fetch_recent_stream(redis_client, SIGNAL_STREAM, limit=10)
if signals:
    st.dataframe(pd.DataFrame(signals), use_container_width=True)
else:
    st.write("No recent signals")

st.subheader("Recent OMS Orders")
orders = fetch_recent_stream(redis_client, OMS_ORDER_STREAM, limit=10)
if orders:
    st.dataframe(pd.DataFrame(orders), use_container_width=True)
else:
    st.write("No recent OMS activity")

st.subheader("Risk & OMS Snapshot")
st.json(status.get("risk", {}))

st.subheader("Learner State")
st.json(status.get("regime", {}))

st.subheader("Health Snapshot")
st.json(status.get("health", {}))
