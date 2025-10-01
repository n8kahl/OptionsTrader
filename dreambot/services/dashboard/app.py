"""Streamlit dashboard for DreamBot."""
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from .charts import render_underlying_panel
from .heatmap import render_option_heatmap


st.set_page_config(page_title="DreamBot Dashboard", layout="wide")
st.title("DreamBot Monitoring")

status_path = Path("storage/logs/status.json")
if status_path.exists():
    with status_path.open("r", encoding="utf-8") as handle:
        status = json.load(handle)
else:
    status = {
        "regime": {"playbook": "TREND_PULLBACK", "bandit": {"TREND_PULLBACK": 0.4, "BALANCE_FADE": 0.3}},
        "risk": {"pnl": 0.0, "positions": []},
        "health": {"feeds": "ok"},
    }

col1, col2 = st.columns((2, 1))
with col1:
    render_underlying_panel(status)
with col2:
    render_option_heatmap(pd.DataFrame([
        {"strike": 450, "delta": 0.45, "iv": 0.22, "pot": 0.6, "spread_pct": 0.005},
        {"strike": 452, "delta": 0.50, "iv": 0.24, "pot": 0.62, "spread_pct": 0.007},
    ]))

st.subheader("Risk & OMS")
st.json(status.get("risk", {}))

st.subheader("Learner State")
st.json(status.get("regime", {}))

st.subheader("Health")
st.json(status.get("health", {}))
