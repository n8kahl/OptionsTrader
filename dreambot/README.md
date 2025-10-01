# DreamBot — NDX/SPX/QQQ/SPY Options Scalping & Day-Trading System

DreamBot is a multi-service options scalping stack targeting SPX, NDX, SPY, and QQQ. The system ingests real-time market data from Polygon.io, produces microstructure-driven features, evaluates contextual playbooks, and routes OTOCO orders to Tradier. Risk controls, learner feedback loops, and backtesting are all containerized behind Docker Compose.

## Quickstart

1. Duplicate `.env.example` to `.env` and populate Polygon and Tradier credentials.
2. Ensure Docker and Docker Compose are installed.
3. Launch the stack with `docker compose up -d` from the `dreambot` directory.
4. Open the Streamlit dashboard at `http://localhost:8501`.

## Services

- `ingest`: Polygon websockets and option chain snapshots feeding Redis Streams.
- `features`: Computes VWAP bands, ATR, ADX/DMI, probabilities, microstructure, and vol-surface signals.
- `signals`: Applies playbooks with contextual gating and outputs signal intents.
- `risk`: Enforces guardrails, economic halt windows, and kill-switch logic.
- `oms`: Routes OTOCO orders to Tradier and syncs stops to underlying moves.
- `learner`: Trains the contextual bandit, meta-labeling classifier, and change-point detection.
- `backtest`: Replays historical data via DuckDB flat files for calibration.
- `dashboard`: Streamlit interface for monitoring health, regimes, orders, and learner state.

## Testing

Install the project locally with `pip install -e .[test]` and run `pytest` from the `dreambot` directory. The acceptance suite validates feature parity, gating policies, OMS lifecycle flows, and calibration updates.

## Storage Layout

- `data/`: Polygon flat files for replay/backtests.
- `storage/parquet`: Persisted live outputs and calibration artifacts.
- `storage/logs`: JSON log output from services.
- `backtests/`: Generated reports (`summary.csv`, `fill_quality.csv`, `calibration.json`).

Refer to `ops/runbook.md` for operational procedures and incident response guidance.
