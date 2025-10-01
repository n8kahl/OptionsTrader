# DreamBot — NDX/SPX/QQQ/SPY Options Scalping & Day-Trading System

DreamBot is a multi-service options scalping stack targeting SPX, NDX, SPY, and QQQ. The system ingests real-time market data from Polygon.io, produces microstructure-driven features, evaluates contextual playbooks, and routes OTOCO orders to Tradier. Risk controls, learner feedback loops, and backtesting are all containerized behind Docker Compose.

## Quickstart

1. Duplicate `.env.example` to `.env` and populate Polygon and Tradier credentials (set `STREAM_AUDIT_PATH` if you want JSONL stream captures, and optionally `POLYGON_SNAPSHOT_PATH` to store websocket JSONL snapshots).
2. Ensure Docker and Docker Compose are installed.
3. Launch the stack with `docker compose up -d` from the `dreambot` directory (Redis is bundled for inter-service messaging).
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

## Backtesting

Run option-playbook backtests with

```bash
python -m services.backtest.main \
  --symbol SPY \
  --data data/backtests/spy_1m.csv \
  --output storage/backtests/spy_summary.json \
  --trades-output storage/backtests/spy_trades.json
```

If no CSV is provided the runner falls back to deterministic synthetic bars. The JSON summary includes expectancy, win rate, average win/loss, and max drawdown; the optional trades file captures per-trade entry/exit stats. Pass `--table bars_intraday` when pointing at DuckDB files.

Generate calibration offsets for the learner with

```bash
python -m services.backtest.calibrate --symbols SPY QQQ \
  --data data/backtests/spy_1m.duckdb \
  --table bars_intraday \
  --output backtests/calibration.json
```

The calibration JSON records global/playbook expectancy and win rates. The learner ingests `risk_multiplier` and `pot_threshold` from this file to scale future risk and gating thresholds.

### Historical Data Sync

Pull Polygon flat files (minute aggregates) with

```bash
python -m services.backtest.polygon_sync \
  --symbols SPY QQQ SPX NDX \
  --days 60 \
  --dest data/flatfiles
```

Provide credentials one of two ways:

- Temporary: set `POLYGON_API_KEY` (the script will request a short-lived AWS key).
- Static: set `POLYGON_S3_ACCESS_KEY`, `POLYGON_S3_SECRET_KEY`, and optionally `POLYGON_S3_ENDPOINT`/`POLYGON_S3_BUCKET`.

By default the REST sync writes one CSV per symbol/day under `data/flatfiles/{SYMBOL}/YYYY-MM-DD.csv`. Point the calibrator at that directory (or load the rows into DuckDB) before running the calibration step above. Use `--sync-method s3` if you prefer to pull the bulk flat files from `files.polygon.io`.

To automate the nightly flow end-to-end, run (REST sync by default):

```bash
python -m ops.nightly_calibration \
  --symbols SPY QQQ SPX NDX \
  --days 60 \
  --sync-method rest \
  --flatfile-dest data/flatfiles \
  --calibration-output backtests/calibration.json
```

This orchestrates the sync + calibration in one shot and emits refreshed learner offsets for the next trading session.

## Storage Layout

- `data/`: Polygon flat files for replay/backtests.
- `storage/parquet`: Persisted live outputs and calibration artifacts.
- `storage/logs`: JSON log output from services.
- `backtests/`: Generated reports (`summary.csv`, `fill_quality.csv`, `calibration.json`).

## Configuration Notes

- Tune ingest heartbeat/snapshot behaviour via `POLYGON_HEARTBEAT_SECS`, `POLYGON_SNAPSHOT_PATH`, and `POLYGON_SNAPSHOT_ROTATE_MB`.
- Enable OMS audit logging with `OMS_AUDIT_PATH`/`OMS_AUDIT_ROTATE_MB`; metrics land on `dreambot:oms_metrics` (override with `OMS_METRIC_STREAM`).
- Dashboard connects to Redis via `DASHBOARD_REDIS_URL`; live heartbeat (`dreambot:ingest_heartbeat`) and OMS metrics drive the observability panels.
- Tradier OMS polling is controlled in `config/broker.yaml` (`poll_interval_secs`, `status_timeout_secs`, retry/backoff settings).
- Risk-generated cancel/modify commands flow over `dreambot:risk_commands`; ensure OMS and risk services run together in live deployments.

Refer to `ops/runbook.md` for operational procedures and incident response guidance.
