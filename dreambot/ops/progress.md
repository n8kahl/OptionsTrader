# DreamBot Progress & Roadmap

## Current Status (2025-10-01)
- Full project scaffold generated per production spec (services, configs, docker-compose, tests).
- Core analytics implemented: VWAP/ATR/ADX, microstructure, vol-surface, Black–Scholes probability, PoT approximation.
- Service skeletons in place for ingest, features, signals, risk, OMS, learner, backtest, and dashboard (async loops keep containers alive).
- Ingest option-universe rotation enforces Polygon 1,000-contract ceiling via `OptionUniverseManager`.
- Signal gating enforces NBBO age, spread %, and PoT thresholds; playbooks produce OTOCO-friendly intents.
- Risk manager enforces daily loss cap, position limits, econ halts, force-flat windows; OMS builds OTOCO payloads with in-memory broker.
- Backtest runner reuses live feature code, deterministic RNG, placeholder fill model, and metrics snapshot.
- Streamlit dashboard stub renders status/heatmap panels for health visualization.
- Redis Streams backbone online: ingest publishes synthetic Quote/Agg/OptionMeta batches, features consume & emit Feature packets, signals generate intents downstream.
- Risk service now consumes SignalIntent stream, applies guardrails, and emits OrderRequest messages; OMS consumes risk-approved orders and records placements to Redis.
- Acceptance pytest suite (12 tests) green, including stream pipeline coverage alongside feature parity, gating rules, OMS lifecycle, option universe caps, econ halts, and calibration I/O.
- (2025-10-01) Polygon ingest now streams equities/indices/options with dynamic option-universe rotation, config toggles per cluster, and targeted pytest coverage remains green.
- (2025-10-01) Execution analytics service now derives slippage/latency/risk-reward metrics from OMS fills and publishes `dreambot:execution_reports` for downstream consumers.
- (2025-10-02) Polygon ingest now pulls live websockets + snapshot rotation with delta/DTE filters, redis stream auditing via `STREAM_AUDIT_PATH`, and coverage expanded with audit/universe tests.
- (2025-10-02) Polygon service now records websocket traffic to rotated JSONL snapshots, emits `dreambot:ingest_heartbeat` lag metrics, and ships replay helpers with pytest coverage.
- (2025-10-02) OMS integrates Tradier advanced OTOCO routing (sandbox/live), configurable credentials, resilient client retries, and payload conformance tests.
- (2025-10-02) Tradier OMS now polls sandbox order status, reacts to risk-driven cancel/modify commands on `dreambot:risk_commands`, and risk service schedules time-stop cancels and partial-fill stop tightenings.
- (2025-10-02) OMS status events now stream `dreambot:oms_metrics` latency/fill telemetry and persist JSONL audits (`OMS_AUDIT_PATH`) for compliance replay.
- (2025-10-02) Backtest runner now loads CSV/ synthetic bars, replays signals, and emits JSON summaries with expectancy/win-rate for rapid strategy validation.
- (2025-10-02) Calibration CLI aggregates backtest trades, writes `backtests/calibration.json` (risk multiplier & pot threshold), and learner streams now apply these offsets to live adjustments.
- (2025-10-02) Polygon flat-file sync tool fetches recent aggregates via temporary S3 credentials (`services/backtest/polygon_sync.py`) for nightly calibration ingest.
- (2025-10-02) Nightly orchestration script (`ops/nightly_calibration.py`) pulls flat files and regenerates `backtests/calibration.json` in one step.
- (2025-10-02) Tradier sandbox smoke-test script (`scripts/test_tradier_sandbox.py`) places/cancels an OTOCO for pre-market verification.
- (2025-10-02) Calibration optimizer runs per-symbol grid search (pot/ADX), writes symbol-specific parameters, and learner/signal pipeline now apply per-symbol risk & gating overrides.
- (2025-10-02) Portfolio service streams real-time PnL/positions and dashboard displays recent signals, OMS orders, and PnL metrics.

## Near-Term Roadmap
1. **Live Data Hardening**
   - Surface ingest heartbeat metrics in dashboard/alerts; derive lag thresholds and alerting.
   - Build snapshot replay CLI + golden-path integration test using captured feeds.
2. **Tradier OMS Enhancements**
   - Backfill OMS dashboard panes with live status/latency metrics and partial-fill analytics.
   - Automate archival/rotation policies for OMS audits in ops runbook.
3. **Risk & OMS Enhancements**
   - Track live PnL/exposure from executions; enforce kill switch & force-flat in OMS.
   - Implement stop sync updates reacting to underlying ticks; ensure OTOCO leg modifications/alerts.
4. **Learner & Calibration Loop**
   - Connect backtest outputs to nightly DuckDB jobs, generating calibration JSON (PoT offsets, spread z thresholds).
   - Train contextual bandit/meta-label classifier; persist weights and integrate with signal sizing.
5. **Backtest Fidelity**
   - Flesh out fill model using spread state/event rate; produce `summary.csv`, `fill_quality.csv`, `calibration.json` from runs.
   - Add regression checks comparing live vs. replay equity curves.
6. **Observability & Dashboard**
   - Emit JSON logs, metrics endpoints, and health heartbeats per service.
   - Expand Streamlit panels: VWAP bands chart from live data, option chain heatmap, regime/learner, risk dials, incident timers.
7. **Deployment Hardening**
   - Author Dockerfiles per service (or multi-stage build) with dependency installs.
   - Set up CI/CD, linting, formatting, and infrastructure automation (DigitalOcean Ubuntu target).

## Operational Reminders
- Keep `.env` (Polygon/Tradier secrets) local; never commit.
- Run stack via `docker compose up -d`; dashboard on `http://localhost:8501`.
- Tests: `pytest` from `dreambot/` after environment changes.

## Next Update Procedure
- After each meaningful coding session, append bullet(s) under **Current Status** with date and summarize new capabilities/tests.
- Refresh roadmap items as tasks complete or reprioritize; mark done items explicitly or move to a "Completed" section if preferred.
