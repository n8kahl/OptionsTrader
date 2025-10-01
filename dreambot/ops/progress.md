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
- Acceptance pytest suite (11 tests) green, covering feature parity vs. backtest, gating rules, OMS lifecycle, option universe caps, econ halts, kill switch, calibration I/O.

## Near-Term Roadmap
1. **Wire Real Data & Messaging**
   - Connect ingest outputs to Redis Streams or chosen message bus; emit Quote/Agg/OptionMeta payloads.
   - Plumb features/signals/risk/OMS/learner services to subscribe & publish via the shared contracts.
2. **Tradier & Polygon Integration**
   - Replace in-memory broker with authenticated Tradier client (requires aiohttp + sandbox/live routing config).
   - Implement Polygon websocket/REST handlers with resilience, heartbeats, reconnection, and option chain filtering by delta/DTE.
3. **Risk & OMS Enhancements**
   - Track live PnL/exposure from executions; enforce kill switch & force-flat in OMS.
   - Implement stop sync updates reacting to underlying ticks; ensure OTOCO leg modifications/alerts.
4. **Learner & Calibration Loop**
   - Implement nightly backtest jobs reading DuckDB flat files, generating calibration JSON (PoT offsets, spread z thresholds).
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
