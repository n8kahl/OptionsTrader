# DreamBot Runbook

## Start
`docker compose up -d`

## Stop
`docker compose down`

## Watch Logs
`docker compose logs -f oms` *(replace `oms` with any service name, e.g., `redis` for stream health)*

## Common Incidents
- Feed stale → system enters defensive mode automatically. Investigate network and confirm feeds resume.
- Slippage spike → offsets widen automatically. Consider temporary stand-down until spreads normalize.
- Promotion to live → set `paper=false` in `config/broker.yaml`, redeploy, and confirm with minimal size first.
- Backtest → `docker compose run --rm backtest --from 2024-01-01 --to 2024-12-31 --symbols SPY,QQQ,SPX,NDX`

## Production Checklist (v1)
1. **Market Data** – Run `ops/nightly_calibration.py` (REST sync) nightly to keep `backtests/calibration.json` current. Confirm ingest heartbeat (`dreambot:ingest_heartbeat`) stays < 5000 ms.
2. **Learner Updates** – Restart learner after calibration or send SIGHUP (future automation) so new `risk_multiplier`/`pot_threshold` are loaded.
3. **Tradier Sandbox Test** – Before market open, run `python scripts/test_tradier_sandbox.py` to place/cancel a 1-lot OTOCO in the sandbox. Investigate before going live if any status stays `open` > 60 s.
4. **OMS/Risk Health** – Verify `dreambot:oms_metrics` latency < 500 ms median; check dashboard for red indicators.
5. **Dashboard** – Review Streamlit panels (heartbeat, OMS metrics, calibration summary) before session start.

## Nightly Calibration (REST)
```
export POLYGON_API_KEY=<your_key>
python -m ops.nightly_calibration \
  --symbols SPY QQQ "I:SPX" "I:NDX" \
  --days 60 \
  --sync-method rest \
  --flatfile-dest data/flatfiles \
  --calibration-output backtests/calibration.json \
  --trades-output backtests/trades.json
```
Outputs:
- `data/flatfiles/{SYMBOL}/YYYY-MM-DD.csv` – per-day minute bars.
- `backtests/calibration.json` – learner offsets (`risk_multiplier`, `pot_threshold`).
- `backtests/trades.json` – detailed trade log for audits.

## Tradier Sandbox Smoke Test
Environment:
- `TRADIER_SANDBOX_TOKEN` – sandbox OAuth token.
- `TRADIER_SANDBOX_ACCOUNT` – sandbox account ID (e.g., 12345678).
- Optional `TRADIER_ENV=sandbox` to force sandbox base URL.

Run:
```
python scripts/test_tradier_sandbox.py \
  --underlying SPY \
  --option SPY241011C00450000 \
  --side BUY \
  --quantity 1 \
  --entry 0.05 --target 0.15 --stop 0.02
```
The script will place an OTOCO, poll status, and cancel. Expected statuses: `accepted` → `open` → `cancelled`.

## Alerts & Monitoring (to finish)
- Health thresholds: ingest heartbeat > 5000 ms (page on-call), OMS latency > 1000 ms (warning).
- Retention: rotate `storage/logs/` JSONL weekly; archive `backtests/trades.json` monthly.
- Pending: wire Slack/email hooks to heartbeat + OMS metrics, include in deployment checklist.
