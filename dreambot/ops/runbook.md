# DreamBot Runbook

## Start
`docker compose up -d`

## Stop
`docker compose down`

## Watch Logs
`docker compose logs -f oms` *(replace `oms` with any service name)*

## Common Incidents
- Feed stale → system enters defensive mode automatically. Investigate network and confirm feeds resume.
- Slippage spike → offsets widen automatically. Consider temporary stand-down until spreads normalize.
- Promotion to live → set `paper=false` in `config/broker.yaml`, redeploy, and confirm with minimal size first.
- Backtest → `docker compose run --rm backtest --from 2024-01-01 --to 2024-12-31 --symbols SPY,QQQ,SPX,NDX`
