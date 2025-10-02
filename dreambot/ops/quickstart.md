# DreamBot Quickstart (Non-Developer)

This guide gives you simple commands to run everything and keep Ubuntu in sync.

## 1) Codespaces (Do Work Here)
- Open this repo in GitHub Codespaces. Secrets stay in `.env` (never commit).
- Start/stop stack and calibrate with one command:

```
./scripts/dreamctl.sh up        # start services (dashboard at http://localhost:8501)
./scripts/dreamctl.sh calibrate # sync data + compute calibration with higher win-rate bias
./scripts/dreamctl.sh status    # show last calibration summary
./scripts/dreamctl.sh down      # stop services
```

## 2) Commit Changes
- In Codespaces:
```
git add -A && git commit -m "calibration + changes" && git push
```

## 3) Ubuntu Server (Sync + Run)
- Log in to your Ubuntu box and pull latest code:
```
git pull
cd dreambot
docker compose up -d
```
- Copy your `.env` with secrets on the server (do not commit it).

## 4) Nightly Automation (Ubuntu)
Run calibration nightly inside Docker with a systemd timer (uses your Docker Compose files):

```
sudo bash dreambot/ops/install_nightly.sh /path/to/repo/dreambot "SPY QQQ I:SPX I:NDX" 60 0.4 400
```

What it does:
- Stores `POLYGON_API_KEY` in `/etc/dreambot.env` (you will be prompted).
- Installs `dreambot-nightly.service` and `dreambot-nightly.timer`.
- Runs calibration at 03:15 UTC, writing `backtests/calibration.json`.

Check status:
```
systemctl status dreambot-nightly.timer
journalctl -u dreambot-nightly.service -n 200 -f
```

## 5) Updating After Calibration
- The learner reads `backtests/calibration.json` on start; after the nightly job runs, restart the stack if needed:
```
cd dreambot && docker compose restart learner signals
```

## 6) Sanity Checks
- Dashboard at `http://SERVER_IP:8501` should show heartbeats and metrics.
- To verify the sandbox OMS before the open:
```
python scripts/test_tradier_sandbox.py --option SPY241018C00450000 --side BUY --quantity 1 --entry 0.05 --target 0.15 --stop 0.02
```

That’s it. Use Codespaces for edits and calibration, push to GitHub; on Ubuntu, pull + `docker compose up -d`. Nightly calibration keeps win-rate bias fresh.

