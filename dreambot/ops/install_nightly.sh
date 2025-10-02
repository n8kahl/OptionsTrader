#!/usr/bin/env bash
set -euo pipefail

# Install a systemd timer on Ubuntu to run nightly calibration via Docker Compose.
# This avoids managing Python deps on the host by using the backtest container.

if [[ $EUID -ne 0 ]]; then
  echo "Please run as root (sudo)." >&2
  exit 1
fi

REPO_DIR=${1:-/opt/OptionsTrader/dreambot}
SYMBOLS=${2:-"SPY QQQ I:SPX I:NDX"}
DAYS=${3:-60}
WINRATE=${4:-0.4}
MINTRADES=${5:-400}

read -rp "Enter POLYGON_API_KEY (will be stored in /etc/dreambot.env): " POLY
ENV_FILE=/etc/dreambot.env
echo "POLYGON_API_KEY=${POLY}" > "$ENV_FILE"
chmod 600 "$ENV_FILE"

SERVICE=/etc/systemd/system/dreambot-nightly.service
TIMER=/etc/systemd/system/dreambot-nightly.timer

cat > "$SERVICE" <<EOF
[Unit]
Description=DreamBot Nightly Calibration
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=$ENV_FILE
WorkingDirectory=$REPO_DIR
ExecStart=/usr/bin/docker compose run --rm backtest bash -lc '\
  pip install -e . >/dev/null 2>&1 && \
  python -m ops.nightly_calibration \
    --symbols ${SYMBOLS} \
    --days ${DAYS} \
    --sync-method rest \
    --flatfile-dest data/flatfiles \
    --calibration-output backtests/calibration.json \
    --trades-output backtests/trades.json \
    --min-win-rate ${WINRATE} \
    --min-trades ${MINTRADES} \
    --pot-grid 0.54,0.56,0.58,0.6,0.62 \
    --adx-grid 15,18,20,22,25'
TimeoutStartSec=0
RemainAfterExit=no

[Install]
WantedBy=multi-user.target
EOF

cat > "$TIMER" <<EOF
[Unit]
Description=Run DreamBot Nightly Calibration at 03:15 UTC

[Timer]
OnCalendar=*-*-* 03:15:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now dreambot-nightly.timer
echo "Installed. Check with: systemctl status dreambot-nightly.timer"

