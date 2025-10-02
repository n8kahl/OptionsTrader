#!/usr/bin/env bash
set -euo pipefail

# Simple operator helper for non-developers.
# Usage: ./scripts/dreamctl.sh <command> [args]
# Commands:
#   up                 - start the full stack via Docker Compose
#   down               - stop all services
#   logs [service]     - follow logs (default: all)
#   calibrate          - run nightly calibration once with safe defaults
#   status             - show calibration summary and important files
#   test               - run the test suite

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DBOT_DIR="$ROOT_DIR/dreambot"
cd "$DBOT_DIR"

cmd=${1:-}
shift || true

case "${cmd}" in
  up)
    echo "Starting DreamBot stack..."
    docker compose up -d
    echo "Dashboard: http://localhost:8501"
    ;;

  down)
    echo "Stopping DreamBot stack..."
    docker compose down
    ;;

  logs)
    svc=${1:-}
    if [[ -z "$svc" ]]; then
      docker compose logs -f
    else
      docker compose logs -f "$svc"
    fi
    ;;

  calibrate)
    echo "Running calibration (higher win-rate bias)..."
    # Requires POLYGON_API_KEY exported or in dreambot/.env
    python -m ops.nightly_calibration \
      --symbols SPY QQQ I:SPX I:NDX \
      --days 60 \
      --sync-method rest \
      --flatfile-dest data/flatfiles \
      --calibration-output backtests/calibration.json \
      --trades-output backtests/trades.json \
      --min-win-rate 0.4 \
      --min-trades 400 \
      --pot-grid 0.54,0.56,0.58,0.6,0.62 \
      --adx-grid 15,18,20,22,25
    echo "Calibration complete -> backtests/calibration.json"
    ;;

  status)
    echo "Calibration file: backtests/calibration.json"
    if [[ -f backtests/calibration.json ]]; then
      jq '.generated_at, .global, .global_params' backtests/calibration.json || cat backtests/calibration.json
    else
      echo "No calibration found. Run: ./scripts/dreamctl.sh calibrate"
    fi
    ;;

  test)
    echo "Installing test deps and running tests..."
    python -m pip install -U pip >/dev/null 2>&1 || true
    python -m pip install -e .[test]
    pytest -q
    ;;

  *)
    echo "Usage: $0 {up|down|logs [service]|calibrate|status|test}"
    exit 1
    ;;
esac

