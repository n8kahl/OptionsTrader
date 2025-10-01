#!/usr/bin/env bash
set -euo pipefail
service="$1"
python - <<'PY'
import urllib.request
try:
    urllib.request.urlopen('http://localhost:8501/health', timeout=1)
except Exception:
    raise SystemExit(1)
else:
    raise SystemExit(0)
PY >/dev/null 2>&1 && exit 0
if pgrep -f "$service" >/dev/null 2>&1; then
  exit 0
fi
exit 1
