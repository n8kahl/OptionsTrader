#!/usr/bin/env bash
set -euo pipefail
service="$1"
python - <<'PY' >/dev/null 2>&1 && exit 0
import urllib.request
try:
    urllib.request.urlopen('http://localhost:8501/health', timeout=1)
except Exception:
    raise SystemExit(1)
else:
    raise SystemExit(0)
PY
if python - "$service" <<'PY'
import os
import sys

service = sys.argv[1]
proc_root = "/proc"
try:
    entries = os.listdir(proc_root)
except FileNotFoundError:
    sys.exit(1)

for entry in entries:
    if not entry.isdigit():
        continue
    cmdline_path = os.path.join(proc_root, entry, "cmdline")
    try:
        with open(cmdline_path, "rb") as handle:
            cmdline = handle.read().decode(errors="ignore")
    except OSError:
        continue
    if service and service in cmdline:
        sys.exit(0)

sys.exit(1)
PY
then
    exit 0
fi

exit 1
