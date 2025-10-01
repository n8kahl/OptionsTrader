"""OMS order audit logging."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .schemas import OrderStatus


class OrderAuditRecorder:
    """Writes OMS order status snapshots to JSONL for compliance/audit."""

    def __init__(self, path: str, rotate_bytes: int = 0) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.rotate_bytes = max(int(rotate_bytes), 0)
        self._lock = asyncio.Lock()

    async def write(self, status: OrderStatus) -> None:
        record = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "status": status.to_dict(),
        }
        blob = json.dumps(record, separators=(",", ":"))
        async with self._lock:
            await asyncio.to_thread(self._append, blob)

    def _append(self, blob: str) -> None:
        if self.rotate_bytes and self.path.exists() and self.path.stat().st_size >= self.rotate_bytes:
            ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
            rotated = self.path.with_name(f"{self.path.stem}.{ts}{self.path.suffix or '.jsonl'}")
            self.path.rename(rotated)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(blob + "\n")
