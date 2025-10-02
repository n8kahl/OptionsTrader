"""Polygon snapshot recording utilities."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class SnapshotRecorder:
    """Appends raw websocket payloads to JSONL files for offline replay."""

    def __init__(self, root: str, rotate_bytes: int) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.rotate_bytes = max(rotate_bytes, 0)
        self._lock = asyncio.Lock()
        self._file = self.root / "polygon_messages.jsonl"

    async def write(self, payload: str) -> None:
        if not payload:
            return
        async with self._lock:
            await asyncio.to_thread(self._append, payload)

    def _append(self, payload: str) -> None:
        if self.rotate_bytes and self._file.exists() and self._file.stat().st_size >= self.rotate_bytes:
            timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
            rotated = self._file.with_name(f"polygon_messages.{timestamp}.jsonl")
            self._file.rename(rotated)
        with self._file.open("a", encoding="utf-8") as handle:
            handle.write(payload.strip() + "\n")
