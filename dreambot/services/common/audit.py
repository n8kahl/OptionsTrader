"""Stream auditing utilities."""
from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


def _normalize_stream(stream: str) -> str:
    return stream.replace(":", "_").replace("/", "_")


@dataclass(slots=True)
class StreamAuditConfig:
    root: Path
    streams: Optional[Iterable[str]] = None
    rotate_bytes: int = 0

    @classmethod
    def from_env(cls) -> "StreamAuditConfig | None":
        root = os.environ.get("STREAM_AUDIT_PATH")
        if not root:
            return None
        streams_env = os.environ.get("STREAM_AUDIT_STREAMS", "").strip()
        streams: Iterable[str] | None
        if streams_env:
            streams = [item.strip() for item in streams_env.split(",") if item.strip()]
        else:
            streams = None
        rotate = int(os.environ.get("STREAM_AUDIT_ROTATE_BYTES", "0") or 0)
        return cls(root=Path(root), streams=streams, rotate_bytes=rotate)


class StreamAuditor:
    """Writes stream payloads to JSONL files for auditing."""

    def __init__(self, config: StreamAuditConfig):
        self.config = config
        self.config.root.mkdir(parents=True, exist_ok=True)
        self._locks: Dict[Path, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._enabled_streams = set(config.streams) if config.streams else None

    def _should_log(self, stream: str) -> bool:
        if self._enabled_streams is None:
            return True
        return stream in self._enabled_streams

    async def write(self, stream: str, payload: Mapping[str, Any]) -> None:
        if not self._should_log(stream):
            return
        path = self.config.root / f"{_normalize_stream(stream)}.jsonl"
        record = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "stream": stream,
            "payload": payload,
        }
        data = json.dumps(record, separators=(",", ":"))
        lock = self._locks[path]
        async with lock:
            await asyncio.to_thread(self._append, path, data)

    def _append(self, path: Path, data: str) -> None:
        if self.config.rotate_bytes > 0 and path.exists() and path.stat().st_size >= self.config.rotate_bytes:
            timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            rotated = path.with_name(f"{path.stem}.{timestamp}{path.suffix}")
            path.rename(rotated)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(data + "\n")


_AUDITOR: Optional[StreamAuditor] = None


def get_auditor() -> StreamAuditor | None:
    return _AUDITOR


def configure_auditor(config: StreamAuditConfig | None) -> None:
    global _AUDITOR
    if config is None:
        _AUDITOR = None
        return
    _AUDITOR = StreamAuditor(config)


# Configure auditor from environment at import time if requested.
if _AUDITOR is None:
    configure_auditor(StreamAuditConfig.from_env())
