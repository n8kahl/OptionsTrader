"""Risk guardrails and enforcement."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping


@dataclass
class RiskConfig:
    daily_loss_cap: float
    per_trade_max_risk_pct: float
    max_concurrent_positions: int
    no_trade_first_seconds: int
    econ_halt_minutes_pre_post: int
    force_flat_before_close_secs: int
    defensive_mode: Mapping[str, float]


@dataclass
class RiskState:
    pnl: float = 0.0
    open_positions: int = 0
    session_start_ts: int = 0
    last_trade_ts: int = 0
    defensive: bool = False


class RiskManager:
    def __init__(self, config: RiskConfig):
        self.config = config
        self.state = RiskState()

    def set_session_start(self, ts: int) -> None:
        self.state.session_start_ts = ts

    def register_fill(self, pnl_delta: float, ts: int) -> None:
        self.state.pnl += pnl_delta
        self.state.last_trade_ts = ts

    def register_position(self, delta: int) -> None:
        self.state.open_positions = max(self.state.open_positions + delta, 0)

    def update_defensive(self, slippage_z: float, spread_z: float) -> None:
        thresholds = self.config.defensive_mode
        self.state.defensive = slippage_z >= thresholds["slippage_z"] or spread_z >= thresholds["spread_z"]

    def entry_allowed(self, ts: int, minutes_to_open: int, minutes_to_close: int) -> bool:
        if self.state.pnl <= self.config.daily_loss_cap:
            return False
        if self.state.open_positions >= self.config.max_concurrent_positions:
            return False
        seconds_since_open = (ts - self.state.session_start_ts) / 1_000_000
        if seconds_since_open < self.config.no_trade_first_seconds:
            return False
        if abs(minutes_to_open) <= self.config.econ_halt_minutes_pre_post:
            return False
        if minutes_to_close * 60 <= self.config.force_flat_before_close_secs:
            return False
        return True

    def enforce_exit(self) -> bool:
        if self.state.pnl <= self.config.daily_loss_cap:
            return True
        return False

    def risk_budget(self, account_equity: float) -> float:
        return account_equity * self.config.per_trade_max_risk_pct
