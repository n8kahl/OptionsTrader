"""Backtest performance metrics."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np


@dataclass
class Trade:
    pnl: float


@dataclass
class BacktestReport:
    expectancy: float
    win_rate: float
    avg_win: float
    avg_loss: float
    max_drawdown: float


def summarize(trades: Iterable[Trade]) -> BacktestReport:
    trades = list(trades)
    if not trades:
        return BacktestReport(0.0, 0.0, 0.0, 0.0, 0.0)
    pnls = np.array([trade.pnl for trade in trades], dtype=float)
    expectancy = pnls.mean()
    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    win_rate = len(wins) / len(pnls)
    avg_win = wins.mean() if len(wins) else 0.0
    avg_loss = losses.mean() if len(losses) else 0.0
    equity_curve = pnls.cumsum()
    peak = np.maximum.accumulate(equity_curve)
    drawdowns = peak - equity_curve
    max_dd = drawdowns.max() if len(drawdowns) else 0.0
    return BacktestReport(expectancy, win_rate, avg_win, avg_loss, max_dd)
