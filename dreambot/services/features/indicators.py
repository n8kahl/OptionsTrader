"""Indicator calculations shared by live and backtest pipelines."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np


@dataclass(frozen=True)
class VWAPResult:
    value: float
    bands: dict[str, tuple[float, float]]
    slope: float


def _validate_lengths(*arrays: Sequence[float]) -> None:
    lengths = {len(a) for a in arrays}
    if len(lengths) != 1:
        raise ValueError("All input arrays must share the same length")


def compute_session_vwap(prices: Sequence[float], volumes: Sequence[float]) -> float:
    _validate_lengths(prices, volumes)
    prices_arr = np.asarray(prices, dtype=float)
    volumes_arr = np.asarray(volumes, dtype=float)
    volume_sum = volumes_arr.sum()
    if volume_sum <= 0:
        return float(prices_arr[-1])
    return float(np.dot(prices_arr, volumes_arr) / volume_sum)


def compute_vwap_bands(prices: Sequence[float], volumes: Sequence[float], sigmas: Iterable[int],
                        window: int) -> dict[str, tuple[float, float]]:
    """Compute VWAP sigma bands using rolling window of price deviations."""
    _validate_lengths(prices, volumes)
    if not sigmas:
        return {}
    prices_arr = np.asarray(prices, dtype=float)
    vwap = compute_session_vwap(prices, volumes)
    if len(prices_arr) < 2:
        return {str(k): (vwap, vwap) for k in sigmas}
    tail = prices_arr[-window:] if len(prices_arr) >= window else prices_arr
    deviations = tail - vwap
    std = float(np.std(deviations, ddof=1)) if deviations.size > 1 else 0.0
    bands: dict[str, tuple[float, float]] = {}
    for sigma in sigmas:
        offset = std * sigma
        bands[str(sigma)] = (vwap - offset, vwap + offset)
    return bands


def compute_vwap_slope(prices: Sequence[float], volumes: Sequence[float], lookback: int = 30) -> float:
    """Least squares slope of VWAP series."""
    _validate_lengths(prices, volumes)
    if len(prices) < 2:
        return 0.0
    prices_arr = list(prices)
    volumes_arr = list(volumes)
    lb = min(len(prices_arr), lookback)
    vwaps: list[float] = []
    for end in range(len(prices_arr) - lb, len(prices_arr)):
        start = max(0, end - lb + 1)
        v = compute_session_vwap(prices_arr[start:end + 1], volumes_arr[start:end + 1])
        vwaps.append(v)
    if len(vwaps) < 2:
        return 0.0
    y = np.asarray(vwaps, dtype=float)
    x = np.arange(len(vwaps), dtype=float)
    slope, _ = np.polyfit(x, y, 1)
    return float(slope)


def compute_true_range(high: Sequence[float], low: Sequence[float], close: Sequence[float]) -> np.ndarray:
    _validate_lengths(high, low, close)
    high_arr, low_arr, close_arr = map(lambda s: np.asarray(s, dtype=float), (high, low, close))
    prev_close = np.roll(close_arr, 1)
    prev_close[0] = close_arr[0]
    trs = np.maximum.reduce([
        high_arr - low_arr,
        np.abs(high_arr - prev_close),
        np.abs(low_arr - prev_close),
    ])
    return trs


def wilder_smoothing(values: Sequence[float], period: int) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    if len(arr) == 0:
        return arr
    smoothed = np.empty_like(arr)
    smoothed[0] = arr[0]
    alpha = 1.0 / period
    for i in range(1, len(arr)):
        smoothed[i] = smoothed[i - 1] + alpha * (arr[i] - smoothed[i - 1])
    return smoothed


def compute_atr(high: Sequence[float], low: Sequence[float], close: Sequence[float], period: int = 14) -> float:
    trs = compute_true_range(high, low, close)
    if len(trs) == 0:
        return 0.0
    smooth = wilder_smoothing(trs, period)
    return float(smooth[-1])


def compute_fast_atr(high: Sequence[float], low: Sequence[float], close: Sequence[float], alpha_seconds: int) -> float:
    trs = compute_true_range(high, low, close)
    if len(trs) == 0:
        return 0.0
    alpha = 2 / (alpha_seconds + 1)
    ema = trs[0]
    for value in trs[1:]:
        ema = ema + alpha * (value - ema)
    return float(ema)


def compute_adx(high: Sequence[float], low: Sequence[float], close: Sequence[float], period: int) -> float:
    _validate_lengths(high, low, close)
    if len(high) < 2:
        return 0.0
    high_arr, low_arr, close_arr = map(lambda s: np.asarray(s, dtype=float), (high, low, close))
    up_move = high_arr[1:] - high_arr[:-1]
    down_move = low_arr[:-1] - low_arr[1:]
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    trs = compute_true_range(high_arr[1:], low_arr[1:], close_arr[1:])
    trs_smoothed = wilder_smoothing(trs, period)
    denom = np.where(np.abs(trs_smoothed) < 1e-9, 1e-9, trs_smoothed)
    plus_di = 100 * wilder_smoothing(plus_dm, period) / denom
    minus_di = 100 * wilder_smoothing(minus_dm, period) / denom
    dx = 100 * np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, 1e-9)
    adx = wilder_smoothing(dx, period)
    return float(adx[-1])


def realized_volatility(returns: Sequence[float], window: int) -> float:
    if len(returns) == 0:
        return 0.0
    data = list(returns)
    arr = np.asarray(data[-window:], dtype=float)
    if arr.size < 2:
        return 0.0
    std = np.std(arr, ddof=1)
    annualized = std * np.sqrt(252 * 390 * 60)  # approximate trading seconds scaling
    return float(annualized)


def vwap_bundle(prices: Sequence[float], volumes: Sequence[float], sigmas: Iterable[int],
                window: int, slope_lookback: int = 30) -> VWAPResult:
    value = compute_session_vwap(prices, volumes)
    bands = compute_vwap_bands(prices, volumes, sigmas, window)
    slope = compute_vwap_slope(prices, volumes, slope_lookback)
    return VWAPResult(value=value, bands=bands, slope=slope)
