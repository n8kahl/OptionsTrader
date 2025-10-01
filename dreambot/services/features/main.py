"""Feature service orchestrator."""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Mapping, Optional

import numpy as np
import yaml

from .indicators import (
    compute_adx,
    compute_atr,
    compute_fast_atr,
    compute_session_vwap,
    compute_vwap_bands,
    compute_vwap_slope,
    realized_volatility,
)
from .microstructure import SpreadHistory, classify_spread, compute_spread_pct, cumulative_volume_delta, nbbo_age
from .probability import probability_itm, probability_of_touch
from .schemas import FeaturePacket
from .vol_surface import TermStructure, compute_smile_skew, compute_term_structure, realized_vol_gap, vol_of_vol
from ..ingest.schemas import Agg1s, OptionMeta, Quote


@dataclass
class SymbolState:
    prices: Deque[float] = field(default_factory=lambda: deque(maxlen=3600))
    volumes: Deque[float] = field(default_factory=lambda: deque(maxlen=3600))
    highs: Deque[float] = field(default_factory=lambda: deque(maxlen=3600))
    lows: Deque[float] = field(default_factory=lambda: deque(maxlen=3600))
    closes: Deque[float] = field(default_factory=lambda: deque(maxlen=3600))
    returns: Deque[float] = field(default_factory=lambda: deque(maxlen=5400))
    nbbo_ts: int = 0
    last_quote: Optional[Quote] = None
    spread_history: SpreadHistory = field(default_factory=SpreadHistory)
    trade_history: Deque[tuple[str, float]] = field(default_factory=lambda: deque(maxlen=3600))
    es_agree_until: int = 0
    option_iv_terms: Dict[int, float] = field(default_factory=dict)
    call_surface: Dict[float, float] = field(default_factory=dict)
    put_surface: Dict[float, float] = field(default_factory=dict)
    iv_history: Deque[float] = field(default_factory=lambda: deque(maxlen=3600))
    rv_stats_mean: float = 0.0
    rv_stats_stdev: float = 1.0


class FeatureEngine:
    def __init__(self, config: Mapping[str, Any], calibration: Optional[Mapping[str, Any]] = None):
        self.config = config
        self.calibration = calibration or {}
        self._state: Dict[str, SymbolState] = defaultdict(SymbolState)

    def update_quote(self, quote: Quote) -> None:
        state = self._state[quote.symbol]
        state.last_quote = quote
        state.nbbo_ts = quote.ts
        mid = quote.mid if quote.mid else (quote.bid + quote.ask) / 2
        spread_pct = compute_spread_pct(quote.bid, quote.ask, mid)
        state.spread_history.add(spread_pct)

    def update_trade(self, symbol: str, aggressor: str, size: float) -> None:
        state = self._state[symbol]
        state.trade_history.append((aggressor, size))

    def _update_price_series(self, symbol: str, bar: Agg1s) -> SymbolState:
        state = self._state[symbol]
        state.prices.append(bar.c)
        state.volumes.append(bar.v)
        state.highs.append(bar.h)
        state.lows.append(bar.l)
        state.closes.append(bar.c)
        if len(state.closes) > 1:
            ret = np.log(state.closes[-1] / state.closes[-2])
            state.returns.append(float(ret))
        return state

    def update_option(self, meta: OptionMeta) -> None:
        state = self._state[meta.underlying]
        dte = self._days_to_expiry(meta.exp, meta.ts)
        if dte is not None:
            target_terms = self.config["vol_surface"]["term_days"]
            nearest_term = min(target_terms, key=lambda t: abs(t - dte))
            state.option_iv_terms[nearest_term] = meta.iv
            state.iv_history.append(meta.iv)
        delta_key = round(meta.delta, 2)
        if meta.type.upper() == "C":
            state.call_surface[delta_key] = meta.iv
        else:
            state.put_surface[delta_key] = meta.iv

    @staticmethod
    def _days_to_expiry(expiry: str, ts: int) -> Optional[int]:
        try:
            exp_dt = datetime.strptime(expiry, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        now_dt = datetime.fromtimestamp(ts / 1e6, tz=timezone.utc)
        delta = exp_dt - now_dt
        return max(int(delta.total_seconds() // 86400), 0)

    def compute_features(self, symbol: str, bar: Agg1s, es_agree: bool = True) -> FeaturePacket:
        state = self._update_price_series(symbol, bar)
        if es_agree:
            state.es_agree_until = bar.ts + self.config["microstructure"]["es_nq_lead_confirm_secs"] * 1_000_000
        vwap_config = self.config["vwap"]
        vwap = compute_session_vwap(state.prices, state.volumes)
        bands = compute_vwap_bands(
            state.prices,
            state.volumes,
            vwap_config["bands_sigmas"],
            vwap_config["band_stdev_window_secs"],
        )
        slope = compute_vwap_slope(state.prices, state.volumes)
        atr_slow = compute_atr(state.highs, state.lows, state.closes, self.config["atr"]["min_lookback"])
        atr_fast = compute_fast_atr(state.highs, state.lows, state.closes, self.config["atr"]["fast_secs"])
        period = max(1, self.config["adx"]["tf_minutes"] * 60)
        adx = compute_adx(state.highs, state.lows, state.closes, period) if len(state.closes) > period else 0.0
        rv_5m = realized_volatility(state.returns, 5 * 60)
        rv_15m = realized_volatility(state.returns, 15 * 60)
        term_structure = self._term_structure(state)
        skew = compute_smile_skew(state.put_surface, state.call_surface, self.config["vol_surface"]["skew_delta"])
        volvol = vol_of_vol(state.iv_history)
        spread_pct = 0.0
        spread_state = "normal"
        if state.last_quote:
            mid = state.last_quote.mid
            spread_pct = compute_spread_pct(state.last_quote.bid, state.last_quote.ask, mid)
            spread_state = classify_spread(state.spread_history, spread_pct, self.config["microstructure"]["spread_stress_z"])
        nbbo_age_ms = nbbo_age(bar.ts, state.nbbo_ts)
        cvd = cumulative_volume_delta(state.trade_history)
        es_agree_flag = bar.ts <= state.es_agree_until
        iv_front = term_structure.iv_9d
        rv_gap = realized_vol_gap(iv_front, rv_5m, state.rv_stats_mean, max(state.rv_stats_stdev, 1e-9))
        prob_itm = probability_itm("C", max(bar.c, 1e-9), max(bar.c, 1e-9), 0.0, max(iv_front, 1e-4), 1 / 252)
        pot = probability_of_touch(prob_itm)
        features = FeaturePacket(
            ts=bar.ts,
            symbol=symbol,
            tf="1s",
            vwap=vwap,
            vwap_bands=bands,
            atr_1m=atr_slow,
            atr_1s=atr_fast,
            adx_3m=adx,
            vwap_slope=slope,
            rv_5m=rv_5m,
            rv_15m=rv_15m,
            iv_9d=term_structure.iv_9d,
            iv_30d=term_structure.iv_30d,
            iv_60d=term_structure.iv_60d,
            skew_25d=skew,
            vol_of_vol=volvol,
            micro={
                "nbbo_age_ms": nbbo_age_ms / 1_000,
                "spread_pct": spread_pct,
                "spread_state": spread_state,
                "cvd_90s": cvd,
                "es_lead_agree": es_agree_flag,
            },
            prob={"p_itm": prob_itm, "pot_est": pot},
        )
        return features

    def _term_structure(self, state: SymbolState) -> TermStructure:
        target_terms = self.config["vol_surface"]["term_days"]
        vols = {term: state.option_iv_terms.get(term, 0.0) for term in target_terms}
        return compute_term_structure(vols)


async def run_feature_pipeline(source: asyncio.Queue, sink: asyncio.Queue, engine: FeatureEngine, symbol: str) -> None:
    while True:
        bar: Agg1s = await source.get()
        packet = engine.compute_features(symbol, bar)
        await sink.put(packet)
        source.task_done()


def load_feature_config() -> Mapping[str, Any]:
    return yaml.safe_load(Path("config/features.yaml").read_text(encoding="utf-8"))


async def main_async() -> None:
    config = load_feature_config()
    engine = FeatureEngine(config)
    # Placeholder event loop to keep container alive while integration wiring is added.
    while True:
        await asyncio.sleep(5)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
