import pytest

from services.features.schemas import FeaturePacket
from services.signals.main import SignalEngine


def make_feature(pot):
    return FeaturePacket(
        ts=1700000000000000,
        symbol="SPY",
        tf="1s",
        vwap=450.0,
        vwap_bands={"1": (449.5, 450.5)},
        atr_1m=1.0,
        atr_1s=0.2,
        adx_3m=25,
        vwap_slope=0.001,
        rv_5m=0.02,
        rv_15m=0.03,
        iv_9d=0.2,
        iv_30d=0.22,
        iv_60d=0.25,
        skew_25d=0.0,
        vol_of_vol=0.05,
        micro={
            "nbbo_age_ms": 100,
            "spread_pct": 0.003,
            "spread_state": "normal",
            "cvd_90s": 0.0,
            "es_lead_agree": True,
        },
        prob={"p_itm": 0.5, "pot_est": pot},
    )


def test_signal_engine_respects_pot_override():
    engine = SignalEngine({
        "nbbo_age_ms_max": 800,
        "spread_pct_max": 0.01,
        "trend_threshold": -0.2,
        "adx_threshold": 20,
        "pot_threshold": 0.55,
    })
    feature = make_feature(0.5)
    with pytest.raises(RuntimeError):
        engine.evaluate(feature.ts, feature.symbol, feature, feature.atr_1m, {})
    result = engine.evaluate(
        feature.ts,
        feature.symbol,
        feature,
        feature.atr_1m,
        {"pot_threshold": 0.45},
    )
    assert result["playbook"]
