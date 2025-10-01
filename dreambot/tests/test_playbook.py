from services.features.schemas import FeaturePacket
from services.signals.gating import GateResult
from services.signals.policy import choose_playbook


def test_trend_playbook_selected_on_positive_regime():
    features = FeaturePacket(
        ts=1700000001000000,
        symbol="SPY",
        tf="1s",
        vwap=450,
        vwap_bands={"1": (449, 451)},
        atr_1m=0.6,
        atr_1s=0.2,
        adx_3m=30,
        vwap_slope=0.02,
        rv_5m=0.2,
        rv_15m=0.3,
        iv_9d=0.22,
        iv_30d=0.24,
        iv_60d=0.25,
        skew_25d=-0.1,
        vol_of_vol=0.05,
        micro={
            "nbbo_age_ms": 100,
            "spread_pct": 0.004,
            "spread_state": "normal",
            "cvd_90s": 1500,
            "es_lead_agree": True,
        },
        prob={"p_itm": 0.5, "pot_est": 0.7},
    )
    gate = GateResult(allowed=True, regime_score=0.5, liquidity_score=1.0)
    assert choose_playbook(features, gate) == "TREND_PULLBACK"


def test_balance_playbook_selected_on_negative_regime():
    features = FeaturePacket(
        ts=1700000001000000,
        symbol="SPY",
        tf="1s",
        vwap=450,
        vwap_bands={"1": (449, 451)},
        atr_1m=0.6,
        atr_1s=0.2,
        adx_3m=15,
        vwap_slope=-0.02,
        rv_5m=0.2,
        rv_15m=0.3,
        iv_9d=0.22,
        iv_30d=0.24,
        iv_60d=0.25,
        skew_25d=-0.1,
        vol_of_vol=0.05,
        micro={
            "nbbo_age_ms": 100,
            "spread_pct": 0.004,
            "spread_state": "normal",
            "cvd_90s": -500,
            "es_lead_agree": True,
        },
        prob={"p_itm": 0.5, "pot_est": 0.7},
    )
    gate = GateResult(allowed=True, regime_score=-0.5, liquidity_score=1.0)
    assert choose_playbook(features, gate) == "BALANCE_FADE"
