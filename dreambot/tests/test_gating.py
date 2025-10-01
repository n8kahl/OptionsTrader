from services.features.schemas import FeaturePacket
from services.signals.gating import evaluate_gates


def make_features(nbbo_age: float, spread_pct: float, spread_state: str, pot: float = 0.7) -> FeaturePacket:
    return FeaturePacket(
        ts=1700000000000000,
        symbol="SPY",
        tf="1s",
        vwap=450.0,
        vwap_bands={"1": (449.5, 450.5)},
        atr_1m=0.5,
        atr_1s=0.2,
        adx_3m=30.0,
        vwap_slope=0.01,
        rv_5m=0.2,
        rv_15m=0.3,
        iv_9d=0.22,
        iv_30d=0.24,
        iv_60d=0.25,
        skew_25d=-0.1,
        vol_of_vol=0.05,
        micro={
            "nbbo_age_ms": nbbo_age,
            "spread_pct": spread_pct,
            "spread_state": spread_state,
            "cvd_90s": 1000,
            "es_lead_agree": True,
        },
        prob={"p_itm": 0.5, "pot_est": pot},
    )


def test_gating_blocks_on_stale_or_stressed():
    config = {
        "nbbo_age_ms_max": 800,
        "spread_pct_max": 0.01,
        "trend_threshold": -0.2,
        "adx_threshold": 20,
        "pot_threshold": 0.55,
    }
    stale = evaluate_gates(make_features(900, 0.005, "normal"), config)
    tight = evaluate_gates(make_features(200, 0.02, "normal"), config)
    stressed = evaluate_gates(make_features(200, 0.005, "stressed"), config)

    assert not stale.allowed
    assert not tight.allowed
    assert not stressed.allowed

    ok = evaluate_gates(make_features(200, 0.005, "normal"), config)
    assert ok.allowed
