from services.risk.main import build_risk_manager


def test_kill_switch_triggers_flatten():
    config = {
        "daily_loss_cap": -500,
        "per_trade_max_risk_pct": 0.7,
        "max_concurrent_positions": 2,
        "no_trade_first_seconds": 90,
        "econ_halt_minutes_pre_post": 3,
        "force_flat_before_close_secs": 180,
        "defensive_mode": {"slippage_z": 2.0, "spread_z": 2.0},
    }
    manager = build_risk_manager(config)
    manager.state.pnl = -600
    assert manager.enforce_exit()
    manager.state.pnl = 100
    assert not manager.enforce_exit()
