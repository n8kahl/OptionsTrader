from datetime import datetime, timedelta, timezone

from services.risk.main import build_risk_manager
from services.risk.scheduler import EconScheduler


def test_econ_halt_blocks_entries():
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
    manager.state.pnl = 100
    manager.state.open_positions = 0
    now = datetime.now(tz=timezone.utc)
    manager.set_session_start(int((now - timedelta(minutes=5)).timestamp() * 1_000_000))

    release = now + timedelta(minutes=2)
    start, end = EconScheduler.build_window(release, config["econ_halt_minutes_pre_post"])
    scheduler = EconScheduler([(start, end, "CPI")])

    assert scheduler.is_halted(now + timedelta(minutes=1))
    allowed = manager.entry_allowed(int(now.timestamp() * 1_000_000), 2, 60)
    assert not allowed
