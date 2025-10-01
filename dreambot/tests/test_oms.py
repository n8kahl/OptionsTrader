import asyncio

from services.oms.main import OMSConfig, OMSService
from services.oms.schemas import OrderRequest


def test_otoco_lifecycle_and_stop_sync():
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    service = OMSService(config)
    request = OrderRequest(
        ts=1700000000000000,
        underlying="SPY",
        option_symbol="SPY2410C00450000",
        side="BUY",
        quantity=1,
        entry_price=1.5,
        target_price=1.7,
        stop_price=1.3,
        time_stop_secs=240,
        metadata={"playbook": "TREND_PULLBACK"},
    )
    status = asyncio.run(service.route_order(request))
    assert status.state == "filled"
    assert status.payload["payload"]["legs"][0]["order_type"] == "limit"

    adjusted = service.sync_stop(1.3, 452.0, "BUY")
    assert adjusted >= 1.3
