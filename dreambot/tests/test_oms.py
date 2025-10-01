import asyncio

from services.oms.main import OMSConfig, OMSService


def test_otoco_lifecycle_and_stop_sync():
    config = OMSConfig(
        paper=True,
        order_type="marketable_limit",
        use_otoco=True,
        default_limit_offset_ticks=0.05,
        modify_stop_on_underlying=True,
    )
    service = OMSService(config)
    signal = {"side": "BUY"}
    response = asyncio.run(service.route_signal(signal, "SPY2410C00450000", 1.5, 1.7, 1.3, quantity=1))
    assert response["status"] == "filled"
    assert response["payload"]["legs"][0]["order_type"] == "limit"

    adjusted = service.sync_stop(1.3, 452.0, "BUY")
    assert adjusted >= 1.3
