from services.dashboard.metrics import (
    HeartbeatSnapshot,
    OMSMetric,
    parse_heartbeat,
    parse_oms_metric,
    summarize_oms,
)


def test_parse_heartbeat_and_delay():
    payload = {
        "ts": 1700000000000000,
        "mode": "live",
        "quotes": {"age_ms": 25.5, "count": 500},
        "aggs": {"age_ms": 30.1, "count": 100},
        "option_meta": {"age_ms": None, "count": 5},
    }
    hb = parse_heartbeat(payload)
    assert isinstance(hb, HeartbeatSnapshot)
    assert hb.delay_ms == 30.1
    row = hb.to_row()
    assert row["mode"] == "live"


def test_parse_oms_metric_and_summary():
    payload = {
        "ts": 1700000001000000,
        "order_id": "ORD-1",
        "client_order_id": "CID-1",
        "state": "filled",
        "latency_ms": 120.5,
        "quantity": 2,
        "filled_qty": 2,
        "avg_fill_price": 1.52,
    }
    metric = parse_oms_metric(payload)
    assert isinstance(metric, OMSMetric)
    summary = summarize_oms([metric])
    assert summary["total"] == 1
    assert summary["filled"] == 1
    assert summary["avg_latency_ms"] == 120.5
