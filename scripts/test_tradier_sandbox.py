"""Place and cancel a Tradier sandbox OTOCO order for verification."""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from dreambot.services.oms.main import OMSConfig, OMSService, load_broker_config
from dreambot.services.oms.schemas import OrderRequest


def load_sandbox_config() -> OMSConfig:
    base_config = load_broker_config()
    base_config = dict(base_config)
    base_config["provider"] = "tradier"
    base_config["paper"] = True
    base_config["tradier_duration"] = "day"
    base_config["tradier_access_token_env"] = os.environ.get("TRADIER_ACCESS_TOKEN_ENV", "TRADIER_SANDBOX_TOKEN")
    base_config["tradier_account_id_env"] = os.environ.get("TRADIER_ACCOUNT_ID_ENV", "TRADIER_SANDBOX_ACCOUNT")
    base_config.setdefault("tradier_paper_base_url", "https://sandbox.tradier.com/v1")
    return OMSConfig(**base_config)


def build_request(args: argparse.Namespace, ts: int) -> OrderRequest:
    metadata = {"playbook": "SANDBOX_TEST", "client_order_id": f"SANDBOX-{ts}"}
    return OrderRequest(
        ts=ts,
        underlying=args.underlying,
        option_symbol=args.option,
        side=args.side.upper(),
        quantity=args.quantity,
        entry_price=args.entry,
        target_price=args.target,
        stop_price=args.stop,
        time_stop_secs=args.time_stop,
        metadata=metadata,
    )


async def run_test(args: argparse.Namespace) -> None:
    missing = [var for var in ("TRADIER_SANDBOX_TOKEN", "TRADIER_SANDBOX_ACCOUNT") if not os.environ.get(var)]
    if missing:
        raise SystemExit(f"Set environment variables: {', '.join(missing)}")

    config = load_sandbox_config()
    service = OMSService(config)
    request = build_request(args, ts=args.ts)

    print("Submitting sandbox order...")
    status = await service.route_order(request)
    print("Initial status:", status.state, status.order_id)

    await asyncio.sleep(args.poll_interval)
    response = await service.broker.get_order(status.order_id)
    print("Polled status:", response.get("order", response).get("status"))

    print("Cancelling...")
    cancel_resp = await service.broker.cancel_order(status.order_id)
    cancel_status = cancel_resp.get("order", cancel_resp).get("status", "unknown")
    print("Cancel status:", cancel_status)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tradier sandbox smoke test")
    parser.add_argument("--underlying", default="SPY", help="Underlying symbol")
    parser.add_argument("--option", required=True, help="Option symbol (Tradier format)")
    parser.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--entry", type=float, default=0.05)
    parser.add_argument("--target", type=float, default=0.15)
    parser.add_argument("--stop", type=float, default=0.02)
    parser.add_argument("--time-stop", type=int, default=120)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--ts", type=int, default=1700000000000000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_test(args))


if __name__ == "__main__":
    main()
