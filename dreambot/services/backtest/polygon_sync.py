"""Fetch Polygon flat files (aggregates) into local storage."""
from __future__ import annotations

import argparse
import gzip
import io
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List

import requests

try:  # pragma: no cover - optional dependency for offline tests
    import boto3
    from botocore.config import Config
except ImportError:  # pragma: no cover
    boto3 = None
    Config = None

DEFAULT_PREFIX = "us_stocks_sip/aggregates_1min_v1"


def generate_dates(days: int, *, end: date | None = None) -> List[str]:
    end_date = end or date.today()
    return [
        (end_date - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(days)
    ]


def fetch_temp_credentials(api_key: str, dataset: str) -> dict:
    dataset_path = dataset.strip("/")
    urls = [
        f"https://api.polygon.io/v1/flatfiles/{dataset_path}/aws_access_key",
        "https://api.polygon.io/v1/flatfiles/aws_access_key",
    ]
    last_error = None
    for url in urls:
        response = requests.get(url, params={"apiKey": api_key}, timeout=30)
        if response.status_code == 404 and url != urls[-1]:
            last_error = response.text
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"Failed to fetch credentials: {last_error}")


def build_s3_client(
    credentials: dict | None = None,
    *,
    access_key: str | None = None,
    secret_key: str | None = None,
    session_token: str | None = None,
    endpoint: str | None = None,
    region: str | None = None,
):
    if boto3 is None:
        raise RuntimeError("boto3 is required for polygon_sync but is not installed")
    if credentials is not None:
        creds = credentials["credentials"]
        access_key = creds["access_key_id"]
        secret_key = creds["secret_access_key"]
        session_token = creds.get("session_token")
        region = creds.get("region", "us-east-1")
        endpoint = (
            credentials.get("s3_endpoint")
            or credentials.get("aws_endpoint")
            or credentials.get("endpoint")
            or "https://files.polygon.io"
        )
    if not access_key or not secret_key:
        raise RuntimeError("Polygon S3 credentials not provided")
    session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region or "us-east-1",
    )
    kwargs = {
        "endpoint_url": endpoint or "https://files.polygon.io",
    }
    if Config is not None:
        kwargs["config"] = Config(signature_version="s3v4")
    return session.client("s3", **kwargs)


def download_file(s3_client, bucket: str, key: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    buffer = io.BytesIO()
    s3_client.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    if key.endswith(".gz"):
        data = gzip.decompress(buffer.getvalue())
        dest = dest.with_suffix("") if dest.suffix == ".gz" else dest
        dest.write_bytes(data)
    else:
        dest.write_bytes(buffer.getvalue())


def sync_flatfiles(
    api_key: str | None,
    symbols: Iterable[str],
    dest_dir: Path,
    *,
    days: int = 60,
    prefix: str = DEFAULT_PREFIX,
    table_suffix: str = ".csv.gz",
) -> List[Path]:
    bucket = os.environ.get("POLYGON_S3_BUCKET", "flatfiles")
    access_key = os.environ.get("POLYGON_S3_ACCESS_KEY")
    secret_key = os.environ.get("POLYGON_S3_SECRET_KEY")
    endpoint_override = os.environ.get("POLYGON_S3_ENDPOINT")
    if access_key and secret_key:
        s3_client = build_s3_client(
            credentials=None,
            access_key=access_key,
            secret_key=secret_key,
            endpoint=endpoint_override,
        )
    else:
        if not api_key:
            raise RuntimeError("POLYGON_API_KEY must be set when static S3 credentials are not provided")
        credentials = fetch_temp_credentials(api_key, prefix)
        bucket = credentials.get("bucket", bucket)
        s3_client = build_s3_client(credentials)
    downloaded: List[Path] = []
    for day_str in generate_dates(days):
        for symbol in symbols:
            key = f"{prefix}/{day_str}/{symbol.upper()}{table_suffix}"
            local_path = dest_dir / prefix / day_str / Path(key).name
            try:
                download_file(s3_client, bucket, key, local_path)
                downloaded.append(local_path)
                print(f"Downloaded {key} -> {local_path}")
            except Exception as exc:  # pragma: no cover - network interaction
                print(f"Warning: failed to download {key}: {exc}")
    return downloaded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Polygon flat files into local storage")
    parser.add_argument("--symbols", nargs="+", default=["SPY", "QQQ"], help="Symbols to fetch")
    parser.add_argument("--days", type=int, default=60, help="Number of trailing days to download")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="S3 prefix to fetch (e.g. stocks/aggregates)")
    parser.add_argument("--dest", default="data/flatfiles", help="Local destination root")
    parser.add_argument("--suffix", default=".csv.gz", help="Filename suffix in S3 (e.g. .csv.gz)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise SystemExit("POLYGON_API_KEY environment variable not set")
    dest = Path(args.dest)
    sync_flatfiles(
        api_key,
        args.symbols,
        dest,
        days=args.days,
        prefix=args.prefix.strip("/"),
        table_suffix=args.suffix,
    )


if __name__ == "__main__":
    main()
