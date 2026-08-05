from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import UTC, date, datetime
from typing import Any

import requests
from dotenv import load_dotenv

from steam_game_analytics.storage.minio_storage import MinioStorage


STEAM_APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails"

DEFAULT_APP_IDS = [
    570,      # Dota 2
    730,      # Counter-Strike 2
    440,      # Team Fortress 2
    578080,   # PUBG
    1091500,  # Cyberpunk 2077
]

REQUEST_TIMEOUT_SECONDS = 30
REQUEST_DELAY_SECONDS = 1.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Steam app details into the MinIO raw layer.",
    )

    parser.add_argument(
        "--app-id",
        dest="app_ids",
        action="append",
        type=int,
        help=(
            "Steam application ID. May be specified multiple times. "
            "The default test application list is used when omitted."
        ),
    )

    parser.add_argument(
        "--country-code",
        default="us",
        help="Country code passed to Steam Store API. Default: us.",
    )

    parser.add_argument(
        "--language",
        default="english",
        help="Response language passed to Steam Store API. Default: english.",
    )

    parser.add_argument(
        "--load-date",
        type=parse_iso_date,
        default=None,
        help="Raw MinIO partition date in YYYY-MM-DD format.",
    )

    return parser.parse_args()

def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "Date must use YYYY-MM-DD format",
        ) from error


def fetch_app_details(
    session: requests.Session,
    app_id: int,
    country_code: str,
    language: str,
) -> dict[str, Any]:
    response = session.get(
        STEAM_APP_DETAILS_URL,
        params={
            "appids": app_id,
            "cc": country_code,
            "l": language,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    payload = response.json()
    app_payload = payload.get(str(app_id))

    if app_payload is None:
        raise ValueError(
            f"Steam returned no response object for app_id={app_id}",
        )

    if not app_payload.get("success", False):
        raise ValueError(
            f"Steam returned success=false for app_id={app_id}",
        )

    data = app_payload.get("data")

    if not isinstance(data, dict):
        raise ValueError(
            f"Steam returned invalid data for app_id={app_id}",
        )

    return data


def build_raw_document(
    app_id: int,
    data: dict[str, Any],
    load_date: date | None = None,
) -> tuple[str, dict[str, Any]]:
    fetched_at = datetime.now(UTC)
    partition_date = load_date or fetched_at.date()
    file_timestamp = fetched_at.strftime("%Y%m%dT%H%M%S%fZ")

    object_key = (
        "steam/app_details/"
        f"load_date={partition_date.isoformat()}/"
        f"app_id={app_id}/"
        f"{file_timestamp}.json"
    )

    document = {
        "source": "steam_store_api",
        "entity": "app_details",
        "app_id": app_id,
        "fetched_at": fetched_at.isoformat(),
        "load_date": partition_date.isoformat(),
        "schema_version": 1,
        "data": data,
    }

    return object_key, document


def create_minio_storage() -> MinioStorage:
    return MinioStorage(
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        bucket_name=os.environ["MINIO_BUCKET_RAW"],
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def main() -> int:
    load_dotenv()
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    app_ids = args.app_ids or DEFAULT_APP_IDS

    storage = create_minio_storage()
    storage.ensure_bucket_exists()

    logging.info(
        "Starting Steam ingestion: applications=%s bucket=%s",
        len(app_ids),
        storage.bucket_name,
    )

    success_count = 0
    failure_count = 0

    with requests.Session() as session:
        session.headers.update(
            {
                "User-Agent": (
                    "steam-game-analytics/0.1 "
                    "(educational data engineering project)"
                ),
                "Accept": "application/json",
            },
        )

        for index, app_id in enumerate(app_ids):
            try:
                logging.info("Fetching app_id=%s", app_id)

                data = fetch_app_details(
                    session=session,
                    app_id=app_id,
                    country_code=args.country_code,
                    language=args.language,
                )

                object_key, document = build_raw_document(
                    app_id=app_id,
                    data=data,
                    load_date=args.load_date,
                )

                storage.put_json(
                    object_key=object_key,
                    payload=document,
                )

                success_count += 1

                logging.info(
                    "Saved app_id=%s to s3://%s/%s",
                    app_id,
                    storage.bucket_name,
                    object_key,
                )

            except requests.RequestException:
                failure_count += 1
                logging.exception(
                    "Steam HTTP request failed for app_id=%s",
                    app_id,
                )

            except (ValueError, TypeError, KeyError):
                failure_count += 1
                logging.exception(
                    "Invalid Steam response for app_id=%s",
                    app_id,
                )

            except Exception:
                failure_count += 1
                logging.exception(
                    "Unexpected ingestion error for app_id=%s",
                    app_id,
                )

            if index < len(app_ids) - 1:
                time.sleep(REQUEST_DELAY_SECONDS)

    logging.info(
        "Steam ingestion finished: success=%s failure=%s",
        success_count,
        failure_count,
    )

    result = {
        "extracted_rows": success_count,
        "source_file_count": success_count,
        "failed_rows": failure_count,
    }
    print(
        "PIPELINE_RESULT="
        + json.dumps(result, separators=(",", ":"), sort_keys=True),
        flush=True,
    )

    return 1 if failure_count else 0


if __name__ == "__main__":
    raise SystemExit(main())