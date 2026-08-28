from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from datetime import UTC, date, datetime
from typing import Any

import requests
from dotenv import load_dotenv

from steam_game_analytics.storage.minio_storage import MinioStorage


STEAM_CURRENT_PLAYERS_URL = (
    "https://api.steampowered.com/"
    "ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
)

DEFAULT_APP_IDS = [570, 730, 440, 578080, 1091500]
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_DELAY_SECONDS = 0.5


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "Date must use YYYY-MM-DD format",
        ) from error


def sanitize_run_id(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")
    if not sanitized:
        raise argparse.ArgumentTypeError("run-id must not be empty")
    return sanitized[:200]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Steam current-player observations into MinIO raw.",
    )
    parser.add_argument(
        "--app-id",
        dest="app_ids",
        action="append",
        type=int,
        help="Steam app ID. May be specified multiple times.",
    )
    parser.add_argument("--load-date", required=True, type=parse_iso_date)
    parser.add_argument("--run-id", required=True, type=sanitize_run_id)
    return parser.parse_args()


def create_minio_storage() -> MinioStorage:
    return MinioStorage(
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        bucket_name=os.environ["MINIO_BUCKET_RAW"],
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def fetch_current_players(
    session: requests.Session,
    app_id: int,
) -> int:
    response = session.get(
        STEAM_CURRENT_PLAYERS_URL,
        params={"appid": app_id},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    payload = response.json()
    response_data = payload.get("response")

    if not isinstance(response_data, dict):
        raise ValueError(
            f"Steam returned invalid response object for app_id={app_id}",
        )

    if int(response_data.get("result", 0)) != 1:
        raise ValueError(
            f"Steam returned result != 1 for app_id={app_id}",
        )

    player_count = response_data.get("player_count")
    if not isinstance(player_count, int) or player_count < 0:
        raise ValueError(
            f"Steam returned invalid player_count for app_id={app_id}",
        )

    return player_count


def build_raw_document(
    *,
    app_id: int,
    player_count: int,
    load_date: date,
    run_id: str,
) -> tuple[str, dict[str, Any]]:
    observed_at = datetime.now(UTC)
    timestamp = observed_at.strftime("%Y%m%dT%H%M%S%fZ")

    object_key = (
        "steam/current_players/"
        f"load_date={load_date.isoformat()}/"
        f"run_id={run_id}/"
        f"app_id={app_id}/"
        f"{timestamp}.json"
    )

    document = {
        "source": "steam_web_api",
        "entity": "current_players",
        "app_id": app_id,
        "player_count": player_count,
        "observed_at": observed_at.isoformat(),
        "load_date": load_date.isoformat(),
        "run_id": run_id,
        "schema_version": 1,
    }

    return object_key, document


def print_pipeline_result(
    *,
    extracted_rows: int,
    failed_rows: int,
    source_file_count: int,
) -> None:
    result = {
        "extracted_rows": extracted_rows,
        "failed_rows": failed_rows,
        "source_file_count": source_file_count,
    }
    print(
        "PIPELINE_RESULT="
        + json.dumps(result, separators=(",", ":"), sort_keys=True),
        flush=True,
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
            }
        )

        for index, app_id in enumerate(app_ids):
            try:
                player_count = fetch_current_players(session, app_id)
                object_key, document = build_raw_document(
                    app_id=app_id,
                    player_count=player_count,
                    load_date=args.load_date,
                    run_id=args.run_id,
                )
                storage.put_json(object_key=object_key, payload=document)
                success_count += 1
                logging.info(
                    "Saved app_id=%s player_count=%s to s3://%s/%s",
                    app_id,
                    player_count,
                    storage.bucket_name,
                    object_key,
                )
            except Exception:
                failure_count += 1
                logging.exception(
                    "Failed to ingest current players for app_id=%s",
                    app_id,
                )

            if index < len(app_ids) - 1:
                time.sleep(REQUEST_DELAY_SECONDS)

    print_pipeline_result(
        extracted_rows=success_count,
        failed_rows=failure_count,
        source_file_count=success_count,
    )
    return 1 if failure_count else 0


if __name__ == "__main__":
    raise SystemExit(main())