from __future__ import annotations

import argparse
import json
import logging
import os
import re
from datetime import UTC, date, datetime
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv

from steam_game_analytics.storage.minio_storage import MinioStorage


COLUMNS = [
    "app_id",
    "player_count",
    "source",
    "observed_at",
    "load_date",
    "run_id",
    "source_object_key",
]


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
        description="Load Steam current players from MinIO into ClickHouse ODS.",
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


def create_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


def parse_document(
    document: dict[str, Any],
    *,
    expected_load_date: date,
    expected_run_id: str,
    object_key: str,
) -> list[Any]:
    app_id = int(document["app_id"])
    player_count = int(document["player_count"])
    source = str(document["source"])
    run_id = str(document["run_id"])
    document_load_date = date.fromisoformat(str(document["load_date"]))
    observed_at = datetime.fromisoformat(str(document["observed_at"]))

    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    else:
        observed_at = observed_at.astimezone(UTC)

    if app_id <= 0:
        raise ValueError("app_id must be greater than zero")
    if player_count < 0:
        raise ValueError("player_count must be non-negative")
    if run_id != expected_run_id:
        raise ValueError(
            f"Unexpected run_id={run_id!r}; expected {expected_run_id!r}",
        )
    if document_load_date != expected_load_date:
        raise ValueError(
            f"Unexpected load_date={document_load_date}; "
            f"expected {expected_load_date}",
        )

    return [
        app_id,
        player_count,
        source,
        observed_at,
        document_load_date,
        run_id,
        object_key,
    ]


def event_exists(
    client,
    *,
    app_id: int,
    observed_at: datetime,
    run_id: str,
) -> bool:
    result = client.query(
        """
        SELECT count()
        FROM ods.steam_current_players
        WHERE app_id = {app_id:UInt32}
          AND observed_at = {observed_at:DateTime64(6, 'UTC')}
          AND run_id = {run_id:String}
        """,
        parameters={
            "app_id": app_id,
            "observed_at": observed_at,
            "run_id": run_id,
        },
    )
    return int(result.result_rows[0][0]) > 0


def print_pipeline_result(
    *,
    loaded_rows: int,
    skipped_rows: int,
    failed_rows: int,
    source_file_count: int,
) -> None:
    result = {
        "loaded_rows": loaded_rows,
        "skipped_rows": skipped_rows,
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

    minio = create_minio_storage()
    client = create_clickhouse_client()

    prefix = (
        "steam/current_players/"
        f"load_date={args.load_date.isoformat()}/"
        f"run_id={args.run_id}/"
    )
    object_keys = sorted(minio.list_json_objects(prefix=prefix))

    rows: list[list[Any]] = []
    skipped_count = 0
    failed_count = 0

    for object_key in object_keys:
        try:
            row = parse_document(
                minio.get_json(object_key),
                expected_load_date=args.load_date,
                expected_run_id=args.run_id,
                object_key=object_key,
            )
            if event_exists(
                client,
                app_id=row[0],
                observed_at=row[3],
                run_id=row[5],
            ):
                skipped_count += 1
                continue
            rows.append(row)
        except Exception:
            failed_count += 1
            logging.exception(
                "Failed to process raw object s3://%s/%s",
                minio.bucket_name,
                object_key,
            )

    if rows:
        client.insert(
            table="ods.steam_current_players",
            data=rows,
            column_names=COLUMNS,
        )

    print_pipeline_result(
        loaded_rows=len(rows),
        skipped_rows=skipped_count,
        failed_rows=failed_count,
        source_file_count=len(object_keys),
    )
    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())