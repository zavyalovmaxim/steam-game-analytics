from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime
from typing import Any

from dotenv import load_dotenv

from steam_game_analytics.ingestion.steam_app_details import (
    calculate_record_hash,
    normalize_app_details,
)
from steam_game_analytics.storage.clickhouse_storage import (
    ClickHouseStorage,
)
from steam_game_analytics.storage.minio_storage import MinioStorage


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "Date must use YYYY-MM-DD format",
        ) from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load Steam application details from MinIO raw "
            "storage into ClickHouse ODS."
        ),
    )

    parser.add_argument(
        "--load-date",
        required=True,
        type=parse_iso_date,
        help="Raw MinIO partition date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def create_minio_storage() -> MinioStorage:
    return MinioStorage(
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        bucket_name=os.environ["MINIO_BUCKET_RAW"],
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def create_clickhouse_storage() -> ClickHouseStorage:
    return ClickHouseStorage(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


def parse_raw_document(
    document: dict[str, Any],
) -> tuple[int, dict[str, Any], datetime]:
    app_id = int(document["app_id"])

    data = document["data"]
    if not isinstance(data, dict):
        raise TypeError(
            f"Field 'data' must be an object for app_id={app_id}",
        )

    fetched_at_raw = document["fetched_at"]
    if not isinstance(fetched_at_raw, str):
        raise TypeError(
            f"Field 'fetched_at' must be a string for app_id={app_id}",
        )

    fetched_at = datetime.fromisoformat(fetched_at_raw)

    return app_id, data, fetched_at


def process_object(
    object_key: str,
    minio: MinioStorage,
    clickhouse: ClickHouseStorage,
) -> str:
    document = minio.get_json(object_key)

    app_id, data, fetched_at = parse_raw_document(document)

    row = normalize_app_details(
        app_id=app_id,
        data=data,
        fetched_at=fetched_at,
    )

    row["record_hash"] = calculate_record_hash(row)

    latest_hash = clickhouse.get_latest_record_hash(app_id)

    if latest_hash == row["record_hash"]:
        logging.info(
            "No business changes for app_id=%s; insert skipped",
            app_id,
        )
        return "unchanged"

    clickhouse.insert_app(row)

    logging.info(
        "Inserted new version for app_id=%s "
        "record_hash=%s source=s3://%s/%s",
        app_id,
        row["record_hash"],
        minio.bucket_name,
        object_key,
    )

    return "inserted"


def main() -> int:
    load_dotenv()
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    minio = create_minio_storage()
    clickhouse = create_clickhouse_storage()

    prefix = (
        "steam/app_details/"
        f"load_date={args.load_date.isoformat()}/"
    )

    object_keys = sorted(
        minio.list_json_objects(prefix=prefix),
    )

    if not object_keys:
        logging.warning(
            "No JSON objects found under s3://%s/%s",
            minio.bucket_name,
            prefix,
        )
        return 0

    logging.info(
        "Starting ODS load: objects=%s prefix=s3://%s/%s",
        len(object_keys),
        minio.bucket_name,
        prefix,
    )

    inserted_count = 0
    unchanged_count = 0
    failed_count = 0

    for object_key in object_keys:
        try:
            status = process_object(
                object_key=object_key,
                minio=minio,
                clickhouse=clickhouse,
            )

            if status == "inserted":
                inserted_count += 1
            else:
                unchanged_count += 1

        except (KeyError, TypeError, ValueError):
            failed_count += 1
            logging.exception(
                "Invalid raw object: s3://%s/%s",
                minio.bucket_name,
                object_key,
            )

        except Exception:
            failed_count += 1
            logging.exception(
                "Failed to process raw object: s3://%s/%s",
                minio.bucket_name,
                object_key,
            )

    logging.info(
        "ODS load finished: inserted=%s unchanged=%s failed=%s",
        inserted_count,
        unchanged_count,
        failed_count,
    )

    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())