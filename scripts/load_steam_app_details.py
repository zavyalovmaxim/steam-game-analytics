from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import UTC, date, datetime
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


def normalize_utc_datetime(value: datetime) -> datetime:
    """
    Convert datetime to a timezone-aware UTC datetime.

    ClickHouse may return a timezone-naive datetime even when the stored
    timestamp semantically represents UTC. Raw JSON timestamps are normally
    timezone-aware because they contain +00:00.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)

    return value.astimezone(UTC)


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

    try:
        fetched_at = datetime.fromisoformat(fetched_at_raw)
    except ValueError as error:
        raise ValueError(
            f"Invalid fetched_at for app_id={app_id}: "
            f"{fetched_at_raw!r}",
        ) from error

    return app_id, data, normalize_utc_datetime(fetched_at)


def process_object(
    object_key: str,
    minio: MinioStorage,
    clickhouse: ClickHouseStorage,
    latest_states: dict[int, tuple[int, datetime] | None],
) -> str:
    document = minio.get_json(object_key)

    app_id, data, fetched_at = parse_raw_document(document)

    if app_id not in latest_states:
        latest_state = clickhouse.get_latest_record_state(app_id)

        if latest_state is None:
            latest_states[app_id] = None
        else:
            latest_hash, latest_fetched_at = latest_state

            latest_states[app_id] = (
                int(latest_hash),
                normalize_utc_datetime(latest_fetched_at),
            )

    latest_state = latest_states[app_id]

    if latest_state is not None:
        latest_hash, latest_fetched_at = latest_state

        # The loader reads the whole daily MinIO partition.
        # Files that are not newer than the latest processed state must
        # never replay historical transitions.
        if fetched_at <= latest_fetched_at:
            logging.info(
                "Raw snapshot already processed for app_id=%s: "
                "fetched_at=%s latest_fetched_at=%s; skipped",
                app_id,
                fetched_at.isoformat(),
                latest_fetched_at.isoformat(),
            )
            return "already_processed"
    else:
        latest_hash = None

    row = normalize_app_details(
        app_id=app_id,
        data=data,
        fetched_at=fetched_at,
    )

    current_hash = int(calculate_record_hash(row))
    row["record_hash"] = current_hash

    if latest_hash == current_hash:
        # Business fields did not change. We do not insert a new ODS row,
        # but advance the in-memory watermark for the current loader run.
        latest_states[app_id] = (
            current_hash,
            fetched_at,
        )

        logging.info(
            "No business changes for app_id=%s: "
            "record_hash=%s; insert skipped",
            app_id,
            current_hash,
        )

        return "unchanged"

    clickhouse.insert_app(row)

    latest_states[app_id] = (
        current_hash,
        fetched_at,
    )

    logging.info(
        "Inserted new version for app_id=%s "
        "record_hash=%s source=s3://%s/%s",
        app_id,
        current_hash,
        minio.bucket_name,
        object_key,
    )

    return "inserted"


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
        + json.dumps(
            result,
            separators=(",", ":"),
            sort_keys=True,
        ),
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

        print_pipeline_result(
            loaded_rows=0,
            skipped_rows=0,
            failed_rows=0,
            source_file_count=0,
        )

        return 0

    logging.info(
        "Starting ODS load: objects=%s prefix=s3://%s/%s",
        len(object_keys),
        minio.bucket_name,
        prefix,
    )

    inserted_count = 0
    skipped_count = 0
    failed_count = 0

    # Stores the latest known state for each app during the current run.
    # It prevents repeated ClickHouse queries and duplicate transitions
    # when several raw files for one app are processed sequentially.
    latest_states: dict[
        int,
        tuple[int, datetime] | None,
    ] = {}

    for object_key in object_keys:
        try:
            status = process_object(
                object_key=object_key,
                minio=minio,
                clickhouse=clickhouse,
                latest_states=latest_states,
            )

            if status == "inserted":
                inserted_count += 1
            else:
                skipped_count += 1

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
        "ODS load finished: inserted=%s skipped=%s failed=%s",
        inserted_count,
        skipped_count,
        failed_count,
    )

    print_pipeline_result(
        loaded_rows=inserted_count,
        skipped_rows=skipped_count,
        failed_rows=failed_count,
        source_file_count=len(object_keys),
    )

    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())