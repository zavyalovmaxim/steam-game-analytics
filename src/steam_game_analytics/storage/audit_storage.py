from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Final
import json
import requests


PIPELINE_RUNS_TABLE: Final = "metadata.pipeline_runs"

VALID_STATUSES: Final[set[str]] = {
    "RUNNING",
    "SUCCESS",
    "FAILED",
}


class AuditStorageError(RuntimeError):
    """Raised when pipeline audit data cannot be written to ClickHouse."""


class AuditStorage:
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.username = username or os.getenv("CLICKHOUSE_USER", "default")
        self.password = password or os.getenv("CLICKHOUSE_PASSWORD", "")
        self.database = "metadata"
        self.timeout_seconds = timeout_seconds

    @property
    def endpoint(self) -> str:
        return f"http://{self.host}:{self.port}/"

    def start_pipeline_run(
        self,
        *,
        pipeline_name: str,
        run_id: str,
        load_date: date,
        started_at: datetime | None = None,
    ) -> None:
        event_time = self._normalize_datetime(
            started_at or datetime.now(timezone.utc)
        )

        self._insert_run(
            pipeline_name=pipeline_name,
            run_id=run_id,
            load_date=load_date,
            started_at=event_time,
            finished_at=None,
            status="RUNNING",
            extracted_rows=0,
            loaded_rows=0,
            skipped_rows=0,
            failed_rows=0,
            source_file_count=0,
            error_message=None,
        )

    def finish_pipeline_run(
        self,
        *,
        pipeline_name: str,
        run_id: str,
        load_date: date,
        started_at: datetime,
        status: str,
        extracted_rows: int = 0,
        loaded_rows: int = 0,
        skipped_rows: int = 0,
        failed_rows: int = 0,
        source_file_count: int = 0,
        error_message: str | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        normalized_status = status.upper()

        if normalized_status not in VALID_STATUSES:
            raise ValueError(
                f"Unsupported pipeline status: {status}. "
                f"Expected one of {sorted(VALID_STATUSES)}"
            )

        if normalized_status == "RUNNING":
            raise ValueError(
                "finish_pipeline_run cannot be called with RUNNING status"
            )

        self._insert_run(
            pipeline_name=pipeline_name,
            run_id=run_id,
            load_date=load_date,
            started_at=self._normalize_datetime(started_at),
            finished_at=self._normalize_datetime(
                finished_at or datetime.now(timezone.utc)
            ),
            status=normalized_status,
            extracted_rows=self._validate_counter(
                "extracted_rows", extracted_rows
            ),
            loaded_rows=self._validate_counter(
                "loaded_rows", loaded_rows
            ),
            skipped_rows=self._validate_counter(
                "skipped_rows", skipped_rows
            ),
            failed_rows=self._validate_counter(
                "failed_rows", failed_rows
            ),
            source_file_count=self._validate_counter(
                "source_file_count", source_file_count
            ),
            error_message=self._truncate_error(error_message),
        )

    def _insert_run(
        self,
        *,
        pipeline_name: str,
        run_id: str,
        load_date: date,
        started_at: datetime,
        finished_at: datetime | None,
        status: str,
        extracted_rows: int,
        loaded_rows: int,
        skipped_rows: int,
        failed_rows: int,
        source_file_count: int,
        error_message: str | None,
    ) -> None:
        if not pipeline_name.strip():
            raise ValueError("pipeline_name must not be empty")

        if not run_id.strip():
            raise ValueError("run_id must not be empty")

        query = f"""
            INSERT INTO {PIPELINE_RUNS_TABLE}
            (
                pipeline_name,
                run_id,
                load_date,
                started_at,
                finished_at,
                status,
                extracted_rows,
                loaded_rows,
                skipped_rows,
                failed_rows,
                source_file_count,
                error_message,
                updated_at
            )
            FORMAT JSONEachRow
        """

        row = {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "load_date": load_date.isoformat(),
            "started_at": self._format_datetime(started_at),
            "finished_at": (
                self._format_datetime(finished_at)
                if finished_at is not None
                else None
            ),
            "status": status,
            "extracted_rows": extracted_rows,
            "loaded_rows": loaded_rows,
            "skipped_rows": skipped_rows,
            "failed_rows": failed_rows,
            "source_file_count": source_file_count,
            "error_message": error_message,
            "updated_at": self._format_datetime(
                datetime.now(timezone.utc)
            ),
        }

        try:
            response = requests.post(
                self.endpoint,
                params={
                    "database": self.database,
                    "query": query,
                },
                auth=(self.username, self.password),
                data=json.dumps(row, ensure_ascii=False, default=str) + "\n",
                headers={
                    "Content-Type": "application/x-ndjson",
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            response_text = getattr(exc.response, "text", None)

            details = (
                f" ClickHouse response: {response_text.strip()}"
                if response_text
                else ""
            )

            raise AuditStorageError(
                f"Failed to write audit event for "
                f"pipeline={pipeline_name}, run_id={run_id}.{details}"
            ) from exc

    @staticmethod
    def _normalize_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Audit datetime must be timezone-aware")

        return value.astimezone(timezone.utc)

    @staticmethod
    def _format_datetime(value: datetime) -> str:
        return value.isoformat(timespec="microseconds")

    @staticmethod
    def _validate_counter(name: str, value: int) -> int:
        if value < 0:
            raise ValueError(f"{name} must be greater than or equal to zero")

        return value

    @staticmethod
    def _truncate_error(
        error_message: str | None,
        max_length: int = 10_000,
    ) -> str | None:
        if error_message is None:
            return None

        return error_message[:max_length]