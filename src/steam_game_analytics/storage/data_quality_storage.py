from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import clickhouse_connect


@dataclass(frozen=True)
class DataQualityCheck:
    name: str
    check_type: str
    passed: bool
    actual_value: str
    expected_value: str
    error_message: str | None = None


class DataQualityStorage:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
    ) -> None:
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
        )

    def scalar(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> int:
        result = self.client.query(
            query,
            parameters=parameters or {},
        )

        if not result.result_rows:
            raise RuntimeError("Data quality query returned no rows")

        return int(result.result_rows[0][0])

    def insert_results(
        self,
        *,
        pipeline_name: str,
        run_id: str,
        load_date: date,
        checks: list[DataQualityCheck],
    ) -> None:
        rows = [
            [
                pipeline_name,
                run_id,
                load_date,
                check.name,
                check.check_type,
                "PASSED" if check.passed else "FAILED",
                check.actual_value,
                check.expected_value,
                check.error_message,
            ]
            for check in checks
        ]

        self.client.insert(
            table="metadata.data_quality_results",
            data=rows,
            column_names=[
                "pipeline_name",
                "run_id",
                "load_date",
                "check_name",
                "check_type",
                "status",
                "actual_value",
                "expected_value",
                "error_message",
            ],
        )