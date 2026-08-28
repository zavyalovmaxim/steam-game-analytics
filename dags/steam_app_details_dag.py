from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


PROJECT_DIR = "/opt/airflow/project"
PROJECT_SRC_DIR = str(Path(PROJECT_DIR) / "src")
PIPELINE_NAME = "steam_app_details_pipeline"
RESULT_PREFIX = "PIPELINE_RESULT="

if PROJECT_SRC_DIR not in sys.path:
    sys.path.insert(0, PROJECT_SRC_DIR)

from steam_game_analytics.storage.audit_storage import AuditStorage
from steam_game_analytics.storage.data_quality_storage import (
    DataQualityCheck,
    DataQualityStorage,
)


def _get_audit_context(context: dict[str, Any]) -> tuple[str, Any, Any]:
    dag_run = context["dag_run"]
    logical_date = context["logical_date"]

    load_date = logical_date.in_timezone("Europe/Moscow").date()
    started_at = dag_run.start_date or logical_date

    return dag_run.run_id, load_date, started_at


def _parse_pipeline_result(raw_value: Any, task_id: str) -> dict[str, int]:
    if not isinstance(raw_value, str):
        raise ValueError(
            f"Task {task_id} returned no string result through XCom: "
            f"{raw_value!r}"
        )

    if not raw_value.startswith(RESULT_PREFIX):
        raise ValueError(
            f"Task {task_id} returned an invalid result: {raw_value!r}"
        )

    try:
        payload = json.loads(raw_value.removeprefix(RESULT_PREFIX))
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Task {task_id} returned invalid JSON: {raw_value!r}"
        ) from error

    if not isinstance(payload, dict):
        raise ValueError(f"Task {task_id} result must be a JSON object")

    result: dict[str, int] = {}
    for key, value in payload.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(
                f"Task {task_id} metric {key!r} must be a non-negative integer"
            )
        result[key] = value

    return result


def _pull_metrics(context: dict[str, Any], task_id: str) -> dict[str, int]:
    task_instance = context["task_instance"]
    raw_value = task_instance.xcom_pull(task_ids=task_id)
    return _parse_pipeline_result(raw_value, task_id)


def start_pipeline_audit(**context: Any) -> None:
    run_id, load_date, started_at = _get_audit_context(context)

    AuditStorage().start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        load_date=load_date,
        started_at=started_at,
    )



def _create_data_quality_storage() -> DataQualityStorage:
    return DataQualityStorage(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


def validate_steam_app_details(**context: Any) -> None:
    run_id, load_date, _ = _get_audit_context(context)

    extract_metrics = _pull_metrics(
        context,
        "ingest_steam_apps_to_minio",
    )
    load_metrics = _pull_metrics(
        context,
        "load_steam_app_details_to_ods",
    )

    extracted_rows = extract_metrics.get("extracted_rows", 0)
    extract_failed_rows = extract_metrics.get("failed_rows", 0)
    loaded_rows = load_metrics.get("loaded_rows", 0)
    skipped_rows = load_metrics.get("skipped_rows", 0)
    load_failed_rows = load_metrics.get("failed_rows", 0)
    source_file_count = load_metrics.get("source_file_count", 0)

    storage = _create_data_quality_storage()
    checks: list[DataQualityCheck] = []

    def add_check(
        *,
        name: str,
        check_type: str,
        passed: bool,
        actual: Any,
        expected: Any,
        error_message: str | None = None,
    ) -> None:
        checks.append(
            DataQualityCheck(
                name=name,
                check_type=check_type,
                passed=passed,
                actual_value=str(actual),
                expected_value=str(expected),
                error_message=error_message,
            )
        )

    add_check(
        name="extract_returned_rows",
        check_type="METRIC",
        passed=extracted_rows > 0,
        actual=extracted_rows,
        expected="> 0",
        error_message=(
            None if extracted_rows > 0
            else "Steam extraction returned no successful rows"
        ),
    )

    total_failed_rows = extract_failed_rows + load_failed_rows
    add_check(
        name="pipeline_failed_rows",
        check_type="METRIC",
        passed=total_failed_rows == 0,
        actual=total_failed_rows,
        expected=0,
        error_message=(
            None if total_failed_rows == 0
            else "Extract or ODS load contains failed rows"
        ),
    )

    reconciled_rows = loaded_rows + skipped_rows + load_failed_rows
    add_check(
        name="ods_row_reconciliation",
        check_type="METRIC",
        passed=reconciled_rows == source_file_count,
        actual=reconciled_rows,
        expected=source_file_count,
        error_message=(
            None if reconciled_rows == source_file_count
            else "loaded + skipped + failed does not equal source file count"
        ),
    )

    date_params = {"load_date": load_date.isoformat()}

    invalid_app_ids = storage.scalar(
        """
        SELECT count()
        FROM ods.steam_app_details
        WHERE toDate(fetched_at, 'Europe/Moscow')
              = {load_date:Date}
          AND app_id = 0
        """,
        date_params,
    )
    add_check(
        name="valid_app_id",
        check_type="SQL",
        passed=invalid_app_ids == 0,
        actual=invalid_app_ids,
        expected=0,
        error_message=(
            None if invalid_app_ids == 0
            else "ODS contains rows with app_id = 0"
        ),
    )

    invalid_hashes = storage.scalar(
        """
        SELECT count()
        FROM ods.steam_app_details
        WHERE toDate(fetched_at, 'Europe/Moscow')
              = {load_date:Date}
          AND record_hash = 0
        """,
        date_params,
    )
    add_check(
        name="valid_record_hash",
        check_type="SQL",
        passed=invalid_hashes == 0,
        actual=invalid_hashes,
        expected=0,
        error_message=(
            None if invalid_hashes == 0
            else "ODS contains rows with record_hash = 0"
        ),
    )

    invalid_discounts = storage.scalar(
        """
        SELECT count()
        FROM ods.steam_app_details
        WHERE toDate(fetched_at, 'Europe/Moscow')
              = {load_date:Date}
          AND discount_percent IS NOT NULL
          AND (discount_percent < 0 OR discount_percent > 100)
        """,
        date_params,
    )
    add_check(
        name="discount_percent_range",
        check_type="SQL",
        passed=invalid_discounts == 0,
        actual=invalid_discounts,
        expected=0,
        error_message=(
            None if invalid_discounts == 0
            else "discount_percent is outside the 0..100 range"
        ),
    )

    invalid_prices = storage.scalar(
        """
        SELECT count()
        FROM ods.steam_app_details
        WHERE toDate(fetched_at, 'Europe/Moscow')
              = {load_date:Date}
          AND (
              (price_initial IS NOT NULL AND price_initial < 0)
              OR (price_final IS NOT NULL AND price_final < 0)
          )
        """,
        date_params,
    )
    add_check(
        name="non_negative_prices",
        check_type="SQL",
        passed=invalid_prices == 0,
        actual=invalid_prices,
        expected=0,
        error_message=(
            None if invalid_prices == 0
            else "ODS contains negative prices"
        ),
    )

    exact_duplicates = storage.scalar(
        """
        SELECT count()
        FROM
        (
            SELECT
                app_id,
                fetched_at,
                record_hash,
                count() AS row_count
            FROM ods.steam_app_details
            WHERE toDate(fetched_at, 'Europe/Moscow')
                  = {load_date:Date}
            GROUP BY
                app_id,
                fetched_at,
                record_hash
            HAVING row_count > 1
        )
        """,
        date_params,
    )
    add_check(
        name="no_exact_event_duplicates",
        check_type="SQL",
        passed=exact_duplicates == 0,
        actual=exact_duplicates,
        expected=0,
        error_message=(
            None if exact_duplicates == 0
            else "Duplicate (app_id, fetched_at, record_hash) events found"
        ),
    )

    storage.insert_results(
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        load_date=load_date,
        checks=checks,
    )

    failed_checks = [check for check in checks if not check.passed]
    if failed_checks:
        failed_names = ", ".join(check.name for check in failed_checks)
        raise ValueError(
            f"Data quality validation failed: {failed_names}"
        )

def finish_pipeline_audit_success(**context: Any) -> None:
    run_id, load_date, started_at = _get_audit_context(context)

    extract_metrics = _pull_metrics(
        context,
        "ingest_steam_apps_to_minio",
    )
    load_metrics = _pull_metrics(
        context,
        "load_steam_app_details_to_ods",
    )

    extracted_rows = extract_metrics.get("extracted_rows", 0)
    loaded_rows = load_metrics.get("loaded_rows", 0)
    skipped_rows = load_metrics.get("skipped_rows", 0)
    failed_rows = (
        extract_metrics.get("failed_rows", 0)
        + load_metrics.get("failed_rows", 0)
    )
    source_file_count = load_metrics.get(
        "source_file_count",
        extract_metrics.get("source_file_count", 0),
    )

    if loaded_rows + skipped_rows + load_metrics.get("failed_rows", 0) != source_file_count:
        raise ValueError(
            "ODS metrics are inconsistent: "
            "loaded_rows + skipped_rows + failed_rows "
            "must equal source_file_count"
        )

    AuditStorage().finish_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        load_date=load_date,
        started_at=started_at,
        status="SUCCESS",
        extracted_rows=extracted_rows,
        loaded_rows=loaded_rows,
        skipped_rows=skipped_rows,
        failed_rows=failed_rows,
        source_file_count=source_file_count,
    )


def mark_pipeline_audit_failed(context: dict[str, Any]) -> None:
    try:
        run_id, load_date, started_at = _get_audit_context(context)
        task_instance = context.get("task_instance")
        exception = context.get("exception")

        task_id = task_instance.task_id if task_instance is not None else "unknown"
        error_message = f"Task {task_id} failed"
        if exception is not None:
            error_message = f"{error_message}: {exception!r}"

        AuditStorage().finish_pipeline_run(
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            load_date=load_date,
            started_at=started_at,
            status="FAILED",
            failed_rows=1,
            error_message=error_message,
        )
    except Exception:
        logging.exception("Failed to write pipeline audit failure status")


with DAG(
    dag_id=PIPELINE_NAME,
    description="Steam Store API to MinIO raw and ClickHouse ODS",
    start_date=datetime(2026, 8, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "steam-game-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": mark_pipeline_audit_failed,
    },
    tags=["steam", "minio", "clickhouse", "ods", "audit"],
) as dag:
    LOAD_DATE = (
        '{{ logical_date.in_timezone("Europe/Moscow")'
        '.format("YYYY-MM-DD") }}'
    )

    start_audit = PythonOperator(
        task_id="start_pipeline_audit",
        python_callable=start_pipeline_audit,
    )

    ingest_steam_apps = BashOperator(
        task_id="ingest_steam_apps_to_minio",
        cwd=PROJECT_DIR,
        bash_command=(
            "python scripts/ingest_steam_apps.py "
            f"--load-date '{LOAD_DATE}'"
        ),
        append_env=True,
        do_xcom_push=True,
    )

    load_steam_app_details = BashOperator(
        task_id="load_steam_app_details_to_ods",
        cwd=PROJECT_DIR,
        bash_command=(
            "python scripts/load_steam_app_details.py "
            f"--load-date '{LOAD_DATE}'"
        ),
        append_env=True,
        do_xcom_push=True,
    )

    validate_ods = PythonOperator(
        task_id="validate_steam_app_details",
        python_callable=validate_steam_app_details,
    )

    finish_audit = PythonOperator(
        task_id="finish_pipeline_audit_success",
        python_callable=finish_pipeline_audit_success,
    )

    (
        start_audit
        >> ingest_steam_apps
        >> load_steam_app_details
        >> validate_ods
        >> finish_audit
    )