from __future__ import annotations

import json
import logging
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

    finish_audit = PythonOperator(
        task_id="finish_pipeline_audit_success",
        python_callable=finish_pipeline_audit_success,
    )

    start_audit >> ingest_steam_apps >> load_steam_app_details >> finish_audit