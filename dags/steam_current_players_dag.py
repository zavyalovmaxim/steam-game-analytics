from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from steam_game_analytics.storage.audit_storage import AuditStorage


PROJECT_DIR = "/opt/airflow/project"
PIPELINE_NAME = "steam_current_players_pipeline"


def parse_pipeline_result(value: str | None) -> dict[str, int]:
    if not value or not value.startswith("PIPELINE_RESULT="):
        raise ValueError(f"Invalid PIPELINE_RESULT: {value!r}")

    payload = json.loads(value.removeprefix("PIPELINE_RESULT="))
    return {key: int(number) for key, number in payload.items()}


def audit_start(**context: Any) -> None:
    AuditStorage().start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        run_id=context["run_id"],
        load_date=context["logical_date"]
        .in_timezone("Europe/Moscow")
        .date(),
        started_at=context["dag_run"].start_date,
    )


def audit_success(**context: Any) -> None:
    ti = context["ti"]
    extract = parse_pipeline_result(
        ti.xcom_pull(task_ids="ingest_steam_current_players_to_minio")
    )
    load = parse_pipeline_result(
        ti.xcom_pull(task_ids="load_steam_current_players_to_ods")
    )

    AuditStorage().finish_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        run_id=context["run_id"],
        load_date=context["logical_date"]
        .in_timezone("Europe/Moscow")
        .date(),
        started_at=context["dag_run"].start_date,
        status="SUCCESS",
        extracted_rows=extract.get("extracted_rows", 0),
        loaded_rows=load.get("loaded_rows", 0),
        skipped_rows=load.get("skipped_rows", 0),
        failed_rows=(
            extract.get("failed_rows", 0)
            + load.get("failed_rows", 0)
        ),
        source_file_count=load.get("source_file_count", 0),
    )


def validate_metrics(**context: Any) -> None:
    ti = context["ti"]
    extract = parse_pipeline_result(
        ti.xcom_pull(task_ids="ingest_steam_current_players_to_minio")
    )
    load = parse_pipeline_result(
        ti.xcom_pull(task_ids="load_steam_current_players_to_ods")
    )

    extracted = extract.get("extracted_rows", 0)
    extract_failed = extract.get("failed_rows", 0)
    loaded = load.get("loaded_rows", 0)
    skipped = load.get("skipped_rows", 0)
    load_failed = load.get("failed_rows", 0)
    source_count = load.get("source_file_count", 0)

    errors: list[str] = []

    if extracted <= 0:
        errors.append("extracted_rows must be greater than zero")

    if extract_failed + load_failed != 0:
        errors.append("failed_rows must equal zero")

    if loaded + skipped + load_failed != source_count:
        errors.append(
            "loaded_rows + skipped_rows + failed_rows "
            "must equal source_file_count"
        )

    if source_count != extracted:
        errors.append("source_file_count must equal extracted_rows")

    if errors:
        raise ValueError("; ".join(errors))


with DAG(
    dag_id=PIPELINE_NAME,
    description="Steam current players to MinIO raw and ClickHouse ODS",
    start_date=datetime(2026, 8, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "steam-game-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["steam", "players", "minio", "clickhouse", "ods"],
) as dag:
    load_date = (
        '{{ logical_date.in_timezone("Europe/Moscow")'
        '.format("YYYY-MM-DD") }}'
    )
    safe_run_id = (
        "{{ run_id "
        "| replace(':', '-') "
        "| replace('+', '-') "
        "| replace('/', '-') }}"
    )

    start_audit = PythonOperator(
        task_id="start_pipeline_audit",
        python_callable=audit_start,
    )

    extract = BashOperator(
        task_id="ingest_steam_current_players_to_minio",
        cwd=PROJECT_DIR,
        bash_command=(
            "python scripts/ingest_steam_current_players.py "
            f"--load-date '{load_date}' "
            f"--run-id '{safe_run_id}'"
        ),
        append_env=True,
        do_xcom_push=True,
    )

    load_ods = BashOperator(
        task_id="load_steam_current_players_to_ods",
        cwd=PROJECT_DIR,
        bash_command=(
            "python scripts/load_steam_current_players.py "
            f"--load-date '{load_date}' "
            f"--run-id '{safe_run_id}'"
        ),
        append_env=True,
        do_xcom_push=True,
    )

    validate = PythonOperator(
        task_id="validate_steam_current_players",
        python_callable=validate_metrics,
    )

    finish_audit = PythonOperator(
        task_id="finish_pipeline_audit_success",
        python_callable=audit_success,
    )

    start_audit >> extract >> load_ods >> validate >> finish_audit