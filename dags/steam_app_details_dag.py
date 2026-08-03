from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


PROJECT_DIR = "/opt/airflow/project"


with DAG(
    dag_id="steam_app_details_pipeline",
    description="Steam Store API to MinIO raw and ClickHouse ODS",
    start_date=datetime(2026, 8, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "steam-game-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["steam", "minio", "clickhouse", "ods"],
) as dag:
    LOAD_DATE = (
        '{{ logical_date.in_timezone("Europe/Moscow")'
        '.format("YYYY-MM-DD") }}'
    )
    ingest_steam_apps = BashOperator(
        task_id="ingest_steam_apps_to_minio",
        cwd=PROJECT_DIR,
        bash_command=(
            "python scripts/ingest_steam_apps.py "
            f"--load-date '{LOAD_DATE}'"
        ),
        append_env=True,
    )

    load_steam_app_details = BashOperator(
        task_id="load_steam_app_details_to_ods",
        cwd=PROJECT_DIR,
        bash_command=(
            "python scripts/load_steam_app_details.py "
            f"--load-date '{LOAD_DATE}'"
        ),
        append_env=True,
    )

    ingest_steam_apps >> load_steam_app_details