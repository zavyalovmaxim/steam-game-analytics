from datetime import date, datetime, timezone

from steam_game_analytics.storage.audit_storage import AuditStorage

from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    storage = AuditStorage()

    pipeline_name = "audit_smoke_test"
    run_id = "manual__audit_smoke_test"
    load_date = date.today()
    started_at = datetime.now(timezone.utc)

    storage.start_pipeline_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        load_date=load_date,
        started_at=started_at,
    )

    storage.finish_pipeline_run(
        pipeline_name=pipeline_name,
        run_id=run_id,
        load_date=load_date,
        started_at=started_at,
        status="SUCCESS",
        extracted_rows=10,
        loaded_rows=8,
        skipped_rows=2,
        failed_rows=0,
        source_file_count=10,
    )


if __name__ == "__main__":
    main()