from __future__ import annotations

from datetime import datetime
from typing import Any

import clickhouse_connect


class ClickHouseStorage:
    COLUMNS = [
        "app_id",
        "name",
        "type",
        "is_free",
        "required_age",
        "short_description",
        "supported_languages",
        "developers",
        "publishers",
        "platforms_windows",
        "platforms_mac",
        "platforms_linux",
        "release_date",
        "coming_soon",
        "price_currency",
        "price_initial",
        "price_final",
        "discount_percent",
        "metacritic_score",
        "recommendations_total",
        "source",
        "fetched_at",
        "record_hash",
    ]

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

    def insert_app(self, row: dict[str, Any]) -> None:
        self.client.insert(
            table="ods.steam_app_details",
            data=[[row[column] for column in self.COLUMNS]],
            column_names=self.COLUMNS,
        )

    def get_latest_record_state(
        self,
        app_id: int,
    ) -> tuple[int, datetime] | None:
        result = self.client.query(
            """
            SELECT
                record_hash,
                fetched_at
            FROM ods.steam_app_details
            WHERE app_id = {app_id:UInt32}
            ORDER BY fetched_at DESC, inserted_at DESC
            LIMIT 1
            """,
            parameters={"app_id": int(app_id)},
        )

        if not result.result_rows:
            return None

        record_hash, fetched_at = result.result_rows[0]
        return int(record_hash), fetched_at