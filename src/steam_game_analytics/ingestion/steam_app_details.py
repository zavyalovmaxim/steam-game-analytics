from __future__ import annotations

from datetime import date, datetime
from typing import Any
import hashlib
import json

BUSINESS_FIELDS = [
    "name",
    "type",
    "is_free",
    "required_age",
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
]

def calculate_record_hash(row: dict[str, Any]) -> int:
    business_data = {
        field: row.get(field)
        for field in BUSINESS_FIELDS
    }

    serialized = json.dumps(
        business_data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    digest = hashlib.blake2b(
        serialized.encode("utf-8"),
        digest_size=8,
    ).digest()

    return int.from_bytes(digest, byteorder="big", signed=False)

def parse_release_date(value: str | None) -> date | None:
    if not value:
        return None

    formats = (
        "%d %b, %Y",
        "%b %d, %Y",
        "%d %B, %Y",
        "%B %d, %Y",
    )

    for date_format in formats:
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            continue

    return None


def normalize_app_details(
    app_id: int,
    data: dict[str, Any],
    fetched_at: datetime
) -> dict[str, Any]:
    platforms = data.get("platforms") or {}
    price = data.get("price_overview") or {}
    metacritic = data.get("metacritic") or {}
    recommendations = data.get("recommendations") or {}
    release = data.get("release_date") or {}

    return {
        "app_id": app_id,
        "name": data.get("name") or "",
        "type": data.get("type") or "unknown",
        "is_free": bool(data.get("is_free", False)),
        "required_age": int(data.get("required_age") or 0),
        "short_description": data.get("short_description") or "",
        "supported_languages": data.get("supported_languages") or "",
        "developers": data.get("developers") or [],
        "publishers": data.get("publishers") or [],
        "platforms_windows": bool(platforms.get("windows", False)),
        "platforms_mac": bool(platforms.get("mac", False)),
        "platforms_linux": bool(platforms.get("linux", False)),
        "release_date": parse_release_date(release.get("date")),
        "coming_soon": bool(release.get("coming_soon", False)),
        "price_currency": price.get("currency"),
        "price_initial": price.get("initial"),
        "price_final": price.get("final"),
        "discount_percent": price.get("discount_percent"),
        "metacritic_score": metacritic.get("score"),
        "recommendations_total": recommendations.get("total"),
        "source": "steam_store_api",
        "fetched_at": fetched_at,
    }
