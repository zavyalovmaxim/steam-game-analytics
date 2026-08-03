CREATE TABLE IF NOT EXISTS ods.steam_app_details
(
    app_id UInt32,
    name String,
    `type` LowCardinality(String),
    is_free Bool,
    required_age UInt16,
    short_description String,
    supported_languages String,

    developers Array(String),
    publishers Array(String),

    platforms_windows Bool,
    platforms_mac Bool,
    platforms_linux Bool,

    release_date Nullable(Date),
    coming_soon Bool,

    price_currency Nullable(String),
    price_initial Nullable(UInt64),
    price_final Nullable(UInt64),
    discount_percent Nullable(UInt8),

    metacritic_score Nullable(UInt8),
    recommendations_total Nullable(UInt64),

    source LowCardinality(String),
    fetched_at DateTime64(3, 'UTC'),
    record_hash UInt64,

    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (app_id, fetched_at);