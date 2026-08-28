CREATE TABLE IF NOT EXISTS ods.steam_current_players
(
    app_id UInt32,
    player_count UInt64,
    source LowCardinality(String),
    observed_at DateTime64(6, 'UTC'),
    load_date Date,
    run_id String,
    source_object_key String,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (app_id, observed_at, run_id);