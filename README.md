# Steam Game Analytics

Pet project for collecting Steam data and building a local data engineering pipeline.

## Stack

* Python 3.12
* Apache Airflow
* MinIO
* ClickHouse
* PostgreSQL
* Docker Compose
* uv

## Architecture

```text
Steam API
    │
    ▼
Python ingestion
    │
    ▼
MinIO RAW
    │
    ▼
ClickHouse ODS
```

Apache Airflow is used for orchestration, retries, audit logging and data quality checks.

## Implemented Pipelines

### Steam App Details

Collects Steam application metadata:

* name and type;
* developers and publishers;
* supported platforms;
* release date;
* price and discount;
* Metacritic score;
* recommendation count.

Flow:

```text
Steam API
→ MinIO RAW
→ ods.steam_app_details
→ Data Quality
```

Runs daily.

The loader calculates a hash from business fields and compares it with the latest version in ODS. If the data has not changed, the record is skipped.

### Steam Current Players

Collects current online player counts.

Flow:

```text
Steam API
→ MinIO RAW
→ ods.steam_current_players
→ Data Quality
```

Runs hourly.

Player counts are stored as historical time-series observations.

## Data Layers

### RAW

Original API responses are stored in MinIO as JSON.

Example:

```text
raw/
└── steam/
    ├── app_details/
    │   └── load_date=YYYY-MM-DD/
    └── current_players/
        └── load_date=YYYY-MM-DD/
```

### ODS

ClickHouse currently contains:

```text
ods.steam_app_details
ods.steam_current_players
```

## Audit and Data Quality

Each pipeline run stores operational metrics:

* status;
* extracted rows;
* loaded rows;
* skipped rows;
* failed rows;
* processed source files;
* error message.

Data quality checks validate pipeline results and fail the DAG if critical checks are not passed.

## Project Structure

```text
steam-game-analytics/
├── dags/
├── scripts/
├── sql/ods/
├── src/steam_game_analytics/
├── airflow/
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## Run Locally

Clone the repository:

```bash
git clone https://github.com/zavyalovmaxim/steam-game-analytics.git
cd steam-game-analytics
```

Install dependencies:

```bash
uv sync
```

Create `.env` with MinIO, ClickHouse and Airflow credentials.

Create Docker network:

```bash
docker network create steam-analytics-network
```

Start services:

```bash
docker compose up -d --build
```

Interfaces:

* Airflow — `http://localhost:8080`
* MinIO — `http://localhost:9001`
* ClickHouse — `http://localhost:8123`

## Current Status

Implemented:

* [x] Steam API ingestion
* [x] MinIO RAW layer
* [x] ClickHouse ODS layer
* [x] Airflow orchestration
* [x] Incremental app metadata loading
* [x] Historical player-count collection
* [x] Pipeline audit
* [x] Data quality checks

Planned:

* [ ] Additional data sources
* [ ] DDS layer
* [ ] Data marts
* [ ] BI dashboards
