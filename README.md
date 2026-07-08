# Citi Bike Live Availability

A real-time + batch data pipeline for Citi Bike (NYC) station availability, built on Kafka. Live station status is streamed from the public GBFS feed, fanned out to a live map and to durable S3 storage, and summarized hourly by Airflow.

## Architecture

```
Citi Bike GBFS API
        │  (poll every 15s)
        ▼
   [ producer ]  ──►  Kafka topic: citibike-station-status
                              │
              ┌───────────────┴───────────────┐
              ▼                                ▼
   [ live-state-consumer ]            [ batch-consumer ]
        writes latest                  buffers 30s of
        state per station              messages, writes
        to Redis                       JSON lines to S3
              │                        (raw/dt=.../hour=.../)
              ▼                                │
        [ backend (FastAPI) ]                  ▼
        GET /stations                  [ Airflow DAG, hourly ]
        joins Redis state with         summarize_last_hour +
        station_info.json              check_data_quality
              │                        writes CSVs to S3
              ▼                        (summaries/, suspicious/)
        [ frontend ]
        Leaflet map, polls
        /stations every 15s
```

## Components

| Component | Path | Description |
|---|---|---|
| Producer | [producer/station_status_producer.py](producer/station_status_producer.py) | Polls the Citi Bike `station_status` GBFS endpoint every 15s and publishes each station's status to the `citibike-station-status` Kafka topic. |
| Station info fetcher | [producer/fetch_station_info.py](producer/fetch_station_info.py) | One-off script that pulls `station_information` (name, lat/lon) and writes it to `data/station_info.json`, used to enrich live status with static metadata. |
| Live-state consumer | [consumers/live_state_consumer.py](consumers/live_state_consumer.py) | Consumes the topic and writes the latest known state per station to Redis (`station:<id>`), keyed for fast lookup. |
| Batch consumer | [consumers/batch_consumer.py](consumers/batch_consumer.py) | Consumes the topic, buffers messages for 30s, and writes them as newline-delimited JSON to S3 under `raw/dt=YYYY-MM-DD/hour=HH/`. |
| Backend API | [backend/main.py](backend/main.py) | FastAPI service exposing `GET /stations`, joining live Redis state with static station metadata. |
| Frontend | [frontend/index.html](frontend/index.html) | Leaflet map centered on Manhattan; polls the backend every 15s and color-codes stations by bikes available (red/orange/green). |
| Airflow DAG | [airflow/dags/citibike_hourly_summary.py](airflow/dags/citibike_hourly_summary.py) | Runs hourly: aggregates the prior hour's raw S3 data into per-station min/max/avg bike availability (`summaries/`), and flags readings with zero bikes and zero docks as likely feed glitches (`suspicious/`). |

## Tech stack

- **Streaming**: Apache Kafka (KRaft mode, single broker) + Kafka UI
- **State store**: Redis
- **Storage**: AWS S3 (raw events, hourly summaries, data-quality output)
- **Orchestration**: Apache Airflow (CeleryExecutor, Postgres metadata DB)
- **API**: FastAPI
- **Frontend**: static HTML + Leaflet.js, served via nginx
- **Language**: Python 3.11

## Prerequisites

- Docker and Docker Compose
- An AWS S3 bucket and credentials with read/write access to it

## Setup

1. Create a `.env` file in the project root:

   ```
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_DEFAULT_REGION=...
   S3_BUCKET=...
   ```

2. Fetch static station metadata (name/lat/lon), used by the backend to enrich live status:

   ```bash
   pip install -r requirements.txt
   python producer/fetch_station_info.py
   ```

   This writes `data/station_info.json`, which the backend container mounts read-only.

3. Start the core pipeline (Kafka, Redis, producer, consumers, backend, frontend):

   ```bash
   docker-compose up --build
   ```

   - Frontend map: [http://localhost:5500](http://localhost:5500)
   - Backend API: [http://localhost:8000/stations](http://localhost:8000/stations)
   - Kafka UI: [http://localhost:8080](http://localhost:8080)

4. (Optional) Start Airflow for hourly summarization/data-quality checks:

   ```bash
   docker-compose -f airflow-docker-compose.yaml up --build
   ```

   - Airflow UI: [http://localhost:8081](http://localhost:8081)

   The Airflow DAG (`citibike_hourly_summary`) reads raw data the batch consumer wrote to S3 and is unrelated to the core stack's Redis/Kafka instances — it uses its own Postgres/Redis for Airflow's internals.

## S3 layout

```
s3://<bucket>/
├── raw/dt=YYYY-MM-DD/hour=HH/batch_<timestamp>.json   # raw station status, newline-delimited JSON
├── summaries/dt=YYYY-MM-DD/hour=HH.csv                # per-station avg/min/max bikes available
└── suspicious/dt=YYYY-MM-DD/hour=HH.csv                # readings with 0 bikes AND 0 docks (likely feed glitch)
```

## Roadmap

See [todo.txt](todo.txt):
- GPS navigation to a selected station
- Notifications on status change for a selected/favorited station
- Favoriting stations
- Search by zip code / radius
- Strava integration for distance ridden
- Leaderboard of distance ridden (day/week/month/year/all-time)
