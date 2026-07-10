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
| Frontend | [frontend/index.html](frontend/index.html) | Leaflet map centered on Manhattan; polls the backend every 15s and color-codes stations by bikes available (red/orange/green). Includes GPS "Find My Location" feature and nearest station detection. |
| Airflow DAG | [airflow/dags/citibike_hourly_summary.py](airflow/dags/citibike_hourly_summary.py) | Runs hourly: aggregates the prior hour's raw S3 data into per-station min/max/avg bike availability (`summaries/`), and flags readings with zero bikes and zero docks as likely feed glitches (`suspicious/`). |

## Tech stack

- **Streaming**: Apache Kafka (KRaft mode, single broker) + Kafka UI
- **State store**: Redis
- **Storage**: AWS S3 (raw events, hourly summaries, data-quality output)
- **Orchestration**: Apache Airflow (CeleryExecutor, Postgres metadata DB)
- **API**: FastAPI
- **Frontend**: static HTML + Leaflet.js, served via nginx
- **Infrastructure**: Terraform (AWS S3 bucket + IAM user/policy)
- **Language**: Python 3.11

## Prerequisites

- Docker and Docker Compose
- An AWS account with an S3 bucket and IAM credentials
- Terraform (for infrastructure provisioning)

## Infrastructure setup (Terraform)

AWS infrastructure (S3 bucket, IAM user, scoped IAM policy) is managed as code via Terraform. To provision from scratch:

1. Install Terraform: https://developer.hashicorp.com/terraform/install

2. Configure AWS credentials with admin permissions (required for Terraform to create/manage IAM resources):

   ```bash
   aws configure --profile terraform
   ```

3. Initialize and apply:

   ```bash
   cd terraform
   terraform init
   AWS_PROFILE=terraform terraform plan   # preview what will be created
   AWS_PROFILE=terraform terraform apply  # create the resources
   ```

   This creates:
   - An S3 bucket for raw events, summaries, and data-quality output
   - An IAM user (`citibike-pipeline`) scoped to that bucket only
   - A custom IAM policy granting `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, `s3:DeleteObject`

4. After applying, generate an access key for the IAM user in the AWS Console (IAM → Users → citibike-pipeline → Security credentials → Create access key) and add it to your `.env` file (see below).

> **Note:** Terraform commands should always run with admin credentials (`AWS_PROFILE=terraform`). The `citibike-pipeline` user is intentionally scoped too narrowly to run Terraform — it can only access S3, not read IAM metadata.

## Application setup

1. Create a `.env` file in the project root using the access key generated above:

   ```
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_DEFAULT_REGION=us-east-1
   S3_BUCKET=your-bucket-name
   ```

2. Fetch static station metadata (name/lat/lon), used by the backend to enrich live status:

   ```bash
   pip install -r requirements.txt
   python producer/fetch_station_info.py
   ```

   This writes `data/station_info.json`, which the backend container mounts read-only.

3. Start the core pipeline (Kafka, Redis, producer, consumers, backend, frontend):

   ```bash
   docker compose up -d --build
   ```

   - Frontend map: [http://localhost:5500](http://localhost:5500)
   - Backend API: [http://localhost:8000/stations](http://localhost:8000/stations)
   - Kafka UI: [http://localhost:8080](http://localhost:8080)

4. (Optional) Start Airflow for hourly summarization/data-quality checks:

   ```bash
   docker compose -p airflow -f airflow-docker-compose.yaml up -d
   ```

   - Airflow UI: [http://localhost:8081](http://localhost:8081) (login: `airflow` / `airflow`)
   - Unpause the `citibike_hourly_summary` DAG in the UI to enable scheduled runs

   > **Note:** Always use `-p airflow` when running Airflow compose commands to keep it isolated from the pipeline's network and containers.

## S3 layout

```
s3://<bucket>/
├── raw/dt=YYYY-MM-DD/hour=HH/batch_<timestamp>.json   # raw station status, newline-delimited JSON
├── summaries/dt=YYYY-MM-DD/hour=HH.csv                # per-station avg/min/max bikes available
└── suspicious/dt=YYYY-MM-DD/hour=HH.csv               # readings with 0 bikes AND 0 docks (likely feed glitch)
```

## Shutting down

```bash
# Stop pipeline
docker compose down

# Stop Airflow (always include -p airflow)
docker compose -p airflow -f airflow-docker-compose.yaml down
```

## Roadmap

- GPS navigation to a selected station
- Notifications on status change for a selected/favorited station
- Favoriting stations
- Search by zip code / radius
- Strava integration for distance ridden
- Leaderboard of distance ridden (day/week/month/year/all-time)
- Multi-day rollup DAG (busiest/emptiest stations over time)
- DynamoDB as live-state store (for cloud deployment beyond local)