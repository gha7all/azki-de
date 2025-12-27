# azki-de-task — quickstart

This repository contains a small ClickHouse + Kafka local demo for a take-home
exercise. The quickstart below helps you bring the stack up locally, seed the
product/financial data, publish a few purchase events, and verify the denormalized
materialized view has been populated.

Prerequisites
- Docker & Docker Compose
- Python 3.9+ (venv recommended)

Install Python deps (optional, for running scripts outside containers):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Bring up the stack

```bash
cd docker
docker compose up -d
```

Seed data

1. Load users snapshot (example):

```bash
python ingestion/loaders.py data/users.csv
```

2. Seed product and financial tables:

```bash
python scripts/seed_products.py
python scripts/seed_financial.py
```

Publish purchase events to Kafka

```bash
python scripts/seed_purchase_events.py
```

Start the consumer (if not running in container)

```bash
python ingestion/consumer_events.py
```

Verify ClickHouse

```bash
python scripts/verify_clickhouse.py

# or use clickhouse-client inside the server container:
docker exec -it docker_clickhouse_1 clickhouse-client --query "SELECT count() FROM user_events"
docker exec -it docker_clickhouse_1 clickhouse-client --query "SELECT count() FROM user_events_denorm"
```

Notes
- If Kafka fails to start due to a KRaft CLUSTER_ID mismatch, remove the Kafka data directory in `docker/clickhouse/data/kafka` (or run `docker compose down -v`) and restart.
- The materialized view `user_events_denorm_mv` filters on `event_name = 'purchase'` and expects `order_id` to be present in purchase messages.
