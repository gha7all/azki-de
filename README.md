# azki-de-task — Quickstart

This quickstart guide helps you bring the stack up locally, seed the product/financial data, publish purchase events, and backfill historical events into ClickHouse.

---

## Prerequisites

* Docker & Docker Compose
* Python 3.9+ (recommended to use a virtual environment)
* Java 8+ (if running Spark locally)

---

## Install Python Dependencies (Optional)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Bring Up the Stack

```bash
cd docker
docker compose up -d
```

---

## Seed Data

### 1. Load Users Snapshot

```bash
python ingestion/loaders.py data/users.csv
```

### 2. Seed Product and Financial Tables

```bash
python scripts/seed_products.py
python scripts/seed_financial.py
```

---

## Publish Purchase Events to Kafka

```bash
python scripts/seed_purchase_events.py
```

---

## Start the Consumer

If the consumer is not running inside a container:

```bash
python ingestion/consumer_events.py
```

---

## Backfill Historical Events

To backfill historical events into `user_events_denorm`:

### Usage

```bash
python spark/backfill.py \
    --events path/to/events.csv \
    --users path/to/users.csv \
    --since 2025-01-01 \
    --until 2025-12-01 \
    --batch-size 5000
```

### Arguments

* `--events` — Path to events CSV or Parquet file
* `--users` — Path to users CSV, or `"mysql"` to fetch from MySQL directly
* `--since` — Start date (inclusive, format `YYYY-MM-DD`)
* `--until` — End date (exclusive, format `YYYY-MM-DD`)
* `--batch-size` — Number of rows per ClickHouse insert (default: 1000)

### Notes

* Only `purchase` events are inserted.
* Adjust `--batch-size` for faster inserts on large datasets.

---

## Additional Notes

* If Kafka fails to start due to a **KRaft `CLUSTER_ID` mismatch**, remove the Kafka data directory in `docker/clickhouse/data/kafka` (or run `docker compose down -v`) and restart.
* Materialized view `user_events_denorm_mv`:

  * Filters on `event_name = 'purchase'`
  * Expects `order_id` to be present in purchase messages
* Ensure scripts and Docker containers use **consistent environment variables** for database connections.
