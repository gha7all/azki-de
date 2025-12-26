# Data Quality & Monitoring Plan

This document outlines a pragmatic Data Quality (DQ) and monitoring plan for the pipeline in this repository.
It covers sync/delay monitoring, missing events detection, schema drift, load monitoring, and operational actions.

## Objectives
- Detect ingestion delays and sync issues quickly
- Identify missing or duplicate events
- Detect schema drift and incompatible payload changes
- Monitor load and performance of ingestion and downstream jobs
- Provide mechanisms for alerting, debugging, and backfilling

## Metrics to collect
- Ingest throughput (events/sec) per topic and per partition
- Lag (consumer group lag) for each consumer group (earliest/last offsets)
- ClickHouse insertion rates and insert latency (ms per batch)
- Counts: events produced vs events consumed vs rows written to ClickHouse
- Error rates: parse failures, insert errors, deserialization errors
- Schema-related metrics: number of messages with unexpected fields or missing required fields

## Checks and queries (examples)

1) End-to-end row-count check

 - daily_count_produced = number of messages produced to `user_events` (from Kafka)
 - daily_count_consumed = number of messages inserted into `user_events` table (ClickHouse)

 SQL (ClickHouse):

 ```sql
 -- count raw events ingested into ClickHouse in the window
 SELECT
   toDate(event_timestamp) AS day,
   count() AS cnt
 FROM user_events
 WHERE event_timestamp >= '2025-10-01' AND event_timestamp < '2025-11-01'
 GROUP BY day
 ORDER BY day;
 ```

 Compare the counts with Kafka-produced metrics (from Kafka Connect/producer logs) — alert if delta exceeds threshold (e.g., 1% or 1000 messages).

2) Missing events / holes detection

 - If events have sequence IDs or monotonically increasing event_id, detect gaps.
 - Without sequence IDs, monitor daily cardinality of user_ids or session_ids and compare to expected baselines.

 SQL example to detect duplicate event_id or gaps (if event_id exists):

 ```sql
 SELECT
   min(event_id) as min_id,
   max(event_id) as max_id,
   countDistinct(event_id) as distinct_count,
   (max_id - min_id + 1) - distinct_count AS holes
 FROM user_events
 WHERE toDate(event_timestamp) = today()
 ```

3) Schema drift detection

 - Maintain schema contract (e.g., Avro/JSON schema) in a registry. Validate incoming messages with a schema validator.
 - Count messages that fail validation or have unexpected top-level types.

 Example: track messages with missing required fields (pseudo-query):

 ```sql
 SELECT count() FROM user_events
 WHERE event_name IS NULL OR user_id = 0
 ```

4) Delay / freshness monitoring

 - Measure the time delta between event_timestamp and ingestion_time (now) in ClickHouse.
 - Alert when the 95th percentile of ingestion delay exceeds an SLA (e.g., 2 minutes).

 SQL example:
 ```sql
 SELECT
   quantile(0.95)(now() - ingestion_time) AS p95_delay_sec
 FROM user_events
 WHERE ingestion_time >= now() - INTERVAL 1 HOUR
 ```

5) Load monitoring

 - Track ingestion batch sizes and insert latencies. Alert when avg batch insert time grows or failures increase.

## Monitoring & alerting setup
- Export metrics from consumers/producers (Prometheus): consumer lag, throughput, errors.
- Export ClickHouse metrics (via built-in metrics or Prometheus exporter): insert rates, queries, merges.
- Create dashboards for quick triage: lag over time, produced/consumed counts, delays, P95 insertion latencies.
- Alerts: high consumer lag; p95 ingestion delay > SLA; spikes of parse failures; sustained insert errors.

## Data contracts & schema governance
- Use a schema registry (Avro/JSON Schema) for Kafka topics and validate producers at write-time.
- Version schemas and provide compatibility rules (backward/forward). Keep examples in repo.
- Apply lightweight contract tests in CI for any producer changes.

## Error handling & retries
- Producers should write idempotent event keys (event_id) so downstream can deduplicate.
- Consumers should batch and retry transient ClickHouse insert errors with exponential backoff.
- Record poison messages in a dead-letter queue (Kafka topic) and alert on persistent failures.

## Backfill strategy
- Maintain a backfill job (Spark) that can reprocess a time range from source (Kafka archive, S3, or snapshots) and re-populate ClickHouse.
- Use a deduplication strategy (ReplacingMergeTree or dedupe keys) so reprocessing won't create duplicates.

## Operational runbook (short)
1. Check consumer group lag; if high, scale consumers or investigate producer surge.
2. Check ClickHouse insert errors and query logs.
3. If missing data is detected, run backfill job for the affected time range.
4. If schema drift occurs, roll out compatibility fix and reprocess only impacted ranges.

## Sample DQ checks to implement in this repo
- A lightweight script `quality/dq_checks.py` that runs a set of SQL checks against ClickHouse and fails with non-zero exit code when thresholds are breached. (This repo already contains `quality/dq_checks.py`.)
- Periodic scheduler (cron/Kubernetes CronJob) to run the DQ checks and send notifications (Slack/Email) on failures.

---

This plan balances pragmatic monitoring with capability to debug and backfill. I can now implement the Spark backfill job and wire a few example DQ checks into `quality/dq_checks.py` if you want — which would you prefer next?
