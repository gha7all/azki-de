-- Aggregated events table: monthly aggregates by city and event
CREATE TABLE IF NOT EXISTS events_agg
(
    agg_month       String,
    city            String,
    event_name      LowCardinality(String),
    event_count     UInt64,
    total_premium   UInt64,
    updated_at      DateTime
)
ENGINE = MergeTree
ORDER BY (agg_month, city, event_name);
