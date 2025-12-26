CREATE TABLE IF NOT EXISTS user_events
(
    event_timestamp   DateTime,
    user_id           UInt32,
    session_id        String,
    event_name        LowCardinality(String),
    traffic_channel   LowCardinality(String),
    premium_amount    UInt64,
    ingestion_time    DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_timestamp, user_id, session_id);


CREATE TABLE IF NOT EXISTS user_events_kafka
(
    event_timestamp   String,
    user_id           UInt32,
    session_id        String,
    event_name        String,
    traffic_channel   String,
    premium_amount    UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'user_events',
    kafka_group_name = 'clickhouse_user_events_consumer_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;


CREATE MATERIALIZED VIEW IF NOT EXISTS user_events_mv
TO user_events
AS
SELECT
    parseDateTimeBestEffort(event_timestamp) AS event_timestamp,
    user_id,
    session_id,
    event_name,
    traffic_channel,
    premium_amount
FROM user_events_kafka;


CREATE TABLE IF NOT EXISTS users
(
    user_id        UInt32,
    signup_date    Date,
    city           String,
    device_type    String,
    updated_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY user_id;
