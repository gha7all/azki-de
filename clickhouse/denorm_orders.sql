CREATE TABLE IF NOT EXISTS product_third
(
    order_id     UInt64,
    product_id   UInt64,
    details      String
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS product_body
(
    order_id     UInt64,
    product_id   UInt64,
    details      String
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS product_medical
(
    order_id     UInt64,
    product_id   UInt64,
    details      String
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS product_fire
(
    order_id     UInt64,
    product_id   UInt64,
    details      String
)
ENGINE = MergeTree
ORDER BY order_id;

-- Financial order table
CREATE TABLE IF NOT EXISTS financial_order
(
    order_id       UInt64,
    amount         UInt64,
    payment_status String
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE VIEW IF NOT EXISTS product_orders_all AS
SELECT order_id, 'third' AS product_type, product_id, details FROM product_third
UNION ALL
SELECT order_id, 'body' AS product_type, product_id, details FROM product_body
UNION ALL
SELECT order_id, 'medical' AS product_type, product_id, details FROM product_medical
UNION ALL
SELECT order_id, 'fire' AS product_type, product_id, details FROM product_fire;



CREATE TABLE IF NOT EXISTS user_events_denorm
(
    event_timestamp   DateTime,
    user_id           UInt32,
    session_id        String,
    event_name        LowCardinality(String),
    traffic_channel   LowCardinality(String),
    premium_amount    UInt64,
    order_id          UInt64,
    product_type      LowCardinality(String),
    product_id        Nullable(UInt64),
    product_details   Nullable(String),
    financial_amount  Nullable(UInt64),
    payment_status    Nullable(String),

    ingestion_time    DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_timestamp, user_id, order_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS user_events_denorm_mv
TO user_events_denorm
AS
SELECT
    parseDateTimeBestEffort(event_timestamp) AS event_timestamp,
    user_id,
    session_id,
    event_name,
    traffic_channel,
    premium_amount,
    NULL AS order_id,
    NULL AS product_type,
    NULL AS product_id,
    NULL AS product_details,
    NULL AS financial_amount,
    NULL AS payment_status,
    now() AS ingestion_time
FROM user_events_kafka
WHERE event_name = 'purchase';