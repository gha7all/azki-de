from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Iterable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, coalesce
from utils.connections import get_clickhouse_client


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--events", required=True, help="path to events CSV/Parquet")
    p.add_argument("--users", required=True, help="path to users CSV or 'mysql' to use MySQL")
    p.add_argument("--since", required=False, help="start date (YYYY-MM-DD)")
    p.add_argument("--until", required=False, help="end date (YYYY-MM-DD)")
    p.add_argument("--batch-size", type=int, default=1000)
    return p.parse_args()


def read_events(spark: SparkSession, path: str, since: str | None, until: str | None) -> DataFrame:
    df = spark.read.option("header", True).csv(path)
    df = df.withColumnRenamed("event_time", "event_timestamp")
    df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    if since:
        df = df.filter(col("event_timestamp") >= to_timestamp(lit(since)))
    if until:
        df = df.filter(col("event_timestamp") < to_timestamp(lit(until)))
    return df


def read_users(spark: SparkSession, path: str) -> DataFrame:
    if path.lower() == "mysql":
        jdbc_url = "jdbc:mysql://%s:%s/%s" % (
            os.getenv("MYSQL_HOST", "mysql"), os.getenv("MYSQL_PORT", "3306"), os.getenv("MYSQL_DB", "azki_db")
        )
        return spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "users").option("user", os.getenv("MYSQL_USER", "mysql_user")).option("password", os.getenv("MYSQL_PASSWORD", "mysql_pass")).load()
    else:
        return spark.read.option("header", True).csv(path)


def to_denorm_rows(df):
    def map_row(r):
        event_ts = r.event_timestamp.strftime("%Y-%m-%d %H:%M:%S") if r.event_timestamp else None
        return (
            event_ts,
            int(r.user_id) if r.user_id else 0,
            r.session_id or "",
            r.event_name or "",
            r.channel or "",
            int(r.premium_amount) if r.premium_amount else 0,
            getattr(r, "order_id", None),
            getattr(r, "product_type", None),
            getattr(r, "product_id", None),
            getattr(r, "product_details", None),
            getattr(r, "financial_amount", None),
            getattr(r, "payment_status", None),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        )

    return df.rdd.map(map_row)


def write_to_clickhouse(partition: Iterable, batch_size: int = 1000):
    client = get_clickhouse_client()
    batch = []
    for row in partition:
        batch.append(row)
        if len(batch) >= batch_size:
            client.execute("INSERT INTO user_events_denorm (event_timestamp, user_id, session_id, event_name, traffic_channel, premium_amount, order_id, product_type, product_id, product_details, financial_amount, payment_status, ingestion_time) VALUES", batch)
            batch.clear()
    if batch:
        client.execute("INSERT INTO user_events_denorm (event_timestamp, user_id, session_id, event_name, traffic_channel, premium_amount, order_id, product_type, product_id, product_details, financial_amount, payment_status, ingestion_time) VALUES", batch)


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("azki-backfill").getOrCreate()
    events_df = read_events(spark, args.events, args.since, args.until)
    events_df = events_df.withColumn("event_name", coalesce(col("event_name"), col("event_type")))
    events_df = events_df.filter(col("event_name") == "purchase")

    users_df = read_users(spark, args.users)
    joined = events_df.join(users_df, events_df.user_id.cast("int") == users_df.user_id.cast("int"), how="left")

    denorm_rdd = to_denorm_rows(joined.select(
        col("event_timestamp"),
        col("user_id"),
        col("session_id"),
        col("event_name"),
        col("channel"),
        col("premium_amount"),
        col("order_id"),
        col("product_type"),
        col("product_id"),
        col("product_details"),
        col("financial_amount"),
        col("payment_status"),
    ))

    denorm_rdd.foreachPartition(lambda part: write_to_clickhouse(part, args.batch_size))


if __name__ == "__main__":
    main()
