"""Shared connection helpers for ClickHouse, MySQL and Kafka.

Centralizes environment handling so other modules can import standardized
connection factories.
"""
from __future__ import annotations

import os
# typing.Optional intentionally omitted; pymysql import is optional inside helper

from clickhouse_driver import Client


def get_clickhouse_client() -> Client:
    """Return a configured ClickHouse Client using environment variables."""
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    user = os.getenv("CLICKHOUSE_USER", "azki_user")
    password = os.getenv("CLICKHOUSE_PASSWORD", "azki_pass")
    database = os.getenv("CLICKHOUSE_DB", "azki")
    return Client(host=host, port=port, user=user, password=password, database=database)


def get_kafka_bootstrap() -> str:
    """Return the Kafka bootstrap server address used inside Docker Compose by default."""
    return os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")


def get_mysql_connection():
    """Return a pymysql connection using env vars, or None if pymysql not installed.

    Caller must import pymysql themselves if they need typing hints; this helper
    keeps the import optional so the package can be used without pymysql installed.
    """
    try:
        import pymysql
    except Exception:
        return None

    host = os.getenv("MYSQL_HOST", "mysql")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "etl_app_user")
    password = os.getenv("MYSQL_PASSWORD", "etl_app_pass")
    db = os.getenv("MYSQL_DB", "azki_db")

    return pymysql.connect(host=host, port=port, user=user, password=password, db=db)
