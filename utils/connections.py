from __future__ import annotations
import os
from clickhouse_driver import Client
import pymysql


def get_clickhouse_client() -> Client:
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    user = os.getenv("CLICKHOUSE_USER", "azki_user")
    password = os.getenv("CLICKHOUSE_PASSWORD", "azki_pass")
    database = os.getenv("CLICKHOUSE_DB", "azki")
    return Client(host=host, port=port, user=user, password=password, database=database)


def get_kafka_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

def get_mysql_connection():
    conn_params = {
            "host": os.getenv("MYSQL_HOST", "mysql"),
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "user": os.getenv("MYSQL_USER", "mysql_user"),
            "password": os.getenv("MYSQL_PASSWORD", "mysql_pass"),
            "db": os.getenv("MYSQL_DB", "azki_db"),
        }

    return pymysql.connect(**conn_params)
