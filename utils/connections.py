from __future__ import annotations
from clickhouse_driver import Client
import pymysql


def get_clickhouse_client() -> Client:
    return Client(
        host="127.0.0.1", 
        port=9000,
        user="azki_user",
        password="azki_pass",
        database="azki"
    )


def get_kafka_bootstrap() -> str:
    return "127.0.0.1:9092"


def get_mysql_connection():
    return pymysql.connect(
        host="127.0.0.1",     # replace with your MySQL host
        port=3306,
        user="mysql_user",
        password="mysql_pass",
        db="azki_db"
    )
