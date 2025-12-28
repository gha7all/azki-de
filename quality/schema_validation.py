from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, ValidationError
from utils.connections import get_clickhouse_client

client = get_clickhouse_client()
TABLE = "user_events"

class EventSchema(BaseModel):
    event_timestamp: str
    user_id: int
    session_id: str
    event_name: Optional[str]
    traffic_channel: Optional[str]
    premium_amount: int
    ingestion_time: Optional[str]

def validate_schema(batch_size=1000):
    rows = client.execute(f"SELECT * FROM {TABLE} LIMIT {batch_size}")
    columns = [col[0] for col in client.execute(f"DESCRIBE TABLE {TABLE}")]
    
    failed = 0
    for row in rows:
        record = dict(zip(columns, row))
        try:
            EventSchema(**record)
        except ValidationError as e:
            failed += 1
            print(f"Schema validation failed for record: {record}\n{e}")
    print(f"Checked {len(rows)} rows, {failed} failed validation")

if __name__ == "__main__":
    validate_schema()
