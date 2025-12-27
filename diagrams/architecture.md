# Architecture diagram

Below is a high-level architecture diagram (Mermaid) for this repository's data flow and components.

```mermaid
flowchart LR
  subgraph Producers
    A1[seed_*.py / producer.py]
    A2[external apps]
  end

  A1 -->|events| K[Kafka topics]
  A2 -->|events| K

  subgraph Connect/CDC
    DC[Debezium / Kafka Connect]
    CC[connect configs (docker/kafka/connect/*.json)]
  end

  K --> DC
  DC --> K[Kafka topics]

  subgraph Ingestion
    CE[ingestion/consumer_events.py]
    CU[ingestion/consumer_users_cdc.py]
    LD[ingestion/loaders.py]
    AG[ingestion/aggregate_job.py]
  end

  K --> CE
  K --> CU
  CE --> LD
  CU --> LD
  LD --> CH[ClickHouse]
  AG --> CH

  subgraph ClickHouse
    CH[ClickHouse (docker/clickhouse/init.sql)]
    RAW[raw_tables.sql]
    AGG[aggregates.sql]
  end

  CH --> RAW
  CH --> AGG

  subgraph Batch/Backfill
    SPARK[backfill.py / spark/]
    SCRIPTS[scripts/*seed_*.py]
  end

  SCRIPTS --> K
  SPARK --> CH

  subgraph Quality
    DQ[quality/dq_checks.py]
    METRICS[quality/metrics.py]
    VALID[schema_validation.py]
  end

  CH --> DQ
  LD --> DQ
  LD --> METRICS

  subgraph Testing & CI
    TESTS[tests/]
    VERIFY[scripts/verify_clickhouse.py]
  end

  TESTS --> CH
  VERIFY --> CH

  subgraph Orchestration
    DOCKER[docker/docker-compose.yml]
    KAFKA[docker/kafka/]
  end

  DOCKER --> KAFKA
  DOCKER --> CH

  style CH fill:#f9f,stroke:#333,stroke-width:1px
  style K fill:#fffbcc,stroke:#333,stroke-width:1px
  style DQ fill:#e0f7fa,stroke:#333,stroke-width:1px

  %% Legend
  classDef components fill:#fff,stroke:#333;
  class A1,A2,K,DC,CE,CU,LD,AG,CH,SPARK,DQ components;
```

Render this file with a Mermaid renderer (VS Code Mermaid Preview or `mmdc`).

Rendering example (install `@mermaid-js/mermaid-cli`):

```bash
# install once (node/npm required)
npm install -g @mermaid-js/mermaid-cli

# render to PNG
mmdc -i diagrams/architecture.md -o diagrams/architecture.png
```

Quick notes:
- **Producers**: scripts/seed_*.py and `ingestion/producer.py` create events for Kafka.
- **CDC/Connect**: Debezium and Kafka Connect configurations are in `docker/kafka/connect/` and `docker/connect/`.
- **Ingestion**: Consumers under `ingestion/` read Kafka and call `ingestion/loaders.py` to write to ClickHouse.
- **Batch**: `spark/backfill.py` and `ingestion/aggregate_job.py` run historical/aggregation jobs against ClickHouse.
- **Quality**: `quality/dq_checks.py` and `quality/metrics.py` validate data and emit metrics.

Files referenced: [ingestion/loaders.py](ingestion/loaders.py), [docker/docker-compose.yml](docker/docker-compose.yml), [docker/clickhouse/init.sql](docker/clickhouse/init.sql), [scripts/verify_clickhouse.py](scripts/verify_clickhouse.py)
