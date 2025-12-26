# ClickHouse Performance Optimizations


1) Choose appropriate table engines and primary keys
	- Use MergeTree for product and financial detail tables, but tune the
	  ORDER BY expression to match common access patterns (point lookups by
	  `order_id` should include `order_id` in ORDER BY).

2) Partitioning
	- Partition tables where it helps query pruning. For time-series data
	  (events), partitioning by month (toYYYYMM(event_timestamp)) is a sensible
	  default for workloads that query recent data.

3) Order keys and point lookups
	- Ensure `order_id` is part of the ORDER BY keys on product and financial
	  tables for fast point lookups during the materialized view join. This
	  can drastically reduce disk seeks for small number of joined rows.

4) Deduplication and write semantics
	- If upstream can produce duplicate events, consider using
	  ReplacingMergeTree (with a version column or event ID) or CollapsingMergeTree
	  to deduplicate on merge. Choose a deterministic dedup key such as
	  (event_id) or (user_id, order_id, event_timestamp) depending on your domain.

5) Pre-join / denormalize product data
	- If product detail tables are small relative to events, consider pre-joining
	  them into a single table (or maintaining a materialized view) so the MV
	  does not need to run a UNION at runtime. This trades storage for faster
	  MV processing.

6) Storage engines for aggregated data
	- For aggregated or rollup tables, consider SummingMergeTree or
	  AggregatingMergeTree types which can store pre-aggregated metrics and reduce
	  read-time work.

7) Monitoring and capacity planning
	- Monitor merges, disk utilization, and query latencies. Tune `max_bytes_to_merge_at_min_space_in_pool` and other merge settings carefully.

8) Example: recommended ORDER BY for `user_events_denorm`
	- ORDER BY (event_timestamp, user_id, order_id) provides time-range scan
	  efficiency and co-locates events for the same user and order.

These recommendations are pragmatic starting points for local/dev and small
production deployments; tune further based on observed query patterns and data shapes.
