## Data governance & access control suggestions

- Use ClickHouse RBAC (users, roles, grants) to restrict who can query PII or
	financial columns (for example, `financial_amount` and `payment_status`).
- Store highly sensitive columns (like `payment_status`) in a separate table
	or secure store with stricter access controls, and reference them by opaque
	identifiers in tables exposed to analysts.
- For non-development environments, enable TLS and authentication for ClickHouse
	and Kafka; keep credentials out of code by using a secrets manager.
- Audit queries and create monitoring/alerts for anomalous access patterns or
	sudden spikes in data retrieval that might indicate exfiltration.
- Apply column-level masking, or provide curated views that obfuscate sensitive
	fields for analysts who don't require full access.