---
paths: ["schemas/**"]
---
# Avro schema (data contract) rules
- Changes must be BACKWARD compatible: only add fields that have a default.
- Never rename, retype or remove a field. Create `<name>.v2.avsc` and a new topic instead.
- Every field has a `doc`. Timestamps are `long` + `logicalType: timestamp-millis` (UTC).
