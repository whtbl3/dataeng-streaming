# Spec 003: Data contracts (Avro + Schema Registry, invalid events → DLQ)
Status: Implemented (2026-11-18) · Intent: roadmap item 3 · Author: project owner

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
Producers and consumers agree on event shape only by convention. Today a single malformed
message stops a bronze job, and the whole pipeline stops with it (design review note DRN-1).
Nothing prevents a producer from renaming or removing a field that a running job depends on.
A GPS ping with an impossible position or a missing vehicle id is stored like any other.

## Outcome
- Every event on the GPS and CDC topics has a registered schema.
- The registry rejects changes that would break existing consumers (FULL_TRANSITIVE compatibility, see plan decision 1), and CI catches them before merge.
- Events that cannot be read, or that break a basic rule, go to a dead-letter queue with the reason. The good events keep flowing.
- Bronze keeps the same tables and columns as today.

## Users and scenarios
- **Developer:** changes a schema; CI tells them if the change is safe before the stack ever sees it.
- **Analyst:** finds rejected events, and why they were rejected, with SQL.
- **Interviewer:** sees schema evolution, compatibility rules and a DLQ that keeps the pipeline alive.

## Acceptance criteria
- [ ] AC1: Given a fresh stack, when it starts, then the GPS and CDC subjects are registered with FULL_TRANSITIVE compatibility, and the GPS producer and the CDC connector write registry-encoded messages.
- [ ] AC2: Given the stack is running, when N valid pings and M order changes are produced, then bronze contains exactly N and M new rows with the same columns and values as before this feature (spec 001 AC3/AC5, spec 002 AC5 still pass).
- [ ] AC3: Given the stack is running, when a message that is not decodable is produced to a source topic, then the bronze job keeps running, the message appears once in the dead-letter queue with its raw bytes, source topic, partition, offset and the reason, and the next valid messages still reach bronze.
- [ ] AC4: Given the stack is running, when pings break a validation rule (missing vehicle id, position outside the city bounding box, speed outside 0–80 km/h, event time more than 5 minutes in the future), then each is routed to the dead-letter queue with a rule-specific reason and is not written to bronze.
- [ ] AC5: Given a schema change that adds an optional field with a default, when CI runs, then the compatibility check passes; given a change that removes or renames a required field or changes its type, then CI fails with a clear message (no Docker needed).
- [ ] AC6: Given a producer that sends the new optional field, when the running bronze job reads it, then the job keeps running and bronze rows stay correct.
- [ ] AC7: Given the simulator, when an invalid-event rate is set, then about that share of pings is invalid in known, labelled ways, so tests can count exactly what must reach the DLQ (unit test).
- [ ] AC8: Given the stack is running, when the developer asks for DLQ status, then they see the number of rejected events per source and reason.
- [ ] AC9: Given the same dev-size load as spec 002, then the average GPS message is smaller than the JSON baseline and total container memory stays under 6 GB.

## Out of scope
- Late, out-of-order and duplicate events (feature 4: these are valid, not DLQ material).
- Replaying DLQ events back into the pipeline after a fix (feature 11).
- Business rules on order changes (for example, status order); CDC only gets the "not decodable" check.
- AWS Glue Schema Registry (feature 10; ADR 0008 stays Proposed until then).
- Alerting on DLQ growth (feature 8).

## Decisions that change
- Specs 001 and 002 (out of scope "JSON for now"): topics move from JSON to registry-encoded Avro. Topic names keep `.v1`; a breaking change would need a new `.v2` topic.
- Local data written as JSON before this feature is not migrated; the dev stack is reset once.

## Resolved questions (approved 2026-11-18)
1. Avro for **both GPS and CDC** topics.
2. DLQ: **one Kafka topic per source** (`dlq.<source topic>`, 7-day retention) **plus** the Iceberg table `bronze.dlq_events`.
3. Invalid = **not decodable**, or **breaks a rule in AC4**. Late and duplicate events are valid (feature 4).
4. Compatibility is checked by the registry **and offline in CI** against the last committed schema.
5. Schema-evolution demo: optional `battery_pct` on GPS pings, not yet stored in bronze.
