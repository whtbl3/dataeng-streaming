# 0013: Control tower as one state per van, with a distance/speed ETA
Date: 2026-11-19 · Status: Final (spec 006)

## Context
Dispatchers need a live ETA per open order and an alert before an order breaches its promised
window (intent, BR-3), plus an alert when a van stands still where it should not. GPS ping →
alert must take under 10 s (NFR-1). The same day sent clean or dirty must give the same alerts
(spec 006 AC8), and a restart must neither repeat alerts nor raise alerts for old events (NFR-4).

## Decision
- **Read Kafka, not silver.** The job reads the GPS topic and the order change topic directly.
  Silver is 1-2 minutes behind. Bad GPS messages are skipped here; bronze dead-letters them.
- **One state per van** (Flink keyed state, RocksDB): its newest ping, the moving speeds of the
  last 10 minutes, its open orders and their alert state. All rules are pure Python
  (`flink/udf/tower.py`); the Flink job only feeds events in and writes outputs out. The same
  function replays a whole simulated day in tests, so alert quality is measured, not guessed.
- **No timers, no global watermark.** Pings keep coming every 5 s while a van stands still, so
  "idle for 10 minutes" is decided on the van's own pings. With 6 partitions and few live vans, a
  global watermark would stall on idle partitions.
- **ETA v1:** straight-line distance through the van's remaining stops in stop order. The leg
  being driven uses the van's mean moving speed of the last 10 minutes; later legs use the typical
  fleet speed (25 km/h); each stop before the order's adds 4 minutes.
- **Alerts:**
  - SLA risk when the ETA is at least 1 minute past the promised end; once per order; raised
    again only after 10 minutes resolved (BR-3).
  - Abnormal stop: under 3 km/h for 10 minutes without a break (no gap over 2 minutes, no move
    over 150 m), more than 150 m from the depot and from every open stop.
  - Alert id = type, subject and the event time of the raise, so a replay upserts the same row.
- **Alert horizon:** an event older than 10 minutes in processing time updates state and ETAs but
  raises no alert (NFR-4).
- **Outputs:** idempotent JDBC upserts into `serving.vehicle_latest`, `serving.order_eta`
  (deleted when an order closes) and `serving.alerts`; `alerts.v1` (registry Avro, exactly-once).
- **Depot position:** a cached lookup join on the Kafka key against `serving.van_depots` (static).

## Options considered
- **Keyed state per van in Python (chosen):** small state (about 200 vans), deterministic per
  van, testable without Flink. Costs one more Python worker (about 300 MB).
- **SQL joins:** a temporal join must be keyed on the order, not the van, and a regular join gives
  results in arrival order; neither detects "idle for 10 minutes".
- **Event-time timers:** exact idle detection even without pings, but they depend on the global
  watermark, which stalls on idle partitions, and they add 30 s of watermark delay.
- **ETA with the recent speed on every leg:** simpler, but a jam on the current leg inflated every
  later stop: 33 % precision against 82-86 % (spec 006 spike S1).

## Consequences
- \+ Measured on the simulated demo day: precision 82 %, recall 100 % over late orders that can be
  warned 15 minutes ahead (61 % over all late orders); alert latency median 1.1 s, p95 1.5 s live.
- − 30-40 % of the simulated late orders end their window less than 15 minutes after dispatch, so
  no rule starting at dispatch can warn them in time. An ETA before dispatch would need the planned
  departure, which the order data does not carry.
- − After an offline burst, flushed pings arrive after the order changes that happened meanwhile;
  their ETAs use a shorter route, so borderline SLA alerts can differ from a clean run.
- − A van that sends no pings raises no abnormal-stop alert (offline is not stopped).
- − Python in the alert path: decoding and rules cost about 1 s of the latency budget.
- Reverse: cancel the job; `DROP SCHEMA serving CASCADE`; delete the topic `alerts.v1`.
