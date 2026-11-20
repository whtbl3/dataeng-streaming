-- Control tower history for gold (spec 007 AC1). Written by the Flink job `control_tower`,
-- append-only and exactly-once (Iceberg commits with the checkpoint). Idempotent.

-- Every alert event: one row when an alert is raised and one when it is resolved.
CREATE TABLE IF NOT EXISTS lakehouse.bronze.alert_events (
    alert_id    VARCHAR,
    event       VARCHAR,                      -- raised | resolved
    type        VARCHAR,                      -- SLA_RISK | ABNORMAL_STOP
    van_id      VARCHAR,
    order_id    VARCHAR,                      -- SLA_RISK only
    raised_at   TIMESTAMP(6) WITH TIME ZONE,  -- event time of the raise
    resolved_at TIMESTAMP(6) WITH TIME ZONE,
    lat         DOUBLE,
    lon         DOUBLE,
    detail      VARCHAR,
    event_ts    TIMESTAMP(6) WITH TIME ZONE   -- raised_at or resolved_at
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(event_ts)']);

-- The ETA of an order when its deadline was 30 (T30) and 15 (T15) min away.
CREATE TABLE IF NOT EXISTS lakehouse.bronze.eta_marks (
    order_id     VARCHAR,
    van_id       VARCHAR,
    mark         VARCHAR,                      -- T30 | T15
    eta          TIMESTAMP(6) WITH TIME ZONE,
    promised_end TIMESTAMP(6) WITH TIME ZONE,
    event_ts     TIMESTAMP(6) WITH TIME ZONE   -- the ping the ETA came from
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(event_ts)']);
