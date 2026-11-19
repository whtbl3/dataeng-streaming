-- Spec 005: table layout for scale, and the maintenance log. Idempotent; runs before flink-submit
-- resumes the jobs, so nothing writes to these tables while it runs.

-- GPS: one partition a day instead of day x 16 buckets (files of about 5 MB were far below the
-- 128-512 MB Iceberg aims for), and files sorted by van then time when compacted. Fresh installs
-- already get this from 001/004; existing tables evolve here.
ALTER TABLE lakehouse.silver.gps_pings
    SET PROPERTIES partitioning = ARRAY['day(event_ts)'], sorted_by = ARRAY['vehicle_id', 'event_ts'];
ALTER TABLE lakehouse.bronze.gps_pings SET PROPERTIES sorted_by = ARRAY['vehicle_id', 'event_ts'];

-- An equality delete applies only to data files of its own partition spec, so a late copy's upsert
-- would not reach a row still in an old-spec file. Rewrite every file into the current spec now.
-- Cheap when there is nothing to do: maintenance keeps the files compacted (ADR 0012).
ALTER TABLE lakehouse.silver.gps_pings EXECUTE optimize(file_size_threshold => '128MB');

-- One row per maintenance task run (`make table-health`, spec 005 AC10).
CREATE SCHEMA IF NOT EXISTS lakehouse.ops;
CREATE TABLE IF NOT EXISTS lakehouse.ops.maintenance_runs (
    task        VARCHAR,                      -- compact, manifests, expire, orphans, retention
    table_name  VARCHAR,                      -- e.g. silver.gps_pings
    started_at  TIMESTAMP(6) WITH TIME ZONE,
    finished_at TIMESTAMP(6) WITH TIME ZONE,
    status      VARCHAR,                      -- ok, failed
    detail      VARCHAR                       -- the procedure's metrics, or the error
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(started_at)']);
