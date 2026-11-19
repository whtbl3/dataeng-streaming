-- Serving tables for dispatchers and Grafana (spec 006, ADR 0007). Written by the Flink job
-- `control_tower` with idempotent upserts on the primary keys. Idempotent: applied by the
-- `serving-init` service on every `make up` (initdb scripts only run on an empty volume).
CREATE SCHEMA IF NOT EXISTS serving;

-- written_at: when the row last changed in Postgres (wall clock), for freshness and latency.
CREATE OR REPLACE FUNCTION serving.touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.written_at := clock_timestamp();
    RETURN NEW;
END $$;

-- One row per van: its newest ping (an older ping never overwrites it).
CREATE TABLE IF NOT EXISTS serving.vehicle_latest (
    van_id      TEXT PRIMARY KEY,
    event_ts    TIMESTAMPTZ NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    speed_kmh   DOUBLE PRECISION NOT NULL,
    written_at  TIMESTAMPTZ
);

-- One row per open order on a van; deleted when the order closes or leaves the van.
CREATE TABLE IF NOT EXISTS serving.order_eta (
    order_id     TEXT PRIMARY KEY,
    van_id       TEXT NOT NULL,
    status       TEXT NOT NULL,            -- assigned (no ETA yet) or out_for_delivery
    promised_end TIMESTAMPTZ NOT NULL,
    eta          TIMESTAMPTZ,              -- NULL until the van is out for delivery
    distance_km  DOUBLE PRECISION,
    event_ts     TIMESTAMPTZ NOT NULL,     -- event time of the ping the ETA was computed from
    written_at   TIMESTAMPTZ
);

-- One row per alert. raised_written_at is set once, when the alert first reaches Postgres.
CREATE TABLE IF NOT EXISTS serving.alerts (
    alert_id          TEXT PRIMARY KEY,    -- type:subject:raise epoch (deterministic)
    type              TEXT NOT NULL CHECK (type IN ('SLA_RISK', 'ABNORMAL_STOP')),
    van_id            TEXT NOT NULL,
    order_id          TEXT,
    raised_at         TIMESTAMPTZ NOT NULL, -- event time
    resolved_at       TIMESTAMPTZ,          -- event time; NULL while open
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    detail            TEXT,
    raised_written_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    written_at        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS alerts_open_idx ON serving.alerts (type) WHERE resolved_at IS NULL;

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['vehicle_latest', 'order_eta', 'alerts'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS touch ON serving.%I', t);
        EXECUTE format(
            'CREATE TRIGGER touch BEFORE INSERT OR UPDATE ON serving.%I '
            'FOR EACH ROW EXECUTE FUNCTION serving.touch()', t);
    END LOOP;
END $$;

-- Depot position per van, for the abnormal-stop rule (a cached lookup join in Flink).
CREATE OR REPLACE VIEW serving.van_depots AS
SELECT v.van_id, d.lat AS depot_lat, d.lon AS depot_lon
FROM public.vans v
JOIN public.depots d USING (depot_id);
