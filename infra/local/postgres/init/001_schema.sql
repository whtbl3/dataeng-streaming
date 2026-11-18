-- Order database (the "other team's" OLTP system, ADR 0004). Runs once on first start.
CREATE TABLE depots (
    depot_id   TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    lat        DOUBLE PRECISION NOT NULL,
    lon        DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE vans (
    van_id     TEXT PRIMARY KEY,           -- equals the GPS vehicle_id
    depot_id   TEXT NOT NULL REFERENCES depots,
    plate      TEXT NOT NULL UNIQUE,
    capacity   INTEGER NOT NULL CHECK (capacity > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id            TEXT PRIMARY KEY,
    depot_id            TEXT NOT NULL REFERENCES depots,
    van_id              TEXT REFERENCES vans,
    stop_seq            INTEGER,
    dest_lat            DOUBLE PRECISION NOT NULL,
    dest_lon            DOUBLE PRECISION NOT NULL,
    promised_start      TIMESTAMPTZ NOT NULL,
    promised_end        TIMESTAMPTZ NOT NULL,
    status              TEXT NOT NULL CHECK (status IN
                          ('created', 'assigned', 'out_for_delivery', 'delivered', 'failed')),
    failure_reason      TEXT,
    created_at          TIMESTAMPTZ NOT NULL,   -- business (simulated) time
    assigned_at         TIMESTAMPTZ,
    out_for_delivery_at TIMESTAMPTZ,
    delivered_at        TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL,
    CHECK (promised_end > promised_start)
);
CREATE INDEX orders_status_idx ON orders (status);
CREATE INDEX orders_van_idx ON orders (van_id);

-- Full before-images for every order update (complete audit trail in CDC).
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Debezium reads this publication (publication.autocreate.mode = disabled).
CREATE PUBLICATION ct_cdc FOR TABLE depots, vans, orders;
