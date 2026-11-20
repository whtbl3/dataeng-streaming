-- Panel queries of the dashboard design (docs/design/dashboards.md, spec 008).
-- One block per panel: a "-- panel: <dashboard>.<panel> (<source>)" line, then the query.
-- Sources: postgres = the serving database, trino = the lakehouse. A dashboard tool replaces
-- the fixed windows (15 min, 14 days) with its time range, and $final_only with a toggle.
-- tests/integration/test_health.py runs every block against the running stack.

-- panel: dispatch.van_map (postgres)
SELECT van_id, lat, lon, speed_kmh, event_ts,
       round(extract(epoch FROM now() - event_ts)) AS age_s
FROM serving.vehicle_latest
WHERE event_ts > now() - INTERVAL '15 minutes';

-- panel: dispatch.open_alerts (postgres)
SELECT type, van_id, order_id, raised_at, detail,
       round(extract(epoch FROM now() - raised_at) / 60) AS open_min
FROM serving.alerts
WHERE resolved_at IS NULL
ORDER BY raised_at DESC
LIMIT 50;

-- panel: dispatch.at_risk_orders (postgres)
SELECT order_id, van_id, eta, promised_end,
       round(extract(epoch FROM eta - promised_end) / 60) AS minutes_late,
       distance_km
FROM serving.order_eta
WHERE eta > promised_end
ORDER BY minutes_late DESC
LIMIT 50;

-- panel: dispatch.alerts_per_hour (postgres)
SELECT date_trunc('hour', raised_at) AS hour, type, count(*) AS alerts
FROM serving.alerts
WHERE raised_at > now() - INTERVAL '24 hours'
GROUP BY 1, 2
ORDER BY 1;

-- panel: kpi.depot_day (trino)
SELECT local_date, depot_id, orders_closed, on_time_rate, failure_rate, avg_minutes_late,
       eta_mae_t15_min, eta_p90_t15_min, stops_per_hour, alert_precision, alert_recall,
       CASE WHEN is_final THEN 'final' ELSE 'provisional' END AS status
FROM lakehouse.gold.agg_depot_daily
WHERE local_date >= current_date - INTERVAL '14' DAY
    -- the tool's "final only" toggle: AND (is_final OR NOT $final_only)
ORDER BY local_date DESC, depot_id;

-- panel: kpi.on_time_trend (trino)
SELECT local_date,
       sum(on_time) / cast(sum(orders_closed) AS double) AS on_time_rate,
       bool_and(is_final) AS all_final
FROM lakehouse.gold.agg_depot_daily
WHERE local_date >= current_date - INTERVAL '14' DAY
GROUP BY local_date
ORDER BY local_date;

-- panel: kpi.closed_days (trino)
SELECT local_date, closed_at, late_events_absorbed, late_after_close, restatements
FROM lakehouse.gold.closed_days
ORDER BY local_date DESC
LIMIT 14;

-- panel: pipeline.slot_lag (postgres)
SELECT slot_name, active,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
FROM pg_replication_slots;

-- panel: pipeline.newest_row (trino)
SELECT 'bronze.gps_pings' AS layer, max(ingest_ts) AS newest FROM lakehouse.bronze.gps_pings
UNION ALL
SELECT 'silver.gps_pings', max(ingest_ts) FROM lakehouse.silver.gps_pings
UNION ALL
SELECT 'silver.orders', max(last_commit_ts) FROM lakehouse.silver.orders
UNION ALL
SELECT 'gold.fct_delivery', max(last_commit_ts) FROM lakehouse.gold.fct_delivery;

-- panel: pipeline.maintenance (trino)
SELECT task, table_name, max(finished_at) AS last_ok
FROM lakehouse.ops.maintenance_runs
WHERE status = 'ok'
GROUP BY task, table_name
ORDER BY task, table_name;
