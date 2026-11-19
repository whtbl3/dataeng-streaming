-- `make silver-status` (spec 004 AC10): what silver removed or flagged, per table and local day.
--   bronze_rows   rows that arrived (all copies, all versions)
--   silver_rows   rows kept (one per ping, per order, per status reached)
--   removed       bronze_rows - silver_rows (duplicate copies, older order versions)
--   late_rows     silver pings that arrived more than 30 s after they happened
--   conflicting   keys whose copies carry different values (the first copy was kept)
WITH gps_bronze AS (
    SELECT date(event_ts AT TIME ZONE 'Asia/Ho_Chi_Minh') AS local_day,
           count(*) AS bronze_rows,
           count(DISTINCT (vehicle_id, event_ts)) AS keys
    FROM lakehouse.bronze.gps_pings
    GROUP BY 1
),
gps_conflicts AS (
    SELECT local_day, count(*) AS conflicting
    FROM (
        SELECT date(event_ts AT TIME ZONE 'Asia/Ho_Chi_Minh') AS local_day
        FROM lakehouse.bronze.gps_pings
        GROUP BY vehicle_id, event_ts
        HAVING count(DISTINCT (lat, lon, speed_kmh, heading_deg)) > 1
    )
    GROUP BY 1
),
gps_silver AS (
    SELECT date(event_ts AT TIME ZONE 'Asia/Ho_Chi_Minh') AS local_day,
           count(*) AS silver_rows,
           count_if(is_late) AS late_rows
    FROM lakehouse.silver.gps_pings
    GROUP BY 1
),
orders_bronze AS (
    SELECT date("after".created_at AT TIME ZONE 'Asia/Ho_Chi_Minh') AS local_day,
           count(*) AS bronze_rows
    FROM lakehouse.bronze.orders_cdc
    WHERE "after" IS NOT NULL
    GROUP BY 1
),
orders_silver AS (
    SELECT date(created_at AT TIME ZONE 'Asia/Ho_Chi_Minh') AS local_day, count(*) AS silver_rows
    FROM lakehouse.silver.orders
    GROUP BY 1
),
history_silver AS (
    SELECT date(o.created_at AT TIME ZONE 'Asia/Ho_Chi_Minh') AS local_day, count(*) AS silver_rows
    FROM lakehouse.silver.order_status_history h
    JOIN lakehouse.silver.orders o ON h.order_id = o.order_id
    GROUP BY 1
)
SELECT 'gps_pings' AS silver_table, b.local_day, b.bronze_rows,
       coalesce(s.silver_rows, 0) AS silver_rows,
       b.bronze_rows - coalesce(s.silver_rows, 0) AS removed,
       coalesce(s.late_rows, 0) AS late_rows,
       coalesce(c.conflicting, 0) AS conflicting
FROM gps_bronze b
LEFT JOIN gps_silver s ON s.local_day = b.local_day
LEFT JOIN gps_conflicts c ON c.local_day = b.local_day
UNION ALL
SELECT 'orders', b.local_day, b.bronze_rows, coalesce(s.silver_rows, 0),
       b.bronze_rows - coalesce(s.silver_rows, 0), NULL, NULL
FROM orders_bronze b
LEFT JOIN orders_silver s ON s.local_day = b.local_day
UNION ALL
SELECT 'order_status_history', b.local_day, b.bronze_rows, coalesce(h.silver_rows, 0),
       NULL, NULL, NULL
FROM orders_bronze b
LEFT JOIN history_silver h ON h.local_day = b.local_day
ORDER BY 1, 2
