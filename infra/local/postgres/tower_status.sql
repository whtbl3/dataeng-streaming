-- `make tower-status` (spec 006 AC11): what the control tower currently shows dispatchers.
\echo '== open orders and alerts'
SELECT 'open orders with an ETA' AS item, count(*) AS n FROM serving.order_eta WHERE eta IS NOT NULL
UNION ALL
SELECT 'open orders not dispatched', count(*) FROM serving.order_eta WHERE eta IS NULL
UNION ALL
SELECT 'open orders at risk (ETA after promised end)', count(*)
FROM serving.order_eta WHERE eta > promised_end
UNION ALL
SELECT 'open SLA_RISK alerts', count(*)
FROM serving.alerts WHERE type = 'SLA_RISK' AND resolved_at IS NULL
UNION ALL
SELECT 'open ABNORMAL_STOP alerts', count(*)
FROM serving.alerts WHERE type = 'ABNORMAL_STOP' AND resolved_at IS NULL;

\echo '== newest position per van (20 most recent)'
SELECT van_id,
       to_char(event_ts AT TIME ZONE 'Asia/Ho_Chi_Minh', 'YYYY-MM-DD HH24:MI:SS') AS local_time,
       round(extract(epoch FROM now() - event_ts)) AS age_s,
       round(speed_kmh::numeric, 1) AS speed_kmh
FROM serving.vehicle_latest
ORDER BY event_ts DESC
LIMIT 20;
