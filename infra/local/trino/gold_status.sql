-- `make gold-status` (spec 007): what gold knows, per local day, and whether the day is closed.
SELECT
    f.local_date,
    count(*) AS orders_closed,
    round(count_if(f.is_on_time) / cast(count(*) AS double), 3) AS on_time_rate,
    count_if(f.is_final) AS final_rows,
    max(c.closed_at) AS closed_at,
    max(c.late_events_absorbed) AS late_absorbed,
    max(c.late_after_close) AS late_after_close,
    max(c.restatements) AS restatements
FROM lakehouse.gold.fct_delivery AS f
LEFT JOIN lakehouse.gold.closed_days AS c ON c.local_date = f.local_date
GROUP BY f.local_date
ORDER BY f.local_date DESC
LIMIT 14;
