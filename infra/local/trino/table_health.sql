-- `make table-health` (spec 005 AC10): is each table compacted and cleaned up?
--   data_files / delete_files   files in the current snapshot (delete files come from upserts)
--   avg_file_mb                 average data file size (small files mean compaction is due)
--   snapshots                   snapshots kept (expiry keeps 7 days)
--   last_*                      when each maintenance task last succeeded on the table
WITH files AS (
    SELECT 'silver.gps_pings' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.silver."gps_pings$snapshots") AS snapshots
    FROM lakehouse.silver."gps_pings$files"
    UNION ALL
    SELECT 'silver.orders' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.silver."orders$snapshots") AS snapshots
    FROM lakehouse.silver."orders$files"
    UNION ALL
    SELECT 'silver.order_status_history' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.silver."order_status_history$snapshots") AS snapshots
    FROM lakehouse.silver."order_status_history$files"
    UNION ALL
    SELECT 'bronze.gps_pings' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.bronze."gps_pings$snapshots") AS snapshots
    FROM lakehouse.bronze."gps_pings$files"
    UNION ALL
    SELECT 'bronze.orders_cdc' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.bronze."orders_cdc$snapshots") AS snapshots
    FROM lakehouse.bronze."orders_cdc$files"
    UNION ALL
    SELECT 'bronze.depots_cdc' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.bronze."depots_cdc$snapshots") AS snapshots
    FROM lakehouse.bronze."depots_cdc$files"
    UNION ALL
    SELECT 'bronze.vans_cdc' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.bronze."vans_cdc$snapshots") AS snapshots
    FROM lakehouse.bronze."vans_cdc$files"
    UNION ALL
    SELECT 'bronze.dlq_events' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.bronze."dlq_events$snapshots") AS snapshots
    FROM lakehouse.bronze."dlq_events$files"
    UNION ALL
    SELECT 'bronze.alert_events' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.bronze."alert_events$snapshots") AS snapshots
    FROM lakehouse.bronze."alert_events$files"
    UNION ALL
    SELECT 'bronze.eta_marks' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.bronze."eta_marks$snapshots") AS snapshots
    FROM lakehouse.bronze."eta_marks$files"
    UNION ALL
    SELECT 'ops.maintenance_runs' AS table_name, count_if(content = 0) AS data_files,
           count_if(content <> 0) AS delete_files,
           round(coalesce(avg(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) / 1048576e0, 2) AS avg_file_mb,
           (SELECT count(*) FROM lakehouse.ops."maintenance_runs$snapshots") AS snapshots
    FROM lakehouse.ops."maintenance_runs$files"
),
runs AS (
    SELECT table_name,
           max(CASE WHEN task = 'compact' THEN finished_at END) AS last_compact,
           max(CASE WHEN task = 'manifests' THEN finished_at END) AS last_manifests,
           max(CASE WHEN task = 'expire' THEN finished_at END) AS last_expire,
           max(CASE WHEN task = 'orphans' THEN finished_at END) AS last_orphans,
           max(CASE WHEN task = 'retention' THEN finished_at END) AS last_retention
    FROM lakehouse.ops.maintenance_runs
    WHERE status = 'ok'
    GROUP BY table_name
)
SELECT f.table_name, f.data_files, f.delete_files, f.avg_file_mb, f.snapshots,
       date_format(r.last_compact, '%Y-%m-%d %H:%i') AS last_compact,
       date_format(r.last_manifests, '%Y-%m-%d %H:%i') AS last_manifests,
       date_format(r.last_expire, '%Y-%m-%d %H:%i') AS last_expire,
       date_format(r.last_orphans, '%Y-%m-%d %H:%i') AS last_orphans,
       date_format(r.last_retention, '%Y-%m-%d %H:%i') AS last_retention
FROM files f
LEFT JOIN runs r ON r.table_name = f.table_name
ORDER BY f.table_name;
