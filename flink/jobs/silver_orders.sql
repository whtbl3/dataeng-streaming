-- silver_orders: bronze.orders_cdc → silver.orders + silver.order_status_history (spec 004).
-- The CDC changelog is ordered by commit position (source_lsn), never by arrival, so duplicates
-- and changes that reach the job out of order cannot make an older version win (rule 2).
-- Both sinks upsert on the tables' identifier fields (ADR 0011); a rebuild replays all of bronze.
SET 'pipeline.name' = 'silver_orders';
SET 'table.exec.state.ttl' = '7 d';

CREATE TEMPORARY VIEW order_versions AS
SELECT `after`.order_id            AS order_id,
       `after`.created_at          AS created_at,
       `after`.depot_id            AS depot_id,
       `after`.van_id              AS van_id,
       `after`.stop_seq            AS stop_seq,
       `after`.dest_lat            AS dest_lat,
       `after`.dest_lon            AS dest_lon,
       `after`.promised_start      AS promised_start,
       `after`.promised_end        AS promised_end,
       `after`.status              AS status,
       `after`.failure_reason      AS failure_reason,
       `after`.assigned_at         AS assigned_at,
       `after`.out_for_delivery_at AS out_for_delivery_at,
       `after`.delivered_at        AS delivered_at,
       `after`.failed_at           AS failed_at,
       `after`.updated_at          AS updated_at,
       source_lsn,
       commit_ts
FROM lakehouse.bronze.orders_cdc
     /*+ OPTIONS('streaming' = 'true', 'monitor-interval' = '10s',
                 'starting-strategy' = 'TABLE_SCAN_THEN_INCREMENTAL',
                 'streaming-skip-delete-snapshots' = 'true',
                 'streaming-skip-overwrite-snapshots' = 'true') */
-- Orders are never deleted, and a delete would carry no after image.
WHERE `after` IS NOT NULL;

-- Every version's after image holds the time of each status reached so far, so a snapshot read
-- of an order that is already delivered still yields its whole history.
CREATE TEMPORARY VIEW status_points AS
SELECT v.order_id, s.status, s.status_ts, v.source_lsn, v.commit_ts
FROM order_versions AS v
CROSS JOIN UNNEST(ARRAY[
  ROW(CAST('created' AS STRING),          v.created_at),
  ROW(CAST('assigned' AS STRING),         v.assigned_at),
  ROW(CAST('out_for_delivery' AS STRING), v.out_for_delivery_at),
  ROW(CAST('delivered' AS STRING),        v.delivered_at),
  ROW(CAST('failed' AS STRING),           v.failed_at)
]) AS s (status, status_ts)
WHERE s.status_ts IS NOT NULL;

EXECUTE STATEMENT SET
BEGIN
  -- Latest version per order: the highest commit position wins.
  INSERT INTO lakehouse.silver.orders /*+ OPTIONS('upsert-enabled' = 'true') */
  SELECT order_id, created_at, depot_id, van_id, stop_seq, dest_lat, dest_lon, promised_start,
         promised_end, status, failure_reason, assigned_at, out_for_delivery_at, delivered_at,
         failed_at, updated_at, source_lsn AS last_lsn, commit_ts AS last_commit_ts
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_lsn DESC) AS rn
    FROM order_versions
  )
  WHERE rn = 1;

  -- One row per status reached, from the commit that first recorded it.
  INSERT INTO lakehouse.silver.order_status_history /*+ OPTIONS('upsert-enabled' = 'true') */
  SELECT order_id, status, status_ts, source_lsn, commit_ts
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status, status_ts
                                 ORDER BY source_lsn ASC) AS rn
    FROM status_points
  )
  WHERE rn = 1;
END;
