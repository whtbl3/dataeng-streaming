-- One row per closed (delivered or failed) order, from silver (latest version per order).
-- local_date is the business day of the close (BR-4). The destination is kept only as a
-- ~150 m area (NFR-6): a Bing tile quadkey at zoom 18; exact coordinates never leave silver.
select
    order_id,
    depot_id,
    van_id,
    stop_seq,
    promised_start,
    promised_end,
    status as outcome,
    failure_reason,
    out_for_delivery_at as dispatched_at,
    coalesce(delivered_at, failed_at) as closed_at,
    {{ local_date('coalesce(delivered_at, failed_at)') }} as local_date,
    {{ area_of("dest_lat", "dest_lon") }} as dest_area,
    last_lsn,
    last_commit_ts
from {{ source('silver', 'orders') }}
where status in ('delivered', 'failed')
    and coalesce(delivered_at, failed_at) is not null
