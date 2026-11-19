-- One row per version of a van (SCD Type 2) from the CDC changelog (AC4). A new version starts
-- when the depot, plate or capacity changes; copies and no-op updates collapse. The first
-- version is valid from the beginning of time, so history older than the CDC snapshot joins.
with changes as (
    select distinct
        "after".van_id as van_id,
        "after".depot_id as depot_id,
        "after".plate as plate,
        "after".capacity as capacity,
        source_lsn,
        coalesce("after".updated_at, commit_ts) as changed_at
    from {{ source('bronze', 'vans_cdc') }}
    where "after" is not null
),

marked as (
    select
        *,
        lag(depot_id) over (partition by van_id order by source_lsn) as prev_depot,
        lag(plate) over (partition by van_id order by source_lsn) as prev_plate,
        lag(capacity) over (partition by van_id order by source_lsn) as prev_capacity,
        row_number() over (partition by van_id order by source_lsn) as change_no
    from changes
),

versions as (
    select *
    from marked
    where change_no = 1
        or depot_id is distinct from prev_depot
        or plate is distinct from prev_plate
        or capacity is distinct from prev_capacity
)

select
    van_id || '#' || cast(row_number() over (partition by van_id order by source_lsn) as varchar)
        as van_key,
    van_id,
    depot_id,
    plate,
    capacity,
    case
        when change_no = 1 then timestamp '1970-01-01 00:00:00 UTC' else changed_at
    end as valid_from,
    lead(changed_at) over (partition by van_id order by source_lsn) as valid_to
from versions
