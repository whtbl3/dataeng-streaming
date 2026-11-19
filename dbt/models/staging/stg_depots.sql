-- One row per depot: its latest version from the CDC changelog.
select depot_id, name, lat, lon
from (
    select
        "after".depot_id as depot_id,
        "after".name as name,
        "after".lat as lat,
        "after".lon as lon,
        row_number() over (partition by "after".depot_id order by source_lsn desc) as rn
    from {{ source('bronze', 'depots_cdc') }}
    where "after" is not null
)
where rn = 1
