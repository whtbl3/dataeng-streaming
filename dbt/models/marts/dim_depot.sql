-- One row per depot (latest attributes).
select
    depot_id,
    name,
    lat,
    lon
from {{ ref('stg_depots') }}
