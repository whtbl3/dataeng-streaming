-- One row per version of a van (SCD Type 2): facts join the version valid when they closed.
select
    van_key,
    van_id,
    depot_id,
    plate,
    capacity,
    valid_from,
    valid_to,
    valid_to is null as is_current
from {{ ref('stg_van_versions') }}
