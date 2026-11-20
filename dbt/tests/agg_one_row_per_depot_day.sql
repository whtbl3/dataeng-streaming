select
    depot_id,
    local_date
from {{ ref('agg_depot_daily') }}
group by depot_id, local_date
having count(*) > 1
