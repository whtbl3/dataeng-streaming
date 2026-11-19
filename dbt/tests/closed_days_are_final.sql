-- Rule 4: a closed day's facts and aggregates are all final.
select
    f.local_date,
    'fct_delivery' as model
from {{ ref('fct_delivery') }} as f
inner join {{ ref('closed_days') }} as c on f.local_date = c.local_date
where not f.is_final
union all
select
    a.local_date,
    'agg_depot_daily' as model
from {{ ref('agg_depot_daily') }} as a
inner join {{ ref('closed_days') }} as c on a.local_date = c.local_date
where not a.is_final
