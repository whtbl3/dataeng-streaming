-- Rule 4: a closed day's facts and aggregates are all final.
select f.local_date, 'fct_delivery' as model
from {{ ref('fct_delivery') }} as f
join {{ ref('closed_days') }} as c on c.local_date = f.local_date
where not f.is_final
union all
select a.local_date, 'agg_depot_daily'
from {{ ref('agg_depot_daily') }} as a
join {{ ref('closed_days') }} as c on c.local_date = a.local_date
where not a.is_final
