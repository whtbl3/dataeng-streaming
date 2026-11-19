-- One row per local day (BR-4), from the first closed order to tomorrow.
with bounds as (
    select
        coalesce(min(local_date), current_date) as first_day,
        date_add('day', 1, {{ local_date('current_timestamp') }}) as last_day
    from {{ ref('stg_orders_closed') }}
)

select
    d as local_date,
    day_of_week(d) as iso_weekday,
    day_of_week(d) in (6, 7) as is_weekend,
    date_format(d, '%Y-%m') as year_month
from bounds
cross join unnest(sequence(first_day, last_day)) as t (d)
