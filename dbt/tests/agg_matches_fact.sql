-- AC6: every daily figure equals the same figure computed by hand from the fact rows.
with by_hand as (
    select
        depot_id,
        local_date,
        count(*) as orders_closed,
        count_if(is_on_time) / cast(count(*) as double) as on_time_rate,
        count_if(outcome = 'failed') / cast(count(*) as double) as failure_rate,
        avg(abs(eta_error_t15_min)) as eta_mae_t15_min,
        count_if(first_alert_at is not null and outcome = 'delivered' and not is_on_time)
            / cast(nullif(count(first_alert_at), 0) as double) as alert_precision
    from {{ ref('fct_delivery') }}
    group by depot_id, local_date
)

select a.depot_id, a.local_date
from {{ ref('agg_depot_daily') }} as a
left join by_hand as h on h.depot_id = a.depot_id and h.local_date = a.local_date
where h.depot_id is null
    or a.orders_closed <> h.orders_closed
    or abs(a.on_time_rate - h.on_time_rate) > 1e-9
    or abs(a.failure_rate - h.failure_rate) > 1e-9
    or abs(coalesce(a.eta_mae_t15_min, -1) - coalesce(h.eta_mae_t15_min, -1)) > 1e-9
    or abs(coalesce(a.alert_precision, -1) - coalesce(h.alert_precision, -1)) > 1e-9
