{#- One row per depot and local day (spec 007 AC6). Recomputed from fct_delivery for the open
    days (intraday) or for the day being closed (close); closed days are not touched otherwise. -#}
-- depends_on: {{ ref('closed_days') }}
{%- set close = var('mode') == 'close' %}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['depot_id', 'local_date'],
    pre_hook="{{ clear_days('open') }}",
    properties=partitioned_by('local_date'),
    partitioned_by=['local_date'],
    table_type='iceberg',
    on_schema_change='fail',
) }}

with facts as (
    select * from {{ ref('fct_delivery') }}
    {% if close %}
        where local_date = date '{{ var("day") }}'
    {% elif is_incremental() %}
        where local_date not in (select local_date from {{ ref('closed_days') }})
    {% endif %}
),

-- stops per hour (resolved question 7): a van's closed orders ÷ hours from its first dispatch
-- to its last close that day, then averaged over the depot's vans
van_days as (
    select
        depot_id,
        local_date,
        van_id,
        count(*) / greatest(date_diff('second', min(dispatched_at), max(closed_at)) / 3600e0, 1e0 / 60)
            as stops_per_hour
    from facts
    where dispatched_at is not null
    group by depot_id, local_date, van_id
),

depot_stops as (
    select
        depot_id,
        local_date,
        avg(stops_per_hour) as stops_per_hour
    from van_days
    group by depot_id, local_date
),

late as (
    select
        *,
        outcome = 'delivered' and not is_on_time as is_late
    from facts
)

select
    f.depot_id,
    f.local_date,
    count(*) as orders_closed,
    count_if(f.outcome = 'delivered') as delivered,
    count_if(f.outcome = 'failed') as failed,
    count_if(f.is_on_time) as on_time,
    count_if(f.is_on_time) / cast(count(*) as double) as on_time_rate,        -- BR-2
    count_if(f.outcome = 'failed') / cast(count(*) as double) as failure_rate,
    avg(f.minutes_late) filter (where f.is_late) as avg_minutes_late,
    max(s.stops_per_hour) as stops_per_hour,
    count(f.eta_error_t15_min) as orders_with_t15,
    avg(abs(f.eta_error_t15_min)) as eta_mae_t15_min,
    approx_percentile(abs(f.eta_error_t15_min), 0.9) as eta_p90_t15_min,
    avg(abs(f.eta_error_t30_min)) as eta_mae_t30_min,
    approx_percentile(abs(f.eta_error_t30_min), 0.9) as eta_p90_t30_min,
    count(f.first_alert_at) as orders_alerted,
    -- BR-5: precision = alerted orders that were late; recall = late orders warned >= 15 min
    -- ahead, over the late orders that could be warned (spec 006, revised)
    count_if(f.first_alert_at is not null and f.is_late)
    / cast(nullif(count(f.first_alert_at), 0) as double) as alert_precision,
    count_if(f.is_late and f.is_warnable and f.alert_lead_min >= 15)
    / cast(nullif(count_if(f.is_late and f.is_warnable), 0) as double) as alert_recall,
    count_if(f.is_late and f.alert_lead_min >= 15)
    / cast(nullif(count_if(f.is_late), 0) as double) as alert_recall_all_late,
    {{ 'true' if close else 'false' }} as is_final
from late as f
left join depot_stops as s on f.depot_id = s.depot_id and f.local_date = s.local_date
group by f.depot_id, f.local_date
