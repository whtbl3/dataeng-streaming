{#- One row per closed order (requirements section 4, spec 007 AC2).
    intraday: MERGE the orders whose silver row, alert or ETA mark changed in the last
      `lookback_hours` of commit time (system time, so a late change of any age is absorbed),
      never touching a closed day; rows are provisional (is_final = false).
    close: overwrite the whole local-day partition `day`, final (is_final = true). -#}
-- depends_on: {{ ref('closed_days') }}
{%- set close = var('mode') == 'close' %}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert' if close else 'merge',
    unique_key='local_date' if close else 'order_id',
    properties=partitioned_by('local_date'),
    on_schema_change='fail',
) }}

with orders as (
    select * from {{ ref('stg_orders_closed') }}
    {% if close %}
        where local_date = date '{{ var("day") }}'
    {% elif is_incremental() %}
        where
            local_date not in (select local_date from {{ ref('closed_days') }})
            and (
                last_commit_ts >= current_timestamp - interval '{{ var("lookback_hours") }}' hour
                or order_id in (
                    select order_id from {{ ref('stg_alert_events') }}
                    where event_ts >= current_timestamp - interval '{{ var("lookback_hours") }}' hour
                    union all
                    select order_id from {{ ref('stg_eta_marks') }}
                    where event_ts >= current_timestamp - interval '{{ var("lookback_hours") }}' hour
                )
            )
    {% endif %}
),

marks as (
    select
        order_id,
        max(case when mark = 'T30' then eta end) as eta_t30,
        max(case when mark = 'T15' then eta end) as eta_t15
    from {{ ref('stg_eta_marks') }}
    group by order_id
),

alerts as (
    select
        order_id,
        min(raised_at) as first_alert_at
    from {{ ref('stg_alert_events') }}
    where type = 'SLA_RISK' and event = 'raised' and order_id is not null
    group by order_id
)

select
    o.order_id,
    o.local_date,
    coalesce(v.depot_id, o.depot_id) as depot_id,  -- the van's depot when it closed (AC4)
    v.van_key,
    o.van_id,
    o.promised_start,
    o.promised_end,
    o.dispatched_at,
    o.closed_at,
    o.outcome,
    o.outcome = 'delivered' and o.closed_at <= o.promised_end as is_on_time,  -- BR-1, BR-2
    case
        when o.outcome = 'delivered'
            then greatest({{ minutes_between('o.promised_end', 'o.closed_at') }}, 0e0)
    end as minutes_late,
    -- ETA error (spec 007 resolved question 6): predicted - actual, minutes, signed; the
    -- order system records the close, so the close time stands for the arrival
    {{ minutes_between('o.closed_at', 'm.eta_t30') }} as eta_error_t30_min,
    {{ minutes_between('o.closed_at', 'm.eta_t15') }} as eta_error_t15_min,
    a.first_alert_at,
    {{ minutes_between('a.first_alert_at', 'o.promised_end') }} as alert_lead_min,
    {{ minutes_between('o.dispatched_at', 'o.promised_end') }} >= 15 as is_warnable,
    o.dest_area,
    {{ 'true' if close else 'false' }} as is_final,
    o.last_commit_ts
from orders as o
left join {{ ref('dim_van') }} as v
    on
        o.van_id = v.van_id
        and o.closed_at >= v.valid_from
        and (v.valid_to is null or o.closed_at < v.valid_to)
left join marks as m on o.order_id = m.order_id
left join alerts as a on o.order_id = a.order_id
