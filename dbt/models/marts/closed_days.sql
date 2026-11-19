{#- One row per closed local day (rule 4, ADR 0006).
    close mode (`make close` / `make reclose`): upsert the day being closed.
    intraday mode: refresh late_after_close for every closed day (changes committed after the
    close; the closed day itself does not change until an explicit restatement). -#}
{{ config(
    materialized='incremental', incremental_strategy='merge', unique_key='local_date',
    on_schema_change='fail',
) }}

{%- set close = var('mode') == 'close' %}

with orders as (
    select local_date, last_commit_ts from {{ ref('stg_orders_closed') }}
),

{% if close %}
closing as (
    select
        date '{{ var("day") }}' as local_date,
        cast(current_timestamp as timestamp(6) with time zone) as closed_at,
        -- changes that arrived after the day ended (local midnight) and before this close
        (
            select count(*) from orders
            where local_date = date '{{ var("day") }}'
                and at_timezone(last_commit_ts, '{{ var("local_tz") }}')
                    >= cast(date_add('day', 1, date '{{ var("day") }}') as timestamp)
        ) as late_events_absorbed,
        cast(0 as bigint) as late_after_close,
        {% if is_incremental() %}
        coalesce(
            (select restatements from {{ this }} where local_date = date '{{ var("day") }}'),
            -1
        ) + 1
        {% else %}
        0
        {% endif %} as restatements
)
select * from closing
{% if not var('restatement') and is_incremental() %}
-- a plain close of an already closed day changes nothing (idempotent); reclose restates it
where not exists (select 1 from {{ this }} where local_date = date '{{ var("day") }}')
{% endif %}

{% elif is_incremental() %}
refreshed as (
    select
        c.local_date,
        c.closed_at,
        c.late_events_absorbed,
        (select count(*) from orders o
         where o.local_date = c.local_date and o.last_commit_ts > c.closed_at) as late_after_close,
        c.restatements
    from {{ this }} as c
)
select * from refreshed

{% else %}
empty as (
    select
        cast(null as date) as local_date,
        cast(null as timestamp(6) with time zone) as closed_at,
        cast(null as bigint) as late_events_absorbed,
        cast(null as bigint) as late_after_close,
        cast(null as integer) as restatements
)
select * from empty where false
{% endif %}
