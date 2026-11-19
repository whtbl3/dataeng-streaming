-- One ETA per order and mark (T30, T15): the first one written wins.
select order_id, van_id, mark, eta, promised_end, event_ts
from (
    select
        *,
        row_number() over (partition by order_id, mark order by event_ts, eta) as rn
    from {{ source('bronze', 'eta_marks') }}
)
where rn = 1
