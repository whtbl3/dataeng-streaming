-- One row per alert event (raised, resolved); a replayed copy collapses (spec 007 AC1).
select distinct
    alert_id,
    event,
    type,
    van_id,
    order_id,
    raised_at,
    resolved_at,
    event_ts
from {{ source('bronze', 'alert_events') }}
