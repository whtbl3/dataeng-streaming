-- Composite keys of the staging models (one alert event, one mark per order).
select
    'stg_alert_events' as model,
    alert_id || '/' || event as duplicate_key
from {{ ref('stg_alert_events') }}
group by alert_id, event
having count(*) > 1
union all
select
    'stg_eta_marks' as model,
    order_id || '/' || mark as duplicate_key
from {{ ref('stg_eta_marks') }}
group by order_id, mark
having count(*) > 1
