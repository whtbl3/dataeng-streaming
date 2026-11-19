-- Composite keys of the staging models (one alert event, one mark per order).
select 'stg_alert_events' as model, alert_id || '/' || event as key
from {{ ref('stg_alert_events') }}
group by alert_id, event
having count(*) > 1
union all
select 'stg_eta_marks', order_id || '/' || mark
from {{ ref('stg_eta_marks') }}
group by order_id, mark
having count(*) > 1
