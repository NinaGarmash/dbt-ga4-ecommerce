select distinct
  date(event_ts) as date_day
from {{ ref('stg_ga4_events_sessions') }}
