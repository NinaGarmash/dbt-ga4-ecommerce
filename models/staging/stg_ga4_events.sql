select
  user_pseudo_id as user_id,
  event_name,
  timestamp_micros(event_timestamp) as event_ts,
  event_date
from {{ source('ga4_ecom', 'events_20210131') }}
limit 100
