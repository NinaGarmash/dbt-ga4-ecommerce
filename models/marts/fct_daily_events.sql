select
  date(event_ts) as event_day,
  event_name,
  count(*) as event_count
from {{ ref('stg_ga4_events') }}
group by 1, 2
order by 1, 3 desc
