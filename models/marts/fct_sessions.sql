select
  user_id,
  ga_session_id,
  any_value(ga_session_number) as ga_session_number,
  case when any_value(ga_session_number) = 1 then true else false end as is_new_session,

  min(event_ts) as session_start_ts,
  max(event_ts) as session_end_ts,

  timestamp_diff(max(event_ts), min(event_ts), second) as session_duration_sec,
  count(*) as events_in_session,

  countif(event_name = 'purchase') as purchase_events_in_session

from {{ ref('stg_ga4_events_sessions') }}
where ga_session_id is not null
group by 1, 2
