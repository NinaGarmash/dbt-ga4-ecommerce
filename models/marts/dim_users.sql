select
  user_id,
  min(session_start_ts) as first_session_ts
from {{ ref('fct_sessions') }}
group by user_id
