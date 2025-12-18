select
  user_id,
  ga_session_id,

  max(case when event_name = 'view_item' then 1 else 0 end) as did_view_item,
  max(case when event_name = 'add_to_cart' then 1 else 0 end) as did_add_to_cart,
  max(case when event_name = 'begin_checkout' then 1 else 0 end) as did_begin_checkout,
  max(case when event_name = 'purchase' then 1 else 0 end) as did_purchase

from {{ ref('stg_ga4_events_sessions') }}
where ga_session_id is not null
group by 1, 2

