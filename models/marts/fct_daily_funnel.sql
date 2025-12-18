select
  date(s.session_start_ts) as event_date,

  count(*) as sessions,
  sum(f.did_view_item) as sessions_view_item,
  sum(f.did_add_to_cart) as sessions_add_to_cart,
  sum(f.did_begin_checkout) as sessions_begin_checkout,
  sum(f.did_purchase) as sessions_purchase

from {{ ref('fct_sessions') }} s
join {{ ref('fct_funnel_by_session') }} f
  using (user_id, ga_session_id)
group by 1
order by 1
