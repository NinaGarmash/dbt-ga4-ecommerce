with funnel as (
  select
    event_date,
    sessions,
    sessions_view_item,
    sessions_add_to_cart,
    sessions_begin_checkout,
    sessions_purchase
  from {{ ref('fct_daily_funnel') }}
),

revenue as (
  select
    order_date,
    orders,
    revenue,
    aov
  from {{ ref('fct_daily_revenue') }}
)

select
  f.event_date,

  f.sessions,
  f.sessions_view_item,
  f.sessions_add_to_cart,
  f.sessions_begin_checkout,
  f.sessions_purchase,

  r.orders,
  r.revenue,
  r.aov,

  safe_divide(f.sessions_purchase, f.sessions) as session_conversion_rate,
  safe_divide(r.revenue, f.sessions) as revenue_per_session

from funnel f
left join revenue r
  on r.order_date = f.event_date
order by f.event_date
