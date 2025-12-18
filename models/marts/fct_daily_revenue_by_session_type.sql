with orders as (
  select
    transaction_id,
    ga_session_id,
    order_date,
    currency,
    revenue
  from {{ ref('fct_orders') }}
),

sessions as (
  select
    ga_session_id,
    is_new_session
  from {{ ref('fct_sessions') }}
)

select
  o.order_date,
  o.currency,
  s.is_new_session,
  sum(o.revenue) as daily_revenue,
  count(*) as orders,
  safe_divide(sum(o.revenue), count(*)) as aov
from orders o
left join sessions s
  using (ga_session_id)
group by 1, 2, 3
order by 1, 2, 3
