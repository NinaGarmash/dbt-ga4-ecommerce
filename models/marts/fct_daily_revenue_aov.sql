select
  order_date,
  currency,
  sum(revenue) as daily_revenue,
  count(*) as orders,
  safe_divide(sum(revenue), count(*)) as aov
from {{ ref('fct_orders') }}
group by 1, 2
order by 1, 2
