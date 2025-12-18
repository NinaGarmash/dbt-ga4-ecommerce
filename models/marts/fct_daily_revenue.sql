with dates as (
  select date_day
  from {{ ref('dim_dates') }}
),

daily as (
  select
    order_date,
    currency,
    count(*) as orders,
    sum(revenue) as revenue
  from {{ ref('fct_orders') }}
  where revenue is not null
  group by 1, 2
)

select
  d.date_day as order_date,
  coalesce(daily.currency, 'USD') as currency,
  coalesce(daily.orders, 0) as orders,
  coalesce(daily.revenue, 0) as revenue,
  safe_divide(coalesce(daily.revenue, 0), nullif(coalesce(daily.orders, 0), 0)) as aov
from dates d
left join daily
  on daily.order_date = d.date_day
order by 1, 2
