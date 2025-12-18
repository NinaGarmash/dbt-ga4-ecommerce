select
  transaction_id,

  min(event_ts) as order_ts,
  date(min(event_ts)) as order_date,

  any_value(currency) as currency,
  max(revenue) as revenue,

  count(*) as purchase_event_rows,
  count(distinct user_id) as distinct_users_in_order

from {{ ref('stg_ga4_purchases') }}
where transaction_id is not null
group by 1

