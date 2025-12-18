select
  count(*) as sessions_total,

  sum(did_view_item) as sessions_view_item,
  sum(did_add_to_cart) as sessions_add_to_cart,
  sum(did_begin_checkout) as sessions_begin_checkout,
  sum(did_purchase) as sessions_purchase,

  safe_divide(sum(did_add_to_cart), sum(did_view_item)) as view_to_cart_rate,
  safe_divide(sum(did_begin_checkout), sum(did_add_to_cart)) as cart_to_checkout_rate,
  safe_divide(sum(did_purchase), sum(did_begin_checkout)) as checkout_to_purchase_rate,
  safe_divide(sum(did_purchase), count(*)) as session_purchase_rate

from {{ ref('fct_funnel_by_session') }}
