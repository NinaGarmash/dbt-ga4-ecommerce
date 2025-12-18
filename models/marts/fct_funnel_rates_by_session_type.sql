with sessions as (
  select
    user_id,
    ga_session_id,
    is_new_session
  from {{ ref('fct_sessions') }}
),

funnel as (
  select
    user_id,
    ga_session_id,
    did_view_item,
    did_add_to_cart,
    did_begin_checkout,
    did_purchase
  from {{ ref('fct_funnel_by_session') }}
)

select
  s.is_new_session,

  count(*) as sessions_total,
  sum(f.did_view_item) as sessions_view_item,
  sum(f.did_add_to_cart) as sessions_add_to_cart,
  sum(f.did_begin_checkout) as sessions_begin_checkout,
  sum(f.did_purchase) as sessions_purchase,

  safe_divide(sum(f.did_add_to_cart), sum(f.did_view_item)) as view_to_cart_rate,
  safe_divide(sum(f.did_begin_checkout), sum(f.did_add_to_cart)) as cart_to_checkout_rate,
  safe_divide(sum(f.did_purchase), sum(f.did_begin_checkout)) as checkout_to_purchase_rate,
  safe_divide(sum(f.did_purchase), count(*)) as session_purchase_rate

from sessions s
join funnel f
  using (user_id, ga_session_id)
group by 1
order by 1 desc

