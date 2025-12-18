with purchases as (
  select
    user_id,
    ga_session_id,
    event_ts,

    (
      select ep.value.string_value
      from unnest(event_params) ep
      where ep.key = 'transaction_id'
    ) as transaction_id,

    coalesce(
      (
        select ep.value.double_value
        from unnest(event_params) ep
        where ep.key = 'value'
      ),
      cast((
        select ep.value.int_value
        from unnest(event_params) ep
        where ep.key = 'value'
      ) as float64)
    ) as revenue,

    (
      select ep.value.string_value
      from unnest(event_params) ep
      where ep.key = 'currency'
    ) as currency

  from {{ ref('stg_ga4_events_sessions') }}
  where event_name = 'purchase'
)

select
  *,
  case
    when transaction_id is not null and revenue is not null then true
    else false
  end as is_valid_order
from purchases
