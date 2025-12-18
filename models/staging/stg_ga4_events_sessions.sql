with base as (

  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210122') }}

  union all

    select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210123') }}

  union all

  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210124') }}

  union all

  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210125') }}

  union all
  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210126') }}

  union all
  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210127') }}

  union all
  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210128') }}

  union all
  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210129') }}

  union all
  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    -- GA4 session fields live inside event_params
    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210130') }}

  union all

  select
    user_pseudo_id as user_id,
    event_name,
    event_date,
    event_timestamp,
    timestamp_micros(event_timestamp) as event_ts,
        event_params,


    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_id'
    ) as ga_session_id,

    (
      select ep.value.int_value
      from unnest(event_params) ep
      where ep.key = 'ga_session_number'
    ) as ga_session_number

  from {{ source('ga4_ecom', 'events_20210131') }}
)

select *
from base
