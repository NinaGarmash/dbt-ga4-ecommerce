# GA4 Ecommerce Analytics with dbt Cloud + BigQuery

This project builds an analytics layer on top of GA4 BigQuery export ecommerce data using dbt Cloud.

## Data
- Source: BigQuery public dataset (GA4 obfuscated sample ecommerce)
- Grain:
  - Events: one row per event
  - Sessions: one row per (user_id, ga_session_id)
  - Orders: one row per transaction_id
  - Daily marts: one row per day

## Models
### Staging
- stg_ga4_events_sessions: multi-day GA4 events with session fields extracted from event_params
- stg_ga4_purchases: purchase events with extracted transaction_id / revenue / currency + validity flag

### Marts
- fct_sessions: session-level fact table with duration, event counts, new/returning flag
- fct_funnel_by_session: funnel step flags per session
- fct_funnel_rates (+ by_session_type): conversion metrics overall and by new vs returning sessions
- fct_orders: valid ecommerce orders aggregated by transaction_id
- dim_dates + fct_daily_revenue: daily revenue and AOV with date spine
- fct_daily_funnel + fct_daily_funnel_revenue: daily funnel + revenue combined

## Testing
Includes not_null, unique, and rule-based tests (rates within [0,1], purchases <= sessions, non-negative revenue).
