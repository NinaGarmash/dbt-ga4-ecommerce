select
  *
from {{ ref('fct_orders') }}
where purchase_event_rows > 1
   or distinct_users_in_order > 1
order by purchase_event_rows desc, distinct_users_in_order desc
