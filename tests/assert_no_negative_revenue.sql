-- No completed trip should have negative actual fare
select
    trip_id,
    actual_fare_gbp
from {{ ref('fct_trips') }}
where trip_status = 'completed'
  and actual_fare_gbp < 0
