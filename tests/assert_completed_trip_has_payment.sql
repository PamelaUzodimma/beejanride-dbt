-- Every completed trip must have at least one successful payment
select
    t.trip_id
from {{ ref('fct_trips') }} t
left join {{ ref('fct_payments') }} p
    on t.trip_id = p.trip_id
    and p.payment_status = 'success'
where t.trip_status = 'completed'
  and p.payment_id is null
