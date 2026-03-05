with payment_analysis as (
    select * from {{ ref('int_payment_analysis') }}
)

select
    payment_id,
    trip_id,
    driver_id,
    rider_id,
    city_id,
    payment_status,
    payment_provider,
    currency,
    amount_gbp,
    processing_fee_gbp,
    net_amount_gbp,
    trip_status,
    actual_fare_gbp,
    is_failed_on_completed,
    is_amount_mismatch,
    date(payment_created_at)        as payment_date,
    payment_created_at
from payment_analysis
