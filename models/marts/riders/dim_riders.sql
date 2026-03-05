with rider_metrics as (
    select * from {{ ref('int_rider_metrics') }}
)

select
    rider_id,
    country,
    signup_date,
    referral_code,
    is_referred,
    total_trips,
    completed_trips,
    lifetime_value_gbp,
    avg_spend_gbp,
    rider_segment,
    corporate_trips,
    card_payments,
    wallet_payments,
    cash_payments,
    first_trip_at,
    last_trip_at,

    -- preferred payment method
    case
        when card_payments >= wallet_payments
            and card_payments >= cash_payments   then 'card'
        when wallet_payments >= cash_payments    then 'wallet'
        else 'cash'
    end as preferred_payment_method

from rider_metrics
