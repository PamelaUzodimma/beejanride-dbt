with trips as (
    select * from {{ ref('stg_trips') }}
),

riders as (
    select * from {{ ref('stg_riders') }}
),

rider_trips as (
    select
        rider_id,
        count(*)                                        as total_trips,
        countif(trip_status = 'completed')              as completed_trips,
        sum(case when trip_status = 'completed'
            then actual_fare_gbp else 0 end)            as total_spend_gbp,
        avg(case when trip_status = 'completed'
            then actual_fare_gbp else null end)         as avg_spend_gbp,
        min(requested_at)                               as first_trip_at,
        max(requested_at)                               as last_trip_at,
        countif(is_corporate = true)                    as corporate_trips,
        countif(payment_method = 'card')                as card_payments,
        countif(payment_method = 'wallet')              as wallet_payments,
        countif(payment_method = 'cash')                as cash_payments
    from trips
    group by rider_id
),

final as (
    select
        r.rider_id,
        r.country,
        r.signup_date,
        r.referral_code,
        r.created_at,

        coalesce(rt.total_trips, 0)                     as total_trips,
        coalesce(rt.completed_trips, 0)                 as completed_trips,
        coalesce(rt.total_spend_gbp, 0)                 as lifetime_value_gbp,
        coalesce(rt.avg_spend_gbp, 0)                   as avg_spend_gbp,
        rt.first_trip_at,
        rt.last_trip_at,
        coalesce(rt.corporate_trips, 0)                 as corporate_trips,
        coalesce(rt.card_payments, 0)                   as card_payments,
        coalesce(rt.wallet_payments, 0)                 as wallet_payments,
        coalesce(rt.cash_payments, 0)                   as cash_payments,

        -- rider segment by LTV
        case
            when coalesce(rt.total_spend_gbp, 0) >= 100 then 'high_value'
            when coalesce(rt.total_spend_gbp, 0) >= 50  then 'mid_value'
            when coalesce(rt.total_spend_gbp, 0) > 0    then 'low_value'
            else 'inactive'
        end as rider_segment,

        -- referred rider flag
        case when r.referral_code is not null
            then true else false
        end as is_referred

    from riders r
    left join rider_trips rt on r.rider_id = rt.rider_id
)

select * from final
