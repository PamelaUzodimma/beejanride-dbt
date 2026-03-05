with trips as (
    select * from {{ ref('stg_trips') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

cities as (
    select * from {{ ref('stg_cities') }}
),

-- get latest successful payment per trip
successful_payments as (
    select
        trip_id,
        payment_status,
        payment_provider,
        amount_gbp,
        processing_fee_gbp,
        net_amount_gbp
    from payments
    where payment_status = 'success'
),

-- detect duplicate payments (same trip paid more than once)
payment_counts as (
    select
        trip_id,
        count(*) as payment_attempts,
        countif(payment_status = 'failed') as failed_attempts,
        countif(payment_status = 'success') as success_attempts
    from payments
    group by trip_id
),

enriched as (
    select
        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.vehicle_id,
        t.city_id,
        c.city_name,
        c.country,

        -- timestamps
        t.requested_at,
        t.pickup_at,
        t.dropoff_at,
        t.created_at,

        -- trip status
        t.trip_status,
        t.payment_method,
        t.is_corporate,

        -- duration in minutes
        case
            when t.pickup_at is not null and t.dropoff_at is not null
            then timestamp_diff(t.dropoff_at, t.pickup_at, minute)
            else null
        end as trip_duration_minutes,

        -- fares
        t.estimated_fare_gbp,
        t.actual_fare_gbp,
        t.surge_multiplier,

        -- net revenue after processing fee
        {{ calculate_net_revenue('t.actual_fare_gbp', 'coalesce(sp.processing_fee_gbp, 0)') }}
            as net_revenue_gbp,

        -- corporate flag
        case when t.is_corporate = true then 'corporate' else 'personal' end
            as trip_type,

        -- surge impact
        t.actual_fare_gbp - t.estimated_fare_gbp   as surge_revenue_gbp,

        -- payment info
        sp.payment_provider,
        sp.payment_status,
        coalesce(pc.payment_attempts, 0)            as payment_attempts,
        coalesce(pc.failed_attempts, 0)             as failed_payment_attempts,

        -- fraud indicators
        {{ is_fraud_indicator('t.surge_multiplier', 10) }}
            as is_extreme_surge,

        case
            when t.trip_status = 'completed'
                and coalesce(pc.success_attempts, 0) = 0
            then true else false
        end as is_failed_payment_on_completed_trip,

        case
            when coalesce(pc.success_attempts, 0) > 1
            then true else false
        end as is_duplicate_payment

    from trips t
    left join successful_payments sp on t.trip_id = sp.trip_id
    left join payment_counts pc on t.trip_id = pc.trip_id
    left join cities c on t.city_id = c.city_id
)

select * from enriched
