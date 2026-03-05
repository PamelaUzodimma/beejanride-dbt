with payments as (
    select * from {{ ref('stg_payments') }}
),

trips as (
    select * from {{ ref('stg_trips') }}
),

payment_summary as (
    select
        p.payment_id,
        p.trip_id,
        p.payment_status,
        p.payment_provider,
        p.amount_gbp,
        p.processing_fee_gbp,
        p.net_amount_gbp,
        p.currency,
        p.created_at                                    as payment_created_at,
        t.trip_status,
        t.actual_fare_gbp,
        t.city_id,
        t.driver_id,
        t.rider_id,

        -- failed payment on completed trip = fraud/ops issue
        case
            when p.payment_status = 'failed'
                and t.trip_status = 'completed'
            then true else false
        end as is_failed_on_completed,

        -- amount mismatch between payment and fare
        case
            when p.payment_status = 'success'
                and abs(p.amount_gbp - t.actual_fare_gbp) > 1
            then true else false
        end as is_amount_mismatch

    from payments p
    left join trips t on p.trip_id = t.trip_id
)

select * from payment_summary
