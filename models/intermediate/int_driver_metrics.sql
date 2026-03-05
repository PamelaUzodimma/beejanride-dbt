with trips as (
    select * from {{ ref('stg_trips') }}
),

drivers as (
    select * from {{ ref('stg_drivers') }}
),

driver_trips as (
    select
        driver_id,
        count(*)                                        as lifetime_trips,
        countif(trip_status = 'completed')              as completed_trips,
        countif(trip_status = 'cancelled')              as cancelled_trips,
        countif(trip_status = 'no_show')                as no_show_trips,
        sum(case when trip_status = 'completed'
            then actual_fare_gbp else 0 end)            as total_revenue_gbp,
        avg(case when trip_status = 'completed'
            then actual_fare_gbp else null end)         as avg_fare_gbp,
        max(requested_at)                               as last_trip_at,
        min(requested_at)                               as first_trip_at,
        countif(is_corporate = true)                    as corporate_trips
    from trips
    group by driver_id
),

final as (
    select
        d.driver_id,
        d.city_id,
        d.driver_status,
        d.driver_rating,
        d.onboarding_date,
        d.vehicle_id,

        coalesce(dt.lifetime_trips, 0)                  as lifetime_trips,
        coalesce(dt.completed_trips, 0)                 as completed_trips,
        coalesce(dt.cancelled_trips, 0)                 as cancelled_trips,
        coalesce(dt.no_show_trips, 0)                   as no_show_trips,
        coalesce(dt.total_revenue_gbp, 0)               as total_revenue_gbp,
        coalesce(dt.avg_fare_gbp, 0)                    as avg_fare_gbp,
        coalesce(dt.corporate_trips, 0)                 as corporate_trips,
        dt.last_trip_at,
        dt.first_trip_at,

        -- completion rate
        case when coalesce(dt.lifetime_trips, 0) > 0
            then round(dt.completed_trips / dt.lifetime_trips * 100, 2)
            else 0
        end as completion_rate_pct,

        -- churn indicator: no trips in last 30 days
        case
            when dt.last_trip_at < timestamp_sub(current_timestamp(), interval 30 day)
                or dt.last_trip_at is null
            then true else false
        end as is_churned

    from drivers d
    left join driver_trips dt on d.driver_id = dt.driver_id
)

select * from final
