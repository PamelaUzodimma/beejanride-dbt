with driver_metrics as (
    select * from {{ ref('int_driver_metrics') }}
),

cities as (
    select * from {{ ref('stg_cities') }}
)

select
    d.driver_id,
    d.driver_status,
    d.driver_rating,
    d.onboarding_date,
    d.vehicle_id,
    d.city_id,
    c.city_name,
    c.country,
    d.lifetime_trips,
    d.completed_trips,
    d.cancelled_trips,
    d.no_show_trips,
    d.completion_rate_pct,
    d.total_revenue_gbp,
    d.avg_fare_gbp,
    d.corporate_trips,
    d.first_trip_at,
    d.last_trip_at,
    d.is_churned,

    -- driver tier based on revenue
    case
        when d.total_revenue_gbp >= 500  then 'platinum'
        when d.total_revenue_gbp >= 200  then 'gold'
        when d.total_revenue_gbp >= 50   then 'silver'
        else 'bronze'
    end as driver_tier

from driver_metrics d
left join cities c on d.city_id = c.city_id
