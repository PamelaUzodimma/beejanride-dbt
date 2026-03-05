{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "trip_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["city_id", "driver_id"]
    )
}}

with enriched as (
    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
        where date(requested_at) > (select max(trip_date) from {{ this }})
    {% endif %}
)

select
    trip_id,
    rider_id,
    driver_id,
    vehicle_id,
    city_id,
    city_name,
    date(requested_at)              as trip_date,
    requested_at,
    pickup_at,
    dropoff_at,
    trip_status,
    trip_type,
    payment_method,
    payment_provider,
    trip_duration_minutes,
    estimated_fare_gbp,
    actual_fare_gbp,
    surge_multiplier,
    surge_revenue_gbp,
    net_revenue_gbp,
    payment_attempts,
    failed_payment_attempts,
    is_extreme_surge,
    is_duplicate_payment,
    is_failed_payment_on_completed_trip,
    is_corporate
from enriched
