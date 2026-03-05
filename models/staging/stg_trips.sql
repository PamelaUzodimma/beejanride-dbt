with source as (
    select * from {{ source('beejanride_raw', 'trips_raw') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by trip_id
            order by updated_at desc
        ) as row_num
    from source
    where trip_id is not null
),

renamed as (
    select
        trip_id,
        rider_id,
        driver_id,
        vehicle_id,
        city_id,
        cast(requested_at as timestamp)                 as requested_at,
        cast(pickup_at as timestamp)                    as pickup_at,
        cast(dropoff_at as timestamp)                   as dropoff_at,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at,
        lower(trim(status))                             as trip_status,
        lower(trim(payment_method))                     as payment_method,
        cast(estimated_fare as numeric)                 as estimated_fare_gbp,
        cast(actual_fare as numeric)                    as actual_fare_gbp,
        cast(surge_multiplier as numeric)               as surge_multiplier,
        cast(is_corporate as boolean)                   as is_corporate
    from deduplicated
    where row_num = 1
)

select * from renamed
