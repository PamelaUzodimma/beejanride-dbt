with source as (
    select * from {{ source('beejanride_raw', 'drivers_raw') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by driver_id
            order by updated_at desc
        ) as row_num
    from source
    where driver_id is not null
),

renamed as (
    select
        driver_id,
        city_id,
        vehicle_id,
        cast(onboarding_date as date)                   as onboarding_date,
        lower(trim(driver_status))                      as driver_status,
        cast(rating as numeric)                         as driver_rating,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at
    from deduplicated
    where row_num = 1
)

select * from renamed
