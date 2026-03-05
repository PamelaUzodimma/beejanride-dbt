with source as (
    select * from {{ source('beejanride_raw', 'cities_raw') }}
),

renamed as (
    select
        city_id,
        trim(city_name)                                 as city_name,
        lower(trim(country))                            as country,
        cast(launch_date as date)                       as launch_date
    from source
    where city_id is not null
)

select * from renamed
