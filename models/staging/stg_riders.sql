with source as (
    select * from {{ source('beejanride_raw', 'riders_raw') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by rider_id
            order by created_at desc
        ) as row_num
    from source
    where rider_id is not null
),

renamed as (
    select
        rider_id,
        lower(trim(country))                            as country,
        referral_code,
        cast(signup_date as date)                       as signup_date,
        cast(created_at as timestamp)                   as created_at
    from deduplicated
    where row_num = 1
)

select * from renamed
