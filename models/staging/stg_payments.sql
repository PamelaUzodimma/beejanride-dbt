with source as (
    select * from {{ source('beejanride_raw', 'payments_raw') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by payment_id
            order by created_at desc
        ) as row_num
    from source
    where payment_id is not null
),

renamed as (
    select
        payment_id,
        trip_id,
        lower(trim(payment_status))                     as payment_status,
        lower(trim(payment_provider))                   as payment_provider,
        lower(trim(currency))                           as currency,
        cast(amount as numeric)                         as amount_gbp,
        cast(fee as numeric)                            as processing_fee_gbp,
        cast(amount as numeric) - cast(fee as numeric)  as net_amount_gbp,
        cast(created_at as timestamp)                   as created_at
    from deduplicated
    where row_num = 1
)

select * from renamed
