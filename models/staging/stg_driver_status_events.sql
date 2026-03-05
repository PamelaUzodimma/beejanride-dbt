{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["driver_id"]
    )
}}

with source as (
    select * from {{ source('beejanride_raw', 'driver_status_events_raw') }}

    {% if is_incremental() %}
        where cast(event_timestamp as timestamp)
            > (select max(event_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        event_id,
        driver_id,
        lower(trim(status))                             as driver_status,
        cast(event_timestamp as timestamp)              as event_timestamp,
        cast(event_timestamp as date)                   as event_date
    from source
    where event_id is not null
)

select * from renamed
