{% snapshot drivers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='driver_id',
        strategy='check',
        check_cols=['driver_status', 'vehicle_id', 'driver_rating'],
        invalidate_hard_deletes=True
    )
}}

select
    driver_id,
    city_id,
    vehicle_id,
    driver_status,
    driver_rating,
    onboarding_date,
    created_at,
    updated_at
from {{ ref('stg_drivers') }}

{% endsnapshot %}
