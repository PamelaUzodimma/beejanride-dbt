-- ============================================
-- BeejanRide Analytics — Sample Queries
-- ============================================

-- 1. Daily Revenue Per City
select
    trip_date,
    city_name,
    count(trip_id)                          as total_trips,
    countif(trip_status = 'completed')      as completed_trips,
    round(sum(actual_fare_gbp), 2)          as gross_revenue_gbp,
    round(sum(net_revenue_gbp), 2)          as net_revenue_gbp
from `beejan-analytics`.`beejanride_dev`.`fct_trips`
where trip_status = 'completed'
group by trip_date, city_name
order by trip_date, gross_revenue_gbp desc;

-- 2. Gross vs Net Revenue
select
    trip_date,
    round(sum(actual_fare_gbp), 2)          as gross_revenue_gbp,
    round(sum(net_revenue_gbp), 2)          as net_revenue_gbp,
    round(sum(actual_fare_gbp)
        - sum(net_revenue_gbp), 2)          as total_fees_gbp
from `beejan-analytics`.`beejanride_dev`.`fct_trips`
where trip_status = 'completed'
group by trip_date
order by trip_date;

-- 3. Corporate vs Personal Revenue Split
select
    trip_type,
    count(trip_id)                          as total_trips,
    round(sum(actual_fare_gbp), 2)          as gross_revenue_gbp,
    round(avg(actual_fare_gbp), 2)          as avg_fare_gbp
from `beejan-analytics`.`beejanride_dev`.`fct_trips`
where trip_status = 'completed'
group by trip_type;

-- 4. Top Drivers by Revenue
select
    driver_id,
    city_name,
    driver_tier,
    driver_rating,
    completed_trips,
    round(total_revenue_gbp, 2)             as total_revenue_gbp,
    round(avg_fare_gbp, 2)                  as avg_fare_gbp,
    completion_rate_pct
from `beejan-analytics`.`beejanride_dev`.`dim_drivers`
order by total_revenue_gbp desc
limit 10;

-- 5. Driver Churn Tracking
select
    driver_id,
    city_name,
    driver_status,
    driver_tier,
    last_trip_at,
    lifetime_trips,
    is_churned
from `beejan-analytics`.`beejanride_dev`.`dim_drivers`
where is_churned = true
order by last_trip_at;

-- 6. Rider Lifetime Value
select
    rider_id,
    country,
    rider_segment,
    total_trips,
    round(lifetime_value_gbp, 2)            as lifetime_value_gbp,
    round(avg_spend_gbp, 2)                 as avg_spend_gbp,
    preferred_payment_method,
    is_referred
from `beejan-analytics`.`beejanride_dev`.`dim_riders`
order by lifetime_value_gbp desc;

-- 7. Payment Failure Rate
select
    payment_provider,
    count(payment_id)                       as total_payments,
    countif(payment_status = 'failed')      as failed_payments,
    round(countif(payment_status = 'failed')
        / count(payment_id) * 100, 2)       as failure_rate_pct
from `beejan-analytics`.`beejanride_dev`.`fct_payments`
group by payment_provider;

-- 8. Surge Impact Analysis
select
    trip_date,
    city_name,
    round(avg(surge_multiplier), 2)         as avg_surge,
    round(sum(surge_revenue_gbp), 2)        as total_surge_revenue_gbp,
    countif(surge_multiplier > 1)           as surged_trips,
    count(trip_id)                          as total_trips
from `beejan-analytics`.`beejanride_dev`.`fct_trips`
where trip_status = 'completed'
group by trip_date, city_name
order by avg_surge desc;

-- 9. Fraud Detection Insights
select
    trip_id,
    driver_id,
    rider_id,
    city_name,
    actual_fare_gbp,
    surge_multiplier,
    is_extreme_surge,
    is_duplicate_payment,
    is_failed_payment_on_completed_trip
from `beejan-analytics`.`beejanride_dev`.`fct_trips`
where is_extreme_surge = true
   or is_duplicate_payment = true
   or is_failed_payment_on_completed_trip = true;

-- 10. Driver Activity Monitoring
select
    d.driver_id,
    d.city_name,
    d.driver_status,
    d.driver_rating,
    d.lifetime_trips,
    d.completion_rate_pct,
    d.last_trip_at,
    d.is_churned,
    count(e.event_id)                       as total_online_events
from `beejan-analytics`.`beejanride_dev`.`dim_drivers` d
left join `beejan-analytics`.`beejanride_dev_staging`.`stg_driver_status_events` e
    on d.driver_id = e.driver_id
    and e.driver_status = 'online'
group by 1,2,3,4,5,6,7,8
order by d.total_revenue_gbp desc;
