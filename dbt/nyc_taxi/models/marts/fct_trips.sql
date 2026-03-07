with trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
)

select
    -- identifiers
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    rate_code_id,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,
    date_trunc('day', pickup_datetime)      as pickup_date,
    date_part('hour', pickup_datetime)      as pickup_hour,
    date_part('dow', pickup_datetime)       as pickup_day_of_week,

    -- trip info
    passenger_count,
    trip_distance,
    trip_duration_minutes,

    -- payment
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    congestion_surcharge,

    -- derived metrics
    tip_amount / nullif(fare_amount, 0)     as tip_percentage,
    total_amount / nullif(trip_distance, 0) as revenue_per_mile

from trips
