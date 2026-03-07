with source as (
    select * from {{ source('raw', 'yellow_taxi_trips') }}
),

renamed as (
    select
        -- identifiers
        vendorid                                as vendor_id,
        ratecodeid                              as rate_code_id,
        pulocationid                            as pickup_location_id,
        dolocationid                            as dropoff_location_id,

        -- timestamps
        tpep_pickup_datetime                    as pickup_datetime,
        tpep_dropoff_datetime                   as dropoff_datetime,

        -- trip info
        passenger_count,
        trip_distance,
        store_and_fwd_flag,

        -- payment
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,

        -- derived
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes

    from source
    where
        trip_distance > 0
        and total_amount > 0
        and tpep_pickup_datetime >= '2024-01-01'
        and tpep_pickup_datetime < '2024-02-01'
)

select * from renamed
