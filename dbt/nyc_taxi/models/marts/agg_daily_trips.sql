with trips as (
    select * from {{ ref('fct_trips') }}
)

select
    pickup_date,
    pickup_hour,
    count(*)                            as total_trips,
    sum(total_amount)                   as total_revenue,
    avg(total_amount)                   as avg_fare,
    avg(trip_distance)                  as avg_distance,
    avg(trip_duration_minutes)          as avg_duration_minutes,
    avg(tip_percentage)                 as avg_tip_percentage,
    sum(passenger_count)                as total_passengers,
    count(case when payment_type = 1 
          then 1 end)                   as credit_card_trips,
    count(case when payment_type = 2 
          then 1 end)                   as cash_trips

from trips
group by 1, 2
order by 1, 2
