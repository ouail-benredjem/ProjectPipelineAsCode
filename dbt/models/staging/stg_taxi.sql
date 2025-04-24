{{ config(
    materialized = 'view'
) }}

SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    pickup_hour,
    pickup_day_of_week,
    passenger_count,
    trip_distance,
    distance_category,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    tip_amount,
    tip_percentage,
    total_amount,
    payment_type,
    payment_type_desc
FROM {{ source('postgres', 'fact_taxi_trips') }}
