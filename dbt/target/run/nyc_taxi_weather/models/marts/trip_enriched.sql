
  
    

  create  table "Minio"."public"."trip_enriched__dbt_tmp"
  
  
    as
  
  (
    

WITH taxi_data AS (
    SELECT *
    FROM "Minio"."public"."stg_taxi"
),
weather_data AS (
    SELECT *
    FROM "Minio"."public"."stg_weather"
)

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.trip_duration_minutes,
    t.pickup_hour,
    t.pickup_day_of_week,
    t.passenger_count,
    t.trip_distance,
    t.distance_category,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.tip_amount,
    t.tip_percentage,
    t.total_amount,
    t.payment_type,
    t.payment_type_desc,
    w.temperature,
    w.feels_like,
    w.humidity,
    w.wind_speed,
    w.weather_main,
    w.weather_description,
    w.weather_category
FROM 
    taxi_data t
LEFT JOIN 
    weather_data w
    -- Jointure optimisée basée sur les composantes de date/heure
    ON EXTRACT(YEAR FROM t.pickup_datetime) = EXTRACT(YEAR FROM w.timestamp)
    AND EXTRACT(MONTH FROM t.pickup_datetime) = EXTRACT(MONTH FROM w.timestamp)
    AND EXTRACT(DAY FROM t.pickup_datetime) = EXTRACT(DAY FROM w.timestamp)
    AND EXTRACT(HOUR FROM t.pickup_datetime) = EXTRACT(HOUR FROM w.timestamp)
  );
  