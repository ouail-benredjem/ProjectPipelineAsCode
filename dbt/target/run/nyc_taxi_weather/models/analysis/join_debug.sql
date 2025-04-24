
  
    

  create  table "Minio"."public"."join_debug__dbt_tmp"
  
  
    as
  
  (
    

WITH 
taxi_sample AS (
    SELECT *
    FROM "Minio"."public"."stg_taxi"
    LIMIT 10
),
weather_sample AS (
    SELECT *
    FROM "Minio"."public"."stg_weather"
    LIMIT 10
),
join_test AS (
    SELECT
        t.pickup_datetime,
        DATE_TRUNC('hour', t.pickup_datetime) AS taxi_hour,
        t.pickup_hour,
        t.pickup_day_of_week,
        w.timestamp,
        DATE_TRUNC('hour', w.timestamp) AS weather_hour,
        w.hour_of_day,
        w.day_of_week,
        w.weather_category,
        CASE 
            WHEN DATE_TRUNC('hour', t.pickup_datetime) = DATE_TRUNC('hour', w.timestamp) THEN 'Match-Hour'
            ELSE 'No-Match-Hour'
        END AS hour_join_status,
        CASE 
            WHEN t.pickup_hour = w.hour_of_day THEN 'Match-HourOfDay'
            ELSE 'No-Match-HourOfDay'
        END AS hour_of_day_join_status,
        CASE 
            WHEN t.pickup_day_of_week = w.day_of_week THEN 'Match-DayOfWeek'
            ELSE 'No-Match-DayOfWeek'
        END AS day_of_week_join_status
    FROM 
        taxi_sample t
    CROSS JOIN 
        weather_sample w
)

SELECT * FROM join_test
  );
  