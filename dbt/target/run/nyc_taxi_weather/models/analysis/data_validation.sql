
  
    

  create  table "Minio"."public"."data_validation__dbt_tmp"
  
  
    as
  
  (
    

WITH weather_stats AS (
    SELECT 
        COUNT(*) as total_weather_records,
        COUNT(DISTINCT DATE_TRUNC('hour', timestamp)) as distinct_weather_hours,
        MIN(timestamp) as min_weather_time,
        MAX(timestamp) as max_weather_time,
        COUNT(DISTINCT weather_category) as distinct_weather_categories
    FROM "Minio"."public"."stg_weather"
),
taxi_stats AS (
    SELECT 
        COUNT(*) as total_taxi_records,
        COUNT(DISTINCT DATE_TRUNC('hour', pickup_datetime)) as distinct_taxi_hours,
        MIN(pickup_datetime) as min_taxi_time,
        MAX(pickup_datetime) as max_taxi_time
    FROM "Minio"."public"."stg_taxi"
),
join_stats AS (
    SELECT 
        COUNT(*) as potential_matches
    FROM "Minio"."public"."stg_taxi" t
    JOIN "Minio"."public"."stg_weather" w ON DATE_TRUNC('hour', t.pickup_datetime) = DATE_TRUNC('hour', w.timestamp)
),
enriched_stats AS (
    SELECT 
        COUNT(*) as total_enriched_records,
        COUNT(CASE WHEN weather_category IS NOT NULL THEN 1 END) as records_with_weather,
        COUNT(DISTINCT weather_category) as distinct_categories_in_enriched
    FROM "Minio"."public"."trip_enriched"
)

SELECT
    w.total_weather_records,
    w.distinct_weather_hours,
    w.min_weather_time,
    w.max_weather_time,
    w.distinct_weather_categories,
    t.total_taxi_records,
    t.distinct_taxi_hours,
    t.min_taxi_time,
    t.max_taxi_time,
    j.potential_matches,
    e.total_enriched_records,
    e.records_with_weather,
    e.distinct_categories_in_enriched
FROM 
    weather_stats w, 
    taxi_stats t, 
    join_stats j,
    enriched_stats e
  );
  