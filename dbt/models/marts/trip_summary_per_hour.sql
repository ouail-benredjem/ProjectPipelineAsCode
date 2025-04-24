{{ config(
    materialized = 'table'
) }}

WITH trip_data AS (
    SELECT 
        pickup_datetime,
        dropoff_datetime,
        trip_duration_minutes,
        passenger_count,
        trip_distance,
        distance_category,
        fare_amount,
        tip_amount,
        tip_percentage,
        total_amount,
        payment_type,
        payment_type_desc,
        temperature,
        feels_like,
        humidity,
        wind_speed,
        -- Nettoyage et normalisation des catégories météo
        CASE 
            WHEN weather_category IS NULL THEN 'Unknown'
            WHEN weather_category = 'Other' THEN 
                CASE
                    WHEN weather_main IN ('Mist', 'Fog', 'Haze') THEN 'Foggy'
                    WHEN weather_main = 'Wind' OR wind_speed > 8 THEN 'Windy'
                    WHEN weather_main IN ('Drizzle', 'Light rain') THEN 'Light Rain'
                    ELSE weather_category
                END
            ELSE weather_category
        END AS weather_category
    FROM {{ ref('trip_enriched') }}
    -- S'assurer que nous avons des données météo valides
    WHERE weather_main IS NOT NULL OR weather_category IS NOT NULL
)

SELECT
    DATE_TRUNC('hour', pickup_datetime) AS hour,
    weather_category,
    COUNT(*) AS trip_count,
    AVG(trip_duration_minutes) AS avg_trip_duration,
    AVG(tip_percentage) AS avg_tip_percentage,
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue
FROM 
    trip_data
GROUP BY 
    hour,
    weather_category
ORDER BY 
    hour,
    weather_category
