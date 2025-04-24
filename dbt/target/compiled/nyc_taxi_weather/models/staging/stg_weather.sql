

SELECT
    timestamp,
    city,
    temperature,
    feels_like,
    humidity,
    wind_speed,
    wind_direction,
    weather_main,
    weather_description,
    -- Gérer les NULL pour la catégorie météo
    COALESCE(weather_category, 'Unknown') AS weather_category,
    pressure,
    hour_of_day,
    day_of_week
FROM "Minio"."public"."dim_weather"