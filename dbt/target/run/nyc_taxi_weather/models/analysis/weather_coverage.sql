
  
    

  create  table "Minio"."public"."weather_coverage__dbt_tmp"
  
  
    as
  
  (
    

-- Analyse de la couverture des données météo par jour et heure
WITH taxi_hours AS (
    -- Extraire les dates/heures uniques des trajets de taxi
    SELECT DISTINCT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        EXTRACT(DAY FROM pickup_datetime) AS day,
        EXTRACT(HOUR FROM pickup_datetime) AS hour
    FROM "Minio"."public"."stg_taxi"
),

weather_hours AS (
    -- Extraire les dates/heures uniques des données météo
    SELECT DISTINCT
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(DAY FROM timestamp) AS day,
        EXTRACT(HOUR FROM timestamp) AS hour,
        weather_category
    FROM "Minio"."public"."stg_weather"
)

-- Analyser la couverture des données
SELECT
    t.year,
    t.month, 
    t.day,
    t.hour,
    CASE 
        WHEN w.year IS NOT NULL THEN 'Oui'
        ELSE 'Non'
    END AS donnees_meteo_disponibles,
    w.weather_category
FROM
    taxi_hours t
LEFT JOIN
    weather_hours w
    ON t.year = w.year
    AND t.month = w.month
    AND t.day = w.day
    AND t.hour = w.hour
ORDER BY
    t.year, t.month, t.day, t.hour
  );
  