
  
    

  create  table "Minio"."public"."unique_weather_categories__dbt_tmp"
  
  
    as
  
  (
    

-- Afficher toutes les catégories météo uniques dans les données sources
SELECT DISTINCT
    weather_category
FROM 
    "Minio"."public"."stg_weather"
ORDER BY 
    weather_category
  );
  