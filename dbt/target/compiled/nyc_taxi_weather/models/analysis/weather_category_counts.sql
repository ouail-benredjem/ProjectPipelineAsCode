

-- Comptage des enregistrements par catégorie météo
SELECT 
    weather_category,
    COUNT(*) as count
FROM 
    "Minio"."public"."trip_summary_per_hour"
GROUP BY 
    weather_category
ORDER BY 
    count DESC