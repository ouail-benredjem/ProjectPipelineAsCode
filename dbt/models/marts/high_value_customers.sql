{{ config(
    materialized = 'table'
) }}

WITH passenger_stats AS (
    SELECT
        passenger_count,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_spent,
        AVG(tip_percentage) AS avg_tip_percentage
    FROM {{ ref('stg_taxi') }}
    WHERE 
        passenger_count IS NOT NULL AND 
        passenger_count > 0
    GROUP BY
        passenger_count
)

SELECT
    passenger_count,
    trip_count,
    total_spent,
    avg_tip_percentage
FROM 
    passenger_stats
WHERE
    trip_count > 10 AND
    total_spent > 300 AND
    avg_tip_percentage > 15
