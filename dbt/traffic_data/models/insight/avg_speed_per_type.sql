{{ config(
    materialized='table'
) }}

SELECT 
    type,
    ROUND(CAST(AVG(avg_speed) AS numeric), 0) AS average_speed_vehicle_type,
    Max(avg_speed) as Maximum_avg_speed,
    Min(avg_speed) as minimum_avg_speed
FROM 
    {{ source('model', 'traffic_information') }}
GROUP BY 
    type
