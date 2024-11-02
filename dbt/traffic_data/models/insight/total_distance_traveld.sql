{{ config(
    materialized='table'
) }}

SELECT 
    type,
    ROUND(CAST(SUM(traveled_d) AS numeric), 0) as total_distance_traveled,
    Max(traveled_d) as maximum_distance,
    Min(traveled_d) as minimum_distance
    
FROM 
    {{ source('model', 'traffic_information') }}
GROUP BY 
    type

