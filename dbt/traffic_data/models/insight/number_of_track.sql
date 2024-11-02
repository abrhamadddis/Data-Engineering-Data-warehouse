{{ config(
    materialized='table'
) }}

SELECT 
    type,
    COUNT(track_id) as number_of_track
FROM 
    {{ source('model', 'traffic_information') }}
GROUP BY 
    type
ORDER BY
    number_of_track