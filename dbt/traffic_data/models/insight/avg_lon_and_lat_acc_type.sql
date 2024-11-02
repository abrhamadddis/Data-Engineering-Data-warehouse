{{ config(
    materialized='table'
) }}

WITH logged_data as (
    SELECT
        t1.track_id AS track_id,
        t1.type AS type,
        t2.lon_acc AS lon_acc,
        t2.lat_acc AS lat_acc
    FROM
        {{ source('model', 'traffic_information') }} AS t1
    LEFT JOIN 
        {{ source('model', 'trajectory_information') }} AS t2
    ON
        t1.track_id = t2.track_id
)

SELECT 
    type,
    AVG(lon_acc) AS avg_lon_acc,
    AVG(lat_acc) AS avg_lat_acc
FROM 
    logged_data
GROUP BY 
    type