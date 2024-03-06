{{ config(
    materialized='table'
) }}

SELECT "type" as "vehicle type", AVG("traveled_d"::numeric) As "Average Distance"
From traffic_information
GROUP BY "type"