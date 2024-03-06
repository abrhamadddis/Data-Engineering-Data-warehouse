{{ config(materialized='table') }}

SELECT "type" AS "Vehicle Type", MAX("traveled_d") AS "Max Traveled Distance", MAX("avg_speed") AS "Max Average Speed"
FROM traffic_information
GROUP BY "type"