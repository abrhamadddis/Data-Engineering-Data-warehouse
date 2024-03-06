{{ config(materialized='table') }}

SELECT "type" AS "Vehicle Type", SUM("traveled_d"::numeric) AS "Total Distance"
FROM
 traffic_information
GROUP BY "type"