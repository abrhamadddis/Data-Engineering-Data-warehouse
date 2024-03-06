{{config(
    materialized='table')
}}
SELECT "type" AS "Vehicle Type", COUNT("track_id") AS "Type Count"
FROM traffic_information
GROUP By "type"