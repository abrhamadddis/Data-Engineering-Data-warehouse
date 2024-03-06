{{config(materialized='table')

}}

SELECT "type" AS "Vechicle Type", AVG("avg_speed"::numeric) As "Average speed"
FROM traffic_information
GROUP BY "type"