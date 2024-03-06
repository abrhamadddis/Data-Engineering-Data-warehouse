{{ config(materialized='table') }}

SELECT
    m1.vehicle_type,
    m1.column1 AS m,
    m2.column2 AS avg_dist_by_type,
    m3.column3 AS avg_speed_by_type,
    m4.column4 AS count_by_type,
    m5.column5 AS max_dis_and_speed_travel,
    m6.column6 AS total_dist_by_type
FROM
    {{ ref('avg_dist_by_type') }} m1
JOIN
    {{ ref('avg_dist_by_type') }} m2 ON m1.vehicle_type = m2.vehicle_type
JOIN
    {{ ref('avg_speed_by_type') }} m3 ON m1.vehicle_type = m3.vehicle_type
JOIN
    {{ ref('count_by_type') }} m4 ON m1.vehicle_type = m4.vehicle_type
JOIN
    {{ ref('max_dis_and_speed_travel') }} m5 ON m1.vehicle_type = m5.vehicle_type
JOIN
    {{ ref('total_dist_by_type') }} m6 ON m1.vehicle_type = m6.vehicle_type