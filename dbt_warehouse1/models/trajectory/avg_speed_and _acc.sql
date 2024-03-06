{{ config(materialized='table') }}

SELECT
    "track_id",
    AVG("speed") AS "Average Speed",
    AVG("lon_acc") AS "Average Longitudinal Acceleration",
    AVG("lat_acc") AS "Average Lateral Acceleration"
FROM
    trajectory_information
GROUP BY
    "track_id"