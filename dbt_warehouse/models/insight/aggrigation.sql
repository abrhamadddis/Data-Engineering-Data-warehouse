WITH total_distance AS (
    SELECT 
        SUM(traveled_d) as total_distance
    FROM 
        {{ source('model', 'traffic_track') }}
),

average_speed AS (
    SELECT 
        AVG(avg_speed) as average_speed
    FROM 
        {{ source('model', 'traffic_track') }}
),

vehicle_count AS (
    SELECT 
        COUNT(DISTINCT track_id) as total_vehicles
    FROM 
        {{ source('model', 'traffic_track') }}
)

SELECT 
    total_distance,
    average_speed,
    total_vehicles
FROM 
    total_distance,
    average_speed,
    vehicle_count