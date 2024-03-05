SELECT 
    SUM(traveled_d) as total_distance
FROM 
    {{ source('model', 'traffic_track') }}