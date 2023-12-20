CREATE TABLE IF NOT EXISTS  traffic_track (
   track_id INT PRIMARY KEY,
   type varchar(20),
   traveled_d FLOAT,
   avg_speed FLOAT,
);

CREATE TABLE IF NOT EXISTS  traffic_trajectory (
   track_id INT PRIMARY KEY,
   lat FLOAT,
   lon FLOAT,
   speed FLOAT, 
   lon_acc FLOAT,
   lat_acc FLOAT,
   time FLOAT
);