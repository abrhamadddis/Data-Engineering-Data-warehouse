
CREATE TABLE trajectory_information (
    track_id DECIMAL(10, 6) NOT NULL,
    lat DECIMAL(10, 6) NOT NULL,
    lon DECIMAL(10, 6) NOT NULL,
    speed DECIMAL(10, 6) NOT NULL,
    lon_acc DECIMAL(10, 6) NOT NULL,
    lat_acc DECIMAL(10, 6) NOT NULL,
    time DECIMAL(10, 6) NOT NULL
);