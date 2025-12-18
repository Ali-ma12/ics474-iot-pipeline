-- Correct Data Table (device_data)
CREATE TABLE device_data (
    trip_id TEXT,
    car_id TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    event_timestamp TIMESTAMP,
    speed_kmph DOUBLE PRECISION,
    fuel_level DOUBLE PRECISION,
    engine_temp_c DOUBLE PRECISION,
    trip_start_time TIMESTAMP,
    trip_start_latitude DOUBLE PRECISION,
    trip_start_longitude DOUBLE PRECISION,
    trip_start_date DATE,
    message_key TEXT,
    kafka_partition INT,
    kafka_offset BIGINT,
    load_timestamp TIMESTAMP
);

-- Corrupt Data Table (corrupt_records)
CREATE TABLE corrupt_records (
    message_key TEXT,
    kafka_partition INT,
    kafka_offset BIGINT,
    value_str TEXT,                -- Raw JSON string from Kafka
    error_reason TEXT,             -- Explanation of the error
    load_timestamp TIMESTAMP,      -- When inserted
    topic_name TEXT                -- Kafka topic name
);

-- Error Log Table (consumer_error_log)
CREATE TABLE consumer_error_log (
    error_message TEXT,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    target_table TEXT,
    batch_id BIGINT
);