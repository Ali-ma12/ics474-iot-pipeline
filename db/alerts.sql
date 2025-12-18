CREATE TABLE IF NOT EXISTS alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    car_id TEXT,
    trip_id TEXT,
    event_timestamp TIMESTAMP,
    alert_type TEXT,
    alert_message TEXT,
    rule_name TEXT,
    severity TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);