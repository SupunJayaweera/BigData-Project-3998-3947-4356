CREATE TABLE IF NOT EXISTS sensor_readings (
    id              BIGSERIAL PRIMARY KEY,
    sensor_id       VARCHAR(50)     NOT NULL,
    event_time      TIMESTAMPTZ     NOT NULL,   -- from producer timestamp
    processing_time TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    vehicle_count   INTEGER         NOT NULL,
    avg_speed       NUMERIC(5,2)    NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sr_sensor_event ON sensor_readings (sensor_id, event_time);
CREATE INDEX IF NOT EXISTS idx_sr_event_time   ON sensor_readings (event_time);

CREATE TABLE IF NOT EXISTS congestion_windows (
    id                  BIGSERIAL PRIMARY KEY,
    sensor_id           VARCHAR(50)     NOT NULL,
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    total_vehicle_count INTEGER         NOT NULL,
    avg_speed_window    NUMERIC(5,2)    NOT NULL,
    congestion_index    NUMERIC(5,4)    NOT NULL,
    is_congested        BOOLEAN         NOT NULL,
    computed_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cw_sensor_window ON congestion_windows (sensor_id, window_start);

CREATE TABLE IF NOT EXISTS critical_alerts (
    id              BIGSERIAL PRIMARY KEY,
    sensor_id       VARCHAR(50)     NOT NULL,
    event_time      TIMESTAMPTZ     NOT NULL,
    vehicle_count   INTEGER         NOT NULL,
    avg_speed       NUMERIC(5,2)    NOT NULL,
    alert_type      VARCHAR(50)     NOT NULL DEFAULT 'SPEED_BELOW_10',
    alerted_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ca_sensor_time ON critical_alerts (sensor_id, event_time);
