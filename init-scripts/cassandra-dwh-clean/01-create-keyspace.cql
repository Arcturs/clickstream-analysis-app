CREATE KEYSPACE IF NOT EXISTS analytics
    WITH REPLICATION = {
        'class': 'NetworkTopologyStrategy',
        'DC1': 2
        };

USE analytics;

CREATE TABLE IF NOT EXISTS clean_click_events
(
    user_id          INT,
    created_at       TIMESTAMP,
    id               TEXT,
    type             TEXT,
    session_id       TEXT,
    url              TEXT,
    device_type      TEXT,
    event_title      TEXT,
    element_id       TEXT,
    x                INT,
    y                INT
) WITH CLUSTERING ORDER BY (created_at DESC, id ASC);

CREATE INDEX IF NOT EXISTS idx_clean_click_events_session_id ON clean_click_events (session_id);
CREATE INDEX IF NOT EXISTS idx_clean_click_events_type ON clean_click_events (type);
CREATE INDEX IF NOT EXISTS idx_clean_click_events_event_title ON clean_click_events (event_title);
CREATE INDEX IF NOT EXISTS idx_clean_click_events_device_type ON clean_click_events (device_type);

CREATE TABLE IF NOT EXISTS invalid_events
(
    user_id          INT,
    id               TEXT,
    type             TEXT,
    session_id       TEXT,
    event_time       TIMESTAMP,
    processed_at     TIMESTAMP,
    validation_error TEXT
) WITH CLUSTERING ORDER BY (id ASC);