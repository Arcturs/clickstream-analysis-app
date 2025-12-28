CREATE KEYSPACE IF NOT EXISTS analytics
    WITH REPLICATION = {
        'class': 'NetworkTopologyStrategy',
        'datacenter1': 2
        };

USE clickstream;

CREATE TABLE IF NOT EXISTS click_events
(
    user_id          INT,
    created_at       TIMESTAMP,
    id               TEXT,
    type             TEXT,
    received_at      TIMESTAMP,
    session_id       TEXT,
    ip               TEXT,
    url              TEXT,
    referrer         TEXT,
    device_type      TEXT,
    user_agent       TEXT,
    event_title      TEXT,
    element_id       TEXT,
    x                INT,
    y                INT,
    element_text     TEXT,
    element_class    TEXT,
    page_title       TEXT,
    viewport_width   INT,
    viewport_height  INT,
    scroll_position  DOUBLE,
    timestamp_offset BIGINT,
    metadata         MAP<TEXT, TEXT>,
    PRIMARY KEY ((user_id), created_at, id)
) WITH CLUSTERING ORDER BY (created_at DESC, id ASC);

CREATE TABLE IF NOT EXISTS click_events_by_session
(
    session_id  TEXT,
    created_at  TIMESTAMP,
    id          TEXT,
    user_id     INT,
    type        TEXT,
    received_at TIMESTAMP,
    ip          TEXT,
    url         TEXT,
    referrer    TEXT,
    device_type TEXT,
    user_agent  TEXT,
    event_title TEXT,
    PRIMARY KEY ((session_id), created_at, id)
) WITH CLUSTERING ORDER BY (created_at DESC, id ASC);

CREATE TABLE IF NOT EXISTS click_events_by_type
(
    type        TEXT,
    created_at  TIMESTAMP,
    user_id     INT,
    id          TEXT,
    session_id  TEXT,
    received_at TIMESTAMP,
    ip          TEXT,
    url         TEXT,
    referrer    TEXT,
    device_type TEXT,
    user_agent  TEXT,
    event_title TEXT,
    PRIMARY KEY ((type), created_at, user_id, id)
) WITH CLUSTERING ORDER BY (created_at DESC, user_id ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_click_events_session_id ON click_events (session_id);
CREATE INDEX IF NOT EXISTS idx_click_events_type ON click_events (type);
CREATE INDEX IF NOT EXISTS idx_click_events_event_title ON click_events (event_title);
CREATE INDEX IF NOT EXISTS idx_click_events_device_type ON click_events (device_type);