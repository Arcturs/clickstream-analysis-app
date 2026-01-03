CREATE TABLE IF NOT EXISTS user_activity_daily (
    date Date,
    dau UInt32,
    wau UInt32,
    mau UInt32,
    wau_to_dau_ratio Float32,
    mau_to_dau_ratio Float32,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test/replicated_table', '{replica}')
ORDER BY date;

CREATE TABLE IF NOT EXISTS element_conversion (
    date Date,
    element_id String,
    views UInt32,
    clicks UInt32,
    conversion_rate Float32,
    avg_x Float32,
    avg_y Float32,
    total_interactions UInt32,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test/replicated_table', '{replica}')
ORDER BY (date, element_id);

CREATE TABLE IF NOT EXISTS session_metrics_daily (
    date Date,
    avg_duration_seconds Float32,
    median_duration_seconds Float32,
    std_duration_seconds Float32,
    total_events UInt32,
    session_count UInt32,
    unique_users UInt32,
    avg_duration_minutes Float32,
    median_duration_minutes Float32,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test/replicated_table', '{replica}')
ORDER BY date;

CREATE TABLE IF NOT EXISTS device_distribution_daily (
    date Date,
    device_type String,
    unique_users UInt32,
    unique_sessions UInt32,
    device_events UInt32,
    user_percentage Float32,
    session_percentage Float32,
    event_percentage Float32,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test/replicated_table', '{replica}')
ORDER BY (date, device_type);

CREATE TABLE IF NOT EXISTS top_pages_daily (
    date Date,
    url String,
    unique_users UInt32,
    unique_sessions UInt32,
    view_count UInt32,
    click_count UInt32,
    conversion_rate Float32,
    rank UInt8,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test/replicated_table', '{replica}')
ORDER BY (date, rank);