#!/bin/bash

set -e

echo "=== Начало создания таблиц в кластере ClickHouse ==="

wait_for_clickhouse() {
    local host=$1
    local port=$2
    local max_attempts=30
    local attempt=1

    echo "Ожидание доступности ClickHouse на $host:$port..."

    while [ $attempt -le $max_attempts ]; do
        if clickhouse-client --host "$host" --port "$port" --user admin --password admin --query "SELECT 1" 2>/dev/null; then
            echo "ClickHouse на $host:$port доступен"
            return 0
        fi
        echo "Попытка $attempt из $max_attempts: ClickHouse не доступен, жду 2 секунды..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo "Ошибка: ClickHouse на $host:$port не стал доступен"
    return 1
}

wait_for_clickhouse clickhouse-01 9000

echo "=== Создание базы данных analytics ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --multiquery << 'EOF'
-- Создаем базу данных на всем кластере
CREATE DATABASE IF NOT EXISTS analytics
ON CLUSTER cluster_1S_2R;

-- Проверяем создание базы данных
SHOW DATABASES;

-- Проверяем настройки макросов
SELECT * FROM system.macros;
EOF

echo "=== Создание таблицы user_activity_daily ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --multiquery << 'EOF'
CREATE TABLE IF NOT EXISTS analytics.user_activity_daily
ON CLUSTER cluster_1S_2R (
    date Date,
    dau UInt32,
    wau UInt32,
    mau UInt32,
    wau_to_dau_ratio Float32,
    mau_to_dau_ratio Float32,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date
SETTINGS index_granularity = 8192;
EOF

echo "=== Создание таблицы element_conversion ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --multiquery << 'EOF'
CREATE TABLE IF NOT EXISTS analytics.element_conversion
ON CLUSTER cluster_1S_2R (
    date Date,
    element_id String,
    views UInt32,
    clicks UInt32,
    conversion_rate Float32,
    avg_x Float32,
    avg_y Float32,
    total_interactions UInt32,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, element_id)
SETTINGS index_granularity = 8192;
EOF

echo "=== Создание таблицы session_metrics_daily ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --multiquery << 'EOF'
CREATE TABLE IF NOT EXISTS analytics.session_metrics_daily
ON CLUSTER cluster_1S_2R (
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
) ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date
SETTINGS index_granularity = 8192;
EOF

echo "=== Создание таблицы device_distribution_daily ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --multiquery << 'EOF'
CREATE TABLE IF NOT EXISTS analytics.device_distribution_daily
ON CLUSTER cluster_1S_2R (
    date Date,
    device_type String,
    unique_users UInt32,
    unique_sessions UInt32,
    device_events UInt32,
    user_percentage Float32,
    session_percentage Float32,
    event_percentage Float32,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, device_type)
SETTINGS index_granularity = 8192;
EOF

echo "=== Создание таблицы top_pages_daily ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --multiquery << 'EOF'
CREATE TABLE IF NOT EXISTS analytics.top_pages_daily
ON CLUSTER cluster_1S_2R (
    date Date,
    url String,
    unique_users UInt32,
    unique_sessions UInt32,
    view_count UInt32,
    click_count UInt32,
    conversion_rate Float32,
    rank UInt8,
    calculation_timestamp DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, rank)
SETTINGS index_granularity = 8192;
EOF

echo "=== Проверка созданных таблиц ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --format Pretty --query "
SELECT
    name AS table_name,
    engine,
    formatReadableQuantity(total_rows) AS rows,
    formatReadableSize(total_bytes) AS size,
    partition_key,
    sorting_key
FROM system.tables
WHERE database = 'analytics'
ORDER BY table_name"

echo "=== Проверка репликации на всех нодах ==="

echo "Таблицы на clickhouse-01:"
clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --query "
SELECT count() as table_count FROM system.tables WHERE database = 'analytics'"

echo "Таблицы на clickhouse-02:"
clickhouse-client --host clickhouse-02 --port 9000 --user admin --password admin --query "
SELECT count() as table_count FROM system.tables WHERE database = 'analytics'"

echo "=== Статус репликации ==="

clickhouse-client --host clickhouse-01 --port 9000 --user admin --password admin --format Pretty --query "
SELECT
    database,
    table,
    replica_name,
    is_leader,
    is_readonly,
    is_session_expired,
    replica_is_active,
    zookeeper_path
FROM system.replicas
WHERE database = 'analytics'
ORDER BY table"

echo "=== Создание таблиц завершено успешно! ==="