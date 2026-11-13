-- Kho OLAP cho log da chuan hoa (ClickHouse) va STORAGE WRITER.
-- Storage Writer = bang engine Kafka + materialized view: ClickHouse tu doc topic va ghi vao bang MergeTree,
-- khong can tien trinh rieng. Moi topic mot cap (hang doi Kafka, view ghi).

CREATE DATABASE IF NOT EXISTS logs;

-- ============ 1. Log da chuan hoa: bang chinh de tim kiem va phan tich ============
CREATE TABLE IF NOT EXISTS logs.log_events
(
    event_time      DateTime64(3, 'UTC'),
    ingest_time     DateTime64(3, 'UTC'),
    processed_time  DateTime64(3, 'UTC'),
    source          LowCardinality(String),
    host            String,
    level           LowCardinality(String),
    severity        LowCardinality(String),
    component       String,
    content         String,
    template_id     String,
    template        String,
    status          LowCardinality(String),
    label           LowCardinality(String),
    quality_flags   String,
    injected        UInt8,
    seq             Int64,
    raw             String,
    INDEX idx_content  content     TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,   -- tim theo tu trong noi dung
    INDEX idx_template template_id TYPE bloom_filter            GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY (source, toYYYYMM(event_time))
ORDER BY (source, event_time, host);

CREATE TABLE IF NOT EXISTS logs.kafka_parsed_logs
(
    event_time String, ingest_time String, processed_time String, source String, host String, level String,
    component String, content String, raw String, label String, template_id String, template String, status String,
    injected Bool, seq Int64, severity String, quality_flags String, action String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'parsed-logs', kafka_group_name = 'clickhouse-storage-writer',
         kafka_format = 'JSONEachRow', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs.mv_log_events TO logs.log_events AS
SELECT parseDateTime64BestEffortOrZero(event_time, 3, 'UTC')     AS event_time,
       parseDateTime64BestEffortOrZero(ingest_time, 3, 'UTC')    AS ingest_time,
       parseDateTime64BestEffortOrZero(processed_time, 3, 'UTC') AS processed_time,
       source, host, level, severity, component, content, template_id, template, status, label, quality_flags,
       toUInt8(injected) AS injected, seq, raw
FROM logs.kafka_parsed_logs;

-- ============ 2. Dong bi cach ly boi tang chat luong ============
CREATE TABLE IF NOT EXISTS logs.rejected_logs
(
    event_time DateTime64(3, 'UTC'), ingest_time DateTime64(3, 'UTC'), source LowCardinality(String),
    host String, quality_flags String, raw String
)
ENGINE = MergeTree ORDER BY (source, ingest_time);

CREATE TABLE IF NOT EXISTS logs.kafka_rejected_logs
(source String, host String, event_time String, ingest_time String, quality_flags String, raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rejected-logs', kafka_group_name = 'clickhouse-storage-writer',
         kafka_format = 'JSONEachRow', kafka_skip_broken_messages = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs.mv_rejected_logs TO logs.rejected_logs AS
SELECT parseDateTime64BestEffortOrZero(event_time, 3, 'UTC') AS event_time,
       parseDateTime64BestEffortOrZero(ingest_time, 3, 'UTC') AS ingest_time, source, host, quality_flags, raw
FROM logs.kafka_rejected_logs;

-- ============ 3. Canh bao (cung noi dung voi topic alerts ma Alert System doc) ============
CREATE TABLE IF NOT EXISTS logs.alerts
(
    detected_at DateTime64(3, 'UTC'), event_time DateTime64(3, 'UTC'), alert_id String, type LowCardinality(String),
    level LowCardinality(String), source LowCardinality(String), title String, detail String
)
ENGINE = MergeTree ORDER BY (detected_at, alert_id);

CREATE TABLE IF NOT EXISTS logs.kafka_alerts
(alert_id String, type String, level String, source String, event_time String, detected_at String, title String, detail String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'alerts', kafka_group_name = 'clickhouse-storage-writer',
         kafka_format = 'JSONEachRow', kafka_skip_broken_messages = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs.mv_alerts TO logs.alerts AS
SELECT parseDateTime64BestEffortOrZero(detected_at, 3, 'UTC') AS detected_at,
       parseDateTime64BestEffortOrZero(event_time, 3, 'UTC') AS event_time, alert_id, type, level, source, title, detail
FROM logs.kafka_alerts;
