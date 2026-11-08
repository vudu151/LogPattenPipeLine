-- Schema lưu kết quả pipeline (bước 1 / P0). Chạy tự động khi PostgreSQL khởi tạo lần đầu.

-- 1. Registry template: mỗi pattern Drain3 học được, kèm trạng thái known/unknown.
CREATE TABLE IF NOT EXISTS templates (
    template_id     TEXT PRIMARY KEY,                -- cluster id do Drain3 sinh (nhất quán qua Redis)
    source          TEXT NOT NULL,                   -- BGL | HDFS | OpenStack
    template        TEXT NOT NULL,                   -- vd: "instruction cache parity error corrected <*>"
    status          TEXT NOT NULL CHECK (status IN ('known','unknown','promoted')),
    first_seen      TIMESTAMPTZ NOT NULL,
    last_seen       TIMESTAMPTZ NOT NULL,
    occurrences     BIGINT NOT NULL DEFAULT 0,
    from_warmup     BOOLEAN NOT NULL DEFAULT FALSE   -- TRUE nếu học trong giai đoạn warm-up
);

-- 2. Lịch sử log đã parse (mediated schema chung cho mọi nguồn).
CREATE TABLE IF NOT EXISTS log_history (
    id              BIGSERIAL PRIMARY KEY,
    event_time      TIMESTAMPTZ NOT NULL,            -- thời điểm trong log gốc
    ingest_time     TIMESTAMPTZ NOT NULL,            -- thời điểm vào Kafka (replayer gắn)
    processed_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    source          TEXT NOT NULL,
    host            TEXT,
    level           TEXT,
    component       TEXT,
    content         TEXT NOT NULL,
    template_id     TEXT,
    status          TEXT NOT NULL CHECK (status IN ('known','unknown')),
    anomaly_score   DOUBLE PRECISION,                -- điểm Isolation Forest của window chứa dòng này
    label           TEXT,                            -- nhãn ground truth nếu dataset có (BGL: '-' hoặc alert)
    raw             TEXT
);
CREATE INDEX IF NOT EXISTS ix_log_history_event_time ON log_history (event_time DESC);
CREATE INDEX IF NOT EXISTS ix_log_history_status     ON log_history (status, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_log_history_host       ON log_history (host, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_log_history_template   ON log_history (template_id);

-- 3. Cảnh báo theo window (đầu ra của Spark job 2).
CREATE TABLE IF NOT EXISTS window_alerts (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    host            TEXT,                            -- khóa gom: host (BGL) hoặc block_id (HDFS)
    n_events        INTEGER NOT NULL,
    n_unknown       INTEGER NOT NULL DEFAULT 0,
    n_error         INTEGER NOT NULL DEFAULT 0,
    entropy         DOUBLE PRECISION,
    score           DOUBLE PRECISION NOT NULL,       -- điểm bất thường Isolation Forest, chuẩn hóa về [0,1]
    is_anomaly      BOOLEAN NOT NULL,
    reason          TEXT NOT NULL CHECK (reason IN ('normal','unknown-template','anomalous','both')),
    top_templates   JSONB,                           -- template đóng góp nhiều nhất, phục vụ giải thích trên dashboard
    label           TEXT,                            -- nhãn ground truth của window sau khi join
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (window_start, source, host)
);
CREATE INDEX IF NOT EXISTS ix_window_alerts_start   ON window_alerts (window_start DESC);
CREATE INDEX IF NOT EXISTS ix_window_alerts_anomaly ON window_alerts (is_anomaly, window_start DESC);

-- 4. Metric hiệu năng pipeline, một dòng mỗi ~10 giây.
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    stage               TEXT NOT NULL,               -- replayer | job1_parse | job2_detect
    events_per_sec      DOUBLE PRECISION,
    batch_duration_ms   DOUBLE PRECISION,            -- processing time của micro-batch
    trigger_interval_ms DOUBLE PRECISION,
    latency_p50_ms      DOUBLE PRECISION,            -- ingest_time -> processed_time
    latency_p95_ms      DOUBLE PRECISION,
    consumer_lag        BIGINT,
    num_partitions      INTEGER,
    PRIMARY KEY (ts, stage)
);

-- View tiện cho dashboard: tỉ lệ known/unknown theo phút.
CREATE OR REPLACE VIEW v_status_per_minute AS
SELECT date_trunc('minute', event_time) AS minute,
       source,
       count(*) FILTER (WHERE status = 'known')   AS n_known,
       count(*) FILTER (WHERE status = 'unknown') AS n_unknown
FROM log_history
GROUP BY 1, 2;
