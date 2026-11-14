-- =====================================================================================================
-- KHO DỮ LIỆU LOG TRÊN CLICKHOUSE = BƯỚC LOAD CỦA HỆ THỐNG
-- =====================================================================================================
-- Tệp này chạy MỘT LẦN khi container ClickHouse khởi động lần đầu (docker-compose gắn vào
-- /docker-entrypoint-initdb.d). Nó tạo database `logs` và 4 bảng dữ liệu:
--
--   Bảng                 Chứa gì                                              Ai ghi vào
--   -------------------  ---------------------------------------------------  ------------------------------
--   log_events           mỗi dòng log đã chuẩn hóa = một bản ghi LogEvent     mv_log_events (từ parsed-logs)
--   events_per_minute    số dòng đếm theo phút / nguồn / mức độ / trạng thái  mv_events_per_minute (từ log_events)
--   rejected_logs        dòng bị tầng chất lượng cách ly, kèm lý do           mv_rejected_logs (từ rejected-logs)
--   alerts               cảnh báo mẫu mới và cảnh báo phút bất thường         mv_alerts (từ alerts)
--
-- STORAGE WRITER KHÔNG PHẢI MỘT CHƯƠNG TRÌNH RIÊNG. Với mỗi topic Kafka có một bộ 3 đối tượng:
--
--   topic Kafka ──► bảng kafka_xxx (ENGINE = Kafka) ──► materialized view mv_xxx ──► bảng đích xxx
--                   "hàng đợi": đọc message JSON,       chạy SELECT trên mỗi lô      lưu thật trên đĩa
--                   không lưu gì                        vừa đọc, ghi sang bảng đích  (MergeTree)
--
-- Nghĩa là ClickHouse tự đọc Kafka và tự ghi vào bảng; Spark không phải ghi vào kho.
-- Vì mọi giá trị trong JSON là chuỗi, view chuyển thời gian từ chuỗi ISO 8601 sang kiểu DateTime64.
-- =====================================================================================================

CREATE DATABASE IF NOT EXISTS logs;


-- =====================================================================================================
-- 1. log_events: BẢNG CHÍNH, mỗi hàng là một dòng log đã qua bước Transform (lược đồ trung gian LogEvent)
-- =====================================================================================================
-- Dùng cho: tìm kiếm log, thống kê chéo ba hệ điều hành, xem dòng nào là mẫu mới (unknown).
CREATE TABLE IF NOT EXISTS logs.log_events
(
    -- ---- ba mốc thời gian (đều theo UTC) ----
    event_time      DateTime64(3, 'UTC'),         -- thời điểm GHI TRONG DÒNG LOG gốc, đã đổi về UTC bởi khung nhìn GAV
                                                  --   (Linux/Mac: đã suy ra năm; Windows: đã trừ 8 giờ)
    ingest_time     DateTime64(3, 'UTC'),         -- lúc Log Producer gửi dòng này lên Kafka (bước Extract)
    processed_time  DateTime64(3, 'UTC'),         -- lúc Spark job 1 xử lý xong (bước Transform)
                                                  --   processed_time - ingest_time = độ trễ của pipeline

    -- ---- các thuộc tính của lược đồ trung gian LogEvent (do khung nhìn GAV sinh ra) ----
    source          LowCardinality(String),       -- nguồn: 'Windows' | 'Linux' | 'Mac'
    host            String,                       -- tên máy; Windows không ghi nên là hằng 'WIN-CBS-01';
                                                  --   Mac: 38 bí danh DHCP đã được gộp về 'authorMacBook-Pro'
    level           LowCardinality(String),       -- mức log NGUYÊN BẢN của nguồn (chỉ Windows có: Info/Warning/Error);
                                                  --   Linux, Mac: rỗng vì syslog không có mức log
    severity        LowCardinality(String),       -- mức độ theo THANG CHUNG: INFO | WARN | ERROR | FATAL
                                                  --   = mức cao hơn giữa level (qua từ điển mã) và mức suy từ từ khóa
                                                  --   trong nội dung. Dùng cột này để so sánh giữa các hệ điều hành.
    component       String,                       -- tiến trình / thành phần ghi log (CBS, sshd, kernel...);
                                                  --   thiếu thì normalizer điền 'unknown'
    pid             Nullable(Int32),              -- mã tiến trình; NULL nếu nguồn không ghi (Windows, một số dòng Linux)
    content         String,                       -- nội dung thông điệp (phần sau header), là đầu vào của Drain

    -- ---- kết quả trích mẫu (Drain) ----
    template_id     String,                       -- định danh mẫu: 'K…' = mẫu known học lúc warm-up,
                                                  --   'U…' = mẫu mới gặp lúc chạy. Là mã băm của (nguồn, mẫu)
    template        String,                       -- mẫu log, tham số thay bằng <*>, ví dụ 'Out of Memory: Killed process <*> <*>'
    status          LowCardinality(String),       -- 'known' | 'unknown'. unknown = dòng thuộc mẫu chưa từng thấy

    -- ---- chất lượng dữ liệu và phục vụ đánh giá ----
    label           LowCardinality(String),       -- 'inject' nếu là dòng chèn thử để đánh giá; rỗng với dòng gốc
    quality_flags   String,                       -- các cờ chất lượng, cách nhau dấu phẩy, ví dụ 'host_alias,repeated'
                                                  --   (chỉ cờ loại sửa / gắn cờ; dòng bị cách ly nằm ở rejected_logs)
    injected        UInt8,                        -- 1 nếu Log Producer chèn dòng này (mẫu lạ tự đặt), 0 nếu là log gốc
    seq             Int64,                        -- số thứ tự dòng trong lần phát của Log Producer
    raw             String,                       -- dòng log NGUYÊN VĂN (nguồn gốc dữ liệu / provenance)

    -- ---- chỉ mục phụ để bỏ qua khối dữ liệu không liên quan khi lọc ----
    INDEX idx_content  content     TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,   -- tìm theo từ: WHERE hasToken(content, 'Memory')
    INDEX idx_template template_id TYPE bloom_filter            GRANULARITY 4    -- lọc nhanh theo một mẫu cụ thể
)
ENGINE = MergeTree                                -- engine lưu trữ dạng cột chuẩn của ClickHouse, nén theo cột
PARTITION BY (source, toYYYYMM(event_time))       -- mỗi (nguồn, tháng) một phân vùng: truy vấn một nguồn chỉ đọc phân vùng của nó
ORDER BY (source, event_time, host);              -- khóa sắp xếp = khóa chính: lọc theo nguồn và khoảng thời gian rất nhanh

-- Hàng đợi đọc topic `parsed-logs` (Spark job 1 ghi vào). Bảng ENGINE = Kafka không lưu dữ liệu:
-- mỗi lần SELECT trên nó là lấy message tiếp theo từ Kafka. Cột phải trùng tên trường JSON.
CREATE TABLE IF NOT EXISTS logs.kafka_parsed_logs
(
    event_time String, ingest_time String, processed_time String, source String, host String, level String,
    component String, pid Nullable(Int32), content String, raw String, label String, template_id String, template String, status String,
    injected Bool, seq Int64, severity String, quality_flags String, action String
    -- action ('ok' | 'repair') có trong message nhưng không lưu vào log_events (đã thể hiện qua quality_flags)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',                    -- địa chỉ Kafka trong mạng Docker
         kafka_topic_list = 'parsed-logs',                    -- topic cần đọc
         kafka_group_name = 'clickhouse-storage-writer',      -- consumer group: Kafka nhớ đã đọc tới đâu, khởi động lại không đọc trùng
         kafka_format = 'JSONEachRow',                        -- mỗi message là một object JSON
         kafka_num_consumers = 2,                             -- 2 luồng đọc song song
         kafka_skip_broken_messages = 1000;                   -- bỏ qua tối đa 1000 message hỏng mỗi lô thay vì dừng hẳn

-- Storage Writer cho log_events: mỗi lô đọc từ kafka_parsed_logs được đổi kiểu rồi ghi vào log_events.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs.mv_log_events TO logs.log_events AS
SELECT parseDateTime64BestEffortOrZero(event_time, 3, 'UTC')     AS event_time,      -- chuỗi ISO -> DateTime64; lỗi thì ra 1970-01-01
       parseDateTime64BestEffortOrZero(ingest_time, 3, 'UTC')    AS ingest_time,
       parseDateTime64BestEffortOrZero(processed_time, 3, 'UTC') AS processed_time,
       source, host, level, severity, component, pid, content, template_id, template, status, label, quality_flags,
       toUInt8(injected) AS injected, seq, raw
FROM logs.kafka_parsed_logs;


-- =====================================================================================================
-- 1b. events_per_minute: BẢNG TỔNG HỢP, đếm sẵn số dòng theo phút để vẽ biểu đồ nhanh
-- =====================================================================================================
-- Thay vì đếm lại hàng trăm nghìn dòng của log_events, dashboard đọc bảng nhỏ này (nhanh hơn khoảng 3 lần).
CREATE TABLE IF NOT EXISTS logs.events_per_minute
(
    minute    DateTime('UTC'),           -- đầu phút theo event_time, ví dụ 2005-11-15 10:23:00
    source    LowCardinality(String),    -- Windows | Linux | Mac
    severity  LowCardinality(String),    -- INFO | WARN | ERROR | FATAL
    status    LowCardinality(String),    -- known | unknown
    events    UInt64                     -- số dòng thuộc nhóm (phút, nguồn, mức độ, trạng thái)
)
ENGINE = SummingMergeTree                -- khi gộp các phần dữ liệu, hàng trùng khóa ORDER BY được CỘNG dồn cột events
ORDER BY (source, minute, severity, status);
-- Lưu ý: việc cộng dồn diễn ra ở nền, nên khi truy vấn luôn dùng sum(events) ... GROUP BY, không đọc thẳng cột events.

-- Mỗi khi log_events nhận một lô mới, view này đếm lô đó theo phút và ghi kết quả vào events_per_minute.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs.mv_events_per_minute TO logs.events_per_minute AS
SELECT toStartOfMinute(event_time) AS minute, source, severity, status, count() AS events
FROM logs.log_events GROUP BY minute, source, severity, status;


-- =====================================================================================================
-- 2. rejected_logs: DÒNG BỊ CÁCH LY bởi tầng chất lượng (normalizer), giữ lại để tra cứu thay vì xóa
-- =====================================================================================================
-- Spark job 1 gửi các dòng vi phạm luật loại "cách ly" sang topic `rejected-logs`.
CREATE TABLE IF NOT EXISTS logs.rejected_logs
(
    event_time    DateTime64(3, 'UTC'),       -- thời điểm trong dòng log nếu đọc được; 1970-01-01 nếu không tách được
    ingest_time   DateTime64(3, 'UTC'),       -- lúc Log Producer gửi lên Kafka
    source        LowCardinality(String),     -- Windows | Linux | Mac
    host          String,                     -- tên máy nếu đọc được, 'unparsed' nếu wrapper không khớp
    quality_flags String,                     -- LÝ DO cách ly: unparsable | repeat_marker | empty_content |
                                              --   ts_out_of_range | invalid_level (có thể nhiều, cách nhau dấu phẩy)
    raw           String                      -- dòng log nguyên văn, để xem lại vì sao bị loại
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


-- =====================================================================================================
-- 3. alerts: CẢNH BÁO, cùng nội dung với topic `alerts` mà Alert System đọc
-- =====================================================================================================
-- Alert System gộp cảnh báo trùng trước khi thông báo, còn bảng này giữ ĐỦ mọi cảnh báo để tra cứu và vẽ dashboard.
CREATE TABLE IF NOT EXISTS logs.alerts
(
    detected_at DateTime64(3, 'UTC'),         -- lúc hệ thống phát cảnh báo (giờ thật)
    event_time  DateTime64(3, 'UTC'),         -- thời điểm trong log mà cảnh báo nói tới (lần đầu gặp mẫu / đầu cửa sổ)
    alert_id    String,                       -- 'tpl-<template_id>' với mẫu mới, 'win-<nguồn>-<thời điểm>' với cửa sổ
    type        LowCardinality(String),       -- 'new-template'   : job 1 gặp một mẫu log lần đầu
                                              -- 'window-anomaly' : job 2 chấm một phút là bất thường
    level       LowCardinality(String),       -- HIGH (có mẫu lạ) | MEDIUM (chỉ do điểm Isolation Forest)
    source      LowCardinality(String),       -- Windows | Linux | Mac
    title       String,                       -- một dòng mô tả ngắn, hiển thị cho người vận hành
    detail      String                        -- JSON chi tiết: template, điểm số, ngưỡng, số dòng unknown...
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
