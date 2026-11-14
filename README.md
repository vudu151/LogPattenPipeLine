# LogPattenPipeLine

Pipeline xử lý log thời gian thực để **phát hiện mẫu log chưa biết** (unknown log pattern) trên log của ba hệ điều hành trong bộ Loghub: **Windows, Linux, Mac**.

Đồ án cuối kỳ học phần **IT5427 – Tích hợp và xử lý dữ liệu lớn**, Trường CNTT&TT, Đại học Bách khoa Hà Nội.

## Ý tưởng

Mỗi dòng log gồm phần cố định và phần tham số. Thay tham số bằng `<*>` thì được **mẫu log**:

```
Dòng log :  Out of Memory: Killed process 28473 (httpd).
Mẫu log  :  Out of Memory: Killed process <*> <*>
```

Hệ thống học các mẫu từ phần đầu của log (mẫu **known**). Khi chạy, dòng nào không khớp mẫu đã học là **unknown** và sinh cảnh báo. Ba hệ điều hành ghi log theo ba cách khác nhau, nên hệ thống được tổ chức như một **kho dữ liệu** với ba bước:

| Bước | Thành phần | Làm gì |
|---|---|---|
| **Extract** | Log Producer → Kafka `raw-logs.*` | Đọc log thô của từng hệ điều hành, đẩy lên hàng đợi |
| **Transform** | Spark job 1 + `lib-log-handler` | Đưa ba định dạng về lược đồ chung `LogEvent` bằng ánh xạ **GAV**, kiểm tra chất lượng, trích mẫu |
| **Load** | ClickHouse (Storage Writer) | Nạp vào kho dữ liệu để tìm kiếm và phân tích |

## Ánh xạ GAV: ba nguồn về một lược đồ

`LogEvent ⊇ V_Windows(Windows) ∪ V_Linux(Linux) ∪ V_Mac(Mac)`

| Thuộc tính LogEvent | Windows (CBS.log) | Linux (syslog) | Mac (system.log) |
|---|---|---|---|
| `event_time` (UTC) | `local_time(date, time, UTC+8)` | `syslog_time(month, day, time, UTC+0)`: nguồn không ghi năm, năm được suy từ miền thời gian của nguồn | `syslog_time(month, day, time, UTC−7)` |
| `host` | hằng `'WIN-CBS-01'` (nguồn không ghi tên máy) | `host` | `host`, sau đó gộp 37 bí danh DHCP về một tên chuẩn |
| `component` | `component` | `component` | `component` |
| `pid` | `null` | `int(pid)` | `int(pid)` |
| `level` | `level` | `null` | `null` |
| `severity` | `severity(level, content)` | `severity(null, content)` | `severity(null, content)` |
| `content` | `content` | `content` | `content` |

`severity` = mức cao hơn giữa mức log của nguồn (qua từ điển mã) và mức suy từ từ khóa trong nội dung. Nhờ đó Linux và Mac (không có mức log) vẫn so sánh được với Windows.

```bash
./pipeline.sh gav     # in ba khung nhìn GAV dạng luật
./pipeline.sh lav     # mô tả nguồn LAV, ví dụ chọn nguồn và viết lại truy vấn
```

## Kiến trúc

```
Tệp log 3 HĐH ─► Log Producer ─► Kafka raw-logs.* ─► Spark job 1 ─► Kafka parsed-logs ─► ClickHouse (kho dữ liệu) ─► Grafana, bảng điều khiển
   (EXTRACT)                                        lib-log-handler      │                    (LOAD)
                                                    + Drain3             └─► Spark job 2 (cửa sổ 1 phút, Isolation Forest)
                                                    (TRANSFORM)                 │
                                                       ├─► rejected-logs        │
                                                       └─► alerts ◄─────────────┘ ─► Alert System
```

| Thành phần | Nằm ở | Làm gì |
|---|---|---|
| Công cụ tải dữ liệu | `tools/download_datasets.py` | Tải log Windows (5 triệu dòng đầu), Linux, Mac và mẫu Loghub-2k |
| Log Producer | `log_producer/producer.py` | Phát log thô lên Kafka theo nhịp thời gian gốc; có thể chèn mẫu lạ, giả log đến trễ |
| **lib-log-handler** | `lib_log_handler/` | Sổ đăng ký nguồn, lược đồ `LogEvent`, ánh xạ GAV, mô tả nguồn LAV, 8 luật chất lượng |
| Message Queue | Kafka | Topic `raw-logs.*`, `parsed-logs`, `rejected-logs`, `alerts` |
| Stream Processor | `jobs/job1_parse.py`, `jobs/job2_detect.py` | Job 1: chuẩn hóa, trích mẫu, gắn known/unknown. Job 2: chấm điểm từng phút |
| Mô hình ML | `models/`, `features/` | Isolation Forest có sẵn, chỉ dùng để kiểm tra |
| Storage Writer + kho dữ liệu | `clickhouse/init.sql` | ClickHouse tự đọc topic và ghi vào `logs.log_events`; bảng tổng hợp `events_per_minute` |
| Alert System | `alerting/alert_service.py` | Đọc topic `alerts`, gộp cảnh báo trùng, in màn hình / ghi tệp / gửi webhook |
| Dashboard | `grafana/`, `panel/app.py` | Grafana; bảng điều khiển Streamlit để demo và tìm kiếm log |
| Siêu dữ liệu | PostgreSQL, Redis | Sổ mẫu, cửa sổ đã chấm điểm, thống kê; Redis giữ cây mẫu Drain dùng chung |

## Yêu cầu

Linux, Docker Engine ≥ 24 với Docker Compose v2, 8 GB RAM trống, Python ≥ 3.10.
Clone vào thư mục có tên đơn giản (không khoảng trắng, không ký tự đặc biệt).

## Chạy

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-host.txt

# 1. Dựng hạ tầng và tải dữ liệu
./pipeline.sh up && ./pipeline.sh check              # 9 dịch vụ
./pipeline.sh download all                           # Windows (5 triệu dòng đầu), Linux, Mac, Loghub-2k

# 2. Học mẫu known và huấn luyện mô hình (làm một lần, cho từng nguồn)
for d in Windows Linux Mac; do
  ./pipeline.sh warmup --dataset $d --fraction 0.2
  ./pipeline.sh train  --dataset $d --key ALL --window 60
done

# 3. Bật xử lý luồng và cảnh báo
./pipeline.sh job1 && ./pipeline.sh job2
./pipeline.sh alerts                                 # chạy ở cửa sổ terminal riêng

# 4. Phát phần log sau warm-up (có chèn 5 mẫu lạ) và xem kết quả
./pipeline.sh produce --dataset Linux --skip 5113 --limit 20454 --rate 300 \
    --inject log_producer/inject_templates.txt --at 0.5 --inject-count 50
./pipeline.sh ch "SELECT source, status, count() FROM logs.log_events GROUP BY source, status"
```

Không muốn gõ lệnh: chạy `./pipeline.sh panel` rồi mở http://localhost:8501 và bấm nút.
Xem mọi lệnh: `./pipeline.sh help`.

## Giao diện

| Giao diện | Địa chỉ |
|---|---|
| Bảng điều khiển demo và tìm kiếm log | http://localhost:8501 |
| Grafana, dashboard “Log Pattern Pipeline” | http://localhost:3000 (admin/admin) |
| Kafka UI | http://localhost:8090 |
| Spark master; job 1; job 2 | http://localhost:8080; :4040; :4041 |
| ClickHouse | http://localhost:8123/play (app/app) |

## Đánh giá

Log hệ điều hành của Loghub không có nhãn bất thường, nên việc phát hiện được đánh giá bằng **dòng chèn thử**.

```bash
./pipeline.sh profile --dataset Linux          # hồ sơ nguồn
./pipeline.sh quality --dataset Linux          # hồ sơ chất lượng theo từng luật
./pipeline.sh eval-parsing                     # độ chính xác trích mẫu trên Loghub-2k
./pipeline.sh eval-novelty --dataset Linux     # tỉ lệ dòng unknown theo độ dài warm-up
./pipeline.sh eval-detect --dataset Linux      # chèn 100 dòng mẫu lạ, đo tỉ lệ bắt được
```

| Kết quả | Giá trị |
|---|---|
| Wrapper so với đáp án Loghub-2k | 5.999 / 6.000 dòng tách đúng |
| Chất lượng dữ liệu | Mac: 12,4% dòng bị cách ly, 38 tên máy gộp về 1 · Linux: 0,4% · Windows: xem báo cáo |
| Trích mẫu (Grouping Accuracy) | Windows 0,999 · Linux 0,690 · Mac 0,811 |
| Phát hiện dòng chèn | 100 / 100 ở cả ba hệ điều hành |
| Sự cố thật tìm được | Linux: 10.404 dòng `Out of Memory: Killed process` từ 15/11/2005 |
| Kho dữ liệu | 399.450 dòng nén còn 20,3 MiB (10,5 lần); truy vấn 2–18 ms |
| Cảnh báo | 6.341 cảnh báo được gộp thành 32 thông báo |

## Tài liệu tham khảo

1. P. He et al. *Drain: An Online Log Parsing Approach with Fixed Depth Tree.* ICWS 2017.
2. J. Zhu et al. *Tools and Benchmarks for Automated Log Parsing.* ICSE-SEIP 2019.
3. J. Zhu et al. *Loghub.* ISSRE 2023. https://github.com/logpai/loghub
4. F. T. Liu, K. M. Ting, Z.-H. Zhou. *Isolation Forest.* ICDM 2008.
5. A. Doan, A. Halevy, Z. Ives. *Principles of Data Integration.* 2012.
