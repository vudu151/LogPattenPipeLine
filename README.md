# LogPattenPipeLine

Pipeline xử lý log thời gian thực để **phát hiện mẫu log chưa biết** (unknown log pattern) trên ba hệ thống của Loghub: BGL, HDFS, OpenStack.

Đồ án cuối kỳ học phần **IT5427 – Tích hợp và xử lý dữ liệu lớn**, Trường CNTT&TT, Đại học Bách khoa Hà Nội.

## Ý tưởng

Mỗi dòng log gồm phần cố định và phần tham số. Thay tham số bằng `<*>` thì được **mẫu log**:

```
Dòng log :  PacketResponder 1 for block blk_38865049064139660 terminating
Mẫu log  :  PacketResponder <*> for block <*> terminating
```

Hệ thống học các mẫu từ log bình thường (mẫu **known**). Khi chạy, dòng nào không khớp mẫu đã học là **unknown** và sinh cảnh báo. Để làm được việc đó trên ba hệ thống ghi ba định dạng khác nhau, pipeline phải:

1. đưa ba định dạng về **một lược đồ chung** (`LogEvent`);
2. **kiểm tra chất lượng** từng dòng (thiếu giá trị, lệch cột, trùng lặp);
3. trích mẫu và phát hiện bất thường trên luồng;
4. lưu vào kho OLAP để tìm kiếm, và cảnh báo.

## Kiến trúc

```
Tệp Loghub ─► Log Producer ─► Kafka raw-logs.* ─► Spark job 1 ─► Kafka parsed-logs ─► ClickHouse (OLAP) ─► Grafana, bảng điều khiển
                                                  lib-log-handler      │
                                                  + Drain3             └─► Spark job 2 (cửa sổ 1 phút, Isolation Forest)
                                                     │                          │
                                                     ├─► rejected-logs          │
                                                     └─► alerts ◄───────────────┘ ─► Alert System
```

| Thành phần | Nằm ở | Làm gì |
|---|---|---|
| Công cụ tải dữ liệu | `tools/download_datasets.py` | Tải BGL, HDFS, OpenStack và mẫu Loghub-2k |
| Log Producer | `log_producer/producer.py` | Phát log thô lên Kafka theo nhịp thời gian gốc; có thể chèn mẫu lạ, giả log đến trễ |
| **lib-log-handler** | `lib_log_handler/` | Đưa log về lược đồ chung (GAV), mô tả nguồn (LAV), kiểm tra chất lượng, ghép nhãn |
| Message Queue | Kafka | Topic `raw-logs.*`, `parsed-logs`, `rejected-logs`, `alerts` |
| Stream Processor | `jobs/job1_parse.py`, `jobs/job2_detect.py` | Job 1: chuẩn hóa, trích mẫu, gắn known/unknown. Job 2: chấm điểm từng phút |
| Mô hình ML | `models/`, `features/` | Isolation Forest có sẵn, chỉ dùng để kiểm tra |
| Storage Writer + OLAP | `clickhouse/init.sql` | ClickHouse tự đọc topic và ghi vào bảng `logs.log_events` |
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
./pipeline.sh download all                           # BGL, HDFS, OpenStack, Loghub-2k

# 2. Học mẫu known và huấn luyện mô hình (làm một lần)
./pipeline.sh warmup --dataset BGL --fraction 0.2    # tương tự với HDFS
./pipeline.sh warmup --dataset OpenStack --fraction 1.0 --skip-value 0
./pipeline.sh train --dataset BGL --key ALL --window 60   # tương tự với HDFS, OpenStack

# 3. Bật xử lý luồng và cảnh báo
./pipeline.sh job1 && ./pipeline.sh job2
./pipeline.sh alerts                                 # chạy ở cửa sổ terminal riêng

# 4. Phát log (có chèn 5 mẫu lạ) và xem kết quả
./pipeline.sh produce --dataset BGL --skip 949592 --limit 300000 --rate 4000 \
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

## lib-log-handler

Hàm chính `handle(source, line)` làm ba bước: wrapper GAV → ghép nhãn → normalizer. Kết quả là một bản ghi `LogEvent` kèm hành động giữ, sửa hoặc cách ly.

- **GAV** (`gav.py`): `LogEvent ⊇ V_BGL(BGL) ∪ V_HDFS(HDFS) ∪ V_OpenStack(OpenStack)`. Mỗi khung nhìn khai báo từng thuộc tính của LogEvent lấy từ trường nào của nguồn, qua phép biến đổi nào.
- **LAV** (`lav.py`): mỗi nguồn là một khung nhìn trên LogEvent, kèm loại thực thể, cách gán nhãn, miền thời gian. Dùng để chọn nguồn trả lời một truy vấn và viết lại thành SQL.
- **Normalizer** (`normalizer.py`): bảy luật chất lượng (5 cách ly, 1 sửa, 1 gắn cờ) và từ điển đưa 8 mã mức log về thang 4 bậc.

```bash
./pipeline.sh gav     # in khung nhìn GAV của ba nguồn dạng luật
./pipeline.sh lav     # mô tả nguồn LAV, ví dụ chọn nguồn và viết lại truy vấn
```

## Đánh giá

```bash
./pipeline.sh profile --dataset BGL                       # hồ sơ nguồn
./pipeline.sh quality --dataset BGL                       # hồ sơ chất lượng theo từng luật
./pipeline.sh eval-parsing                                # độ chính xác trích mẫu trên Loghub-2k
./pipeline.sh eval-detect --dataset BGL --lines 1000000   # theo dòng và cửa sổ
./pipeline.sh eval-detect --dataset HDFS --lines 2000000  # theo khối
./pipeline.sh eval-detect --dataset OpenStack             # theo máy ảo
```

| Kết quả | Giá trị |
|---|---|
| Chất lượng dữ liệu BGL | 0,73% dòng bị cách ly; 2,46% dòng thiếu định danh nút được sửa |
| Trích mẫu (Grouping Accuracy) | BGL 0,970 · HDFS 0,998 · OpenStack 0,849 |
| Phát hiện bằng mẫu chưa biết | BGL: recall 99,7%, precision 39,6% · HDFS: recall 53,1%, precision 92,8% · OpenStack: không phát hiện được (lỗi không sinh mẫu mới) |
| Kho OLAP | 618.353 dòng nén còn 39,6 MiB (4,9 lần); truy vấn tổng hợp 10–32 ms |
| Cảnh báo | 260 cảnh báo được gộp thành 19 thông báo |

## Tài liệu tham khảo

1. P. He et al. *Drain: An Online Log Parsing Approach with Fixed Depth Tree.* ICWS 2017.
2. J. Zhu et al. *Tools and Benchmarks for Automated Log Parsing.* ICSE-SEIP 2019.
3. J. Zhu et al. *Loghub.* ISSRE 2023. https://github.com/logpai/loghub
4. F. T. Liu, K. M. Ting, Z.-H. Zhou. *Isolation Forest.* ICDM 2008.
5. A. Doan, A. Halevy, Z. Ives. *Principles of Data Integration.* 2012.
