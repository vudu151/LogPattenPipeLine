# LogPattenPipeLine

Pipeline xử lý nhật ký (log) thời gian thực để **phát hiện mẫu log chưa biết** (unknown log pattern), xây dựng trên Apache Kafka, Apache Spark Structured Streaming, Drain3 và Isolation Forest.

Đồ án cuối kỳ học phần **IT5427 – Tích hợp và xử lý dữ liệu lớn**, Trường CNTT&TT, Đại học Bách khoa Hà Nội.

> Tài liệu kỹ thuật đầy đủ (kiến trúc, phương pháp, hướng dẫn từng bước, kết quả): mở [`docs/code_workflow.html`](docs/code_workflow.html) bằng trình duyệt.

## Bài toán

Hệ thống lớn sinh log liên tục. Sự cố thường xuất hiện theo hai dạng:

1. **Mẫu chưa từng thấy**: một dạng thông điệp mới xuất hiện.
2. **Mẫu đã biết nhưng tần suất bất thường**: ví dụ một lỗi bộ nhớ tăng từ vài chục lên gần 1.000 dòng mỗi phút.

Pipeline phát hiện cả hai dạng mà không cần định nghĩa trước danh sách lỗi.

## Kiến trúc

```
Loghub files ─► Replayer ─► Kafka raw-logs.* ─► Spark job 1 ─► Kafka parsed-logs ─► Spark job 2 ─► PostgreSQL ─► Grafana / Panel
                (Python)                        parse + Drain3                       window 1 phút
                                                known / unknown                      Isolation Forest
                                                     │
                                                   Redis (cây mẫu warm-up dùng chung)
```

| Tầng | Thành phần | Vai trò |
|---|---|---|
| Thu thập | `replayer/replay.py`, Kafka | Phát lại log Loghub theo nhịp thời gian gốc, có tăng tốc, log đến trễ, chèn mẫu lạ |
| Xử lý 1 | `jobs/job1_parse.py`, Drain3, Redis | Chuẩn hóa về lược đồ chung, gán mẫu, phân loại known / unknown |
| Xử lý 2 | `jobs/job2_detect.py`, Isolation Forest | Gom cửa sổ 1 phút (watermark 2 phút), chấm điểm bất thường, ghi lý do |
| Phục vụ | PostgreSQL, Grafana, `panel/app.py` | Lưu `templates`, `log_history`, `window_alerts`, `pipeline_metrics`; hiển thị |

## Phương pháp phát hiện

- **Tầng 1 – mẫu chưa biết.** Drain học tập mẫu *known* từ 20% đầu dữ liệu (chỉ dòng bình thường). Khi chạy, dòng không khớp cây warm-up được gắn `unknown`. Mẫu unknown lặp lại ≥ 500 lần được thăng cấp thành known để thích nghi với trôi dạt.
- **Tầng 2 – cửa sổ bất thường.** Mỗi phút được biểu diễn bằng vector 156 chiều (7 thống kê chung + tần suất từng mẫu known, biến đổi `log1p`). Isolation Forest (200 cây) huấn luyện trên các phút bình thường:

  ```
  score(x) = 2 ^ ( − h(x) / c(n) ),   c(n) = 2·H(n−1) − 2(n−1)/n
  ```

  Ngưỡng τ = phân vị 0,95 điểm của phút bình thường (BGL: τ = 0,4277).
- **Kết hợp.** `reason` ∈ {`normal`, `unknown-template`, `anomalous`, `both`}.

## Kết quả sơ bộ (BGL)

| Hạng mục | Kết quả |
|---|---|
| Grouping Accuracy (Loghub-2k) | BGL 0,970 · HDFS 0,998 · OpenStack 0,849 |
| Warm-up | 949.592 dòng → 150 mẫu known, 42 giây |
| Mẫu chèn | 50/50 dòng gắn unknown, 5/5 mẫu mới được ghi nhận trong một lô |
| Cửa sổ có nhãn alert | 14/14 được báo (recall 1,0) |
| Throughput job 1 | ~4.700 dòng/giây, độ trễ p95 ~10 giây (trigger 10 giây) |

## Yêu cầu

- Linux với Docker Engine ≥ 24 và Docker Compose v2 (không cần Docker Desktop)
- 8 GB RAM trống, 4 lõi CPU, ~5 GB đĩa
- Python ≥ 3.10 trên máy chủ (cho replayer, profiling, panel)

## Bắt đầu nhanh

```bash
git clone <url-repo> log-pattern-pipeline && cd log-pattern-pipeline

# Python trên máy chủ
python3 -m venv .venv && .venv/bin/pip install -r requirements-host.txt

./pipeline.sh up                                          # Kafka, Redis, PostgreSQL, Spark (1 master + 2 worker), Grafana, Kafka UI
./pipeline.sh check                                       # kiểm tra hạ tầng
./pipeline.sh download 2k && ./pipeline.sh download BGL   # tải dữ liệu Loghub
./pipeline.sh warmup --dataset BGL --fraction 0.2         # học mẫu known
./pipeline.sh train --dataset BGL --key ALL --window 60   # huấn luyện Isolation Forest
./pipeline.sh job1 && ./pipeline.sh job2                  # chạy 2 Spark job
./pipeline.sh replay --dataset BGL --skip 949592 --rate 2000 \
    --inject replayer/inject_templates.txt --at 0.5 --inject-count 50
```

Không muốn gõ lệnh: `./pipeline.sh panel` rồi mở http://localhost:8501 (bảng điều khiển Streamlit, có nút "Bắt đầu demo").

> Đường dẫn chứa ký tự xuống dòng hoặc ký tự đặc biệt sẽ khiến Docker Compose báo lỗi. Hãy clone vào thư mục có tên đơn giản.

## Giao diện

| Giao diện | Địa chỉ |
|---|---|
| Bảng điều khiển demo | http://localhost:8501 |
| Kafka UI | http://localhost:8090 |
| Spark master | http://localhost:8080 |
| Spark job 1 / job 2 | http://localhost:4040 / http://localhost:4041 |
| Grafana | http://localhost:3000 (admin/admin) |
| PostgreSQL | `localhost:5432`, user `app`, password `app`, db `logs` |

## Cấu trúc mã nguồn

```
├── docker-compose.yml, pipeline.sh, Makefile, requirements-host.txt
├── docker/        Dockerfile.spark, create-topics.sh
├── sql/           init.sql
├── schemas/       log_event.json          lược đồ chung
├── data/          download.sh             (data/raw/ không đưa lên git)
├── replayer/      replay.py, inject_templates.txt
├── parsers/       base.py                 parser BGL, HDFS, OpenStack
├── profiling/     profile.py
├── registry/      drain3.ini, common.py, warmup.py, online.py
├── jobs/          job1_parse.py, job2_detect.py
├── features/      windowing.py
├── models/        train_iforest.py
├── eval/          parsing_accuracy.py
├── panel/         app.py                  bảng điều khiển Streamlit
├── grafana/       provisioning/
└── docs/          code_workflow.html      tài liệu kỹ thuật
```

## Lộ trình

- [x] Bước 1 – Hạ tầng Docker
- [x] Bước 2 – Dữ liệu và replayer
- [x] Bước 3 – Warm-up, Spark job 1 (Drain3, known/unknown)
- [x] Bước 4 – Isolation Forest, Spark job 2
- [ ] Bước 5 – Dashboard Grafana, collector metric
- [ ] Bước 6 – Đánh giá: P/R/F1 ba cấu hình, novelty test, benchmark scale-out

## Tài liệu tham khảo

1. P. He et al. *Drain: An Online Log Parsing Approach with Fixed Depth Tree.* ICWS 2017.
2. J. Zhu et al. *Tools and Benchmarks for Automated Log Parsing.* ICSE-SEIP 2019.
3. J. Zhu et al. *Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics.* ISSRE 2023. https://github.com/logpai/loghub
4. F. T. Liu, K. M. Ting, Z.-H. Zhou. *Isolation Forest.* ICDM 2008.
5. Drain3: https://github.com/logpai/Drain3
