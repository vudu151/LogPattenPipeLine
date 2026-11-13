#!/usr/bin/env bash
# Mọi lệnh của dự án ở một chỗ. Dùng: ./pipeline.sh <lệnh>   (xem danh sách: ./pipeline.sh help)
set -e
cd "$(dirname "$0")"
KT="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
PSQL="docker compose exec -T postgres psql -U app -d logs"

case "${1:-help}" in
  build)  docker compose build spark-master ;;
  up)     mkdir -p lake/logs lake/warmup results models && chmod 777 lake lake/logs lake/warmup results models 2>/dev/null || true
          docker compose up -d --build ;;
  lake)   docker compose --profile lake up -d ;;
  down)   docker compose down ;;
  clean)  docker compose down -v --remove-orphans ;;
  ps)     docker compose ps ;;
  logs)   docker compose logs -f --tail=50 "${@:2}" ;;
  topics) $KT --list ;;
  psql)   docker compose exec postgres psql -U app -d logs ;;
  download) .venv/bin/python tools/download_datasets.py "${2:-2k}" ;;
  profile)  .venv/bin/python profiling/profile.py "${@:2}" ;;
  produce|replay) .venv/bin/python log_producer/producer.py "${@:2}" ;;   # Log Producer (replay = ten cu)
  consume)  # xem N message dau cua topic: ./pipeline.sh consume raw-logs.bgl 5
            docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
              --topic "${2:-raw-logs.bgl}" --from-beginning --max-messages "${3:-5}" --timeout-ms 10000 ;;
  count)    # dem message trong topic: ./pipeline.sh count raw-logs.bgl
            docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
              --topic "${2:-raw-logs.bgl}" | awk -F: '{s+=$3} END {print "total messages:", s+0}' ;;
  warmup)   # hoc template known: ./pipeline.sh warmup --dataset BGL --fraction 0.2
            docker compose exec -T -e PYTHONPATH=/app spark-master python3 /app/registry/warmup.py "${@:2}" ;;
  eval-parsing)  for ds in ${2:-BGL HDFS OpenStack}; do
              docker compose exec -T -e PYTHONPATH=/app spark-master python3 /app/eval/parsing_accuracy.py --dataset "$ds"; echo; done ;;
  job1)     # chay Spark job 1 o nen; log tai lake/logs/job1.log
            docker compose exec -d spark-master bash -c "cd /app && /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 --total-executor-cores 2 --executor-memory 900m \
              --conf spark.driver.host=spark-master --conf spark.executorEnv.PYTHONPATH=/app \
              /app/jobs/job1_parse.py ${*:2} > /app/lake/logs/job1.log 2>&1"
            echo "job1 da khoi dong. Xem log: ./pipeline.sh job1-log | Spark UI: http://localhost:4040" ;;
  job1-log) tail -f -n 30 lake/logs/job1.log | grep --line-buffered -E '\[job1\]|ERROR|Exception|Error' ;;
  job1-stop) docker compose exec -T spark-master pkill -f job1_parse.py && echo "da dung job1" || echo "job1 khong chay" ;;
  job1-reset) # xoa checkpoint de doc lai tu dau
            docker compose exec -T spark-master rm -rf /app/lake/checkpoints/job1 && echo "da xoa checkpoint job1" ;;
  train)    # train Isolation Forest tren window warm-up: ./pipeline.sh train --dataset BGL [--window 60 --key ALL]
            docker compose exec -T -e PYTHONPATH=/app spark-master python3 /app/models/train_iforest.py "${@:2}" ;;
  job2)     docker compose exec -d spark-master bash -c "cd /app && /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 --total-executor-cores 2 --executor-memory 900m \
              --conf spark.driver.host=spark-master --conf spark.executorEnv.PYTHONPATH=/app \
              /app/jobs/job2_detect.py ${*:2} > /app/lake/logs/job2.log 2>&1"
            echo "job2 da khoi dong. Xem log: ./pipeline.sh job2-log | Spark UI: http://localhost:4041" ;;
  job2-log) tail -f -n 30 lake/logs/job2.log | grep --line-buffered -E '\[job2\]|ERROR|Exception|Error' ;;
  job2-stop) docker compose exec -T spark-master pkill -f job2_detect.py && echo "da dung job2" || echo "job2 khong chay" ;;
  job2-reset) docker compose exec -T spark-master rm -rf /app/lake/checkpoints/job2 && echo "da xoa checkpoint job2" ;;
  panel)    # bang dieu khien web: http://localhost:8501
            PIPELINE_DIR="$(pwd)" .venv/bin/streamlit run panel/app.py --server.port 8501 --server.headless true --browser.gatherUsageStats false "${@:2}" ;;
  quality)  # ho so chat luong du lieu: ./pipeline.sh quality --dataset BGL
            .venv/bin/python profiling/quality.py "${@:2}" ;;
  eval-detect) # danh gia phat hien ngoai tuyen: ./pipeline.sh eval-detect --dataset BGL --lines 1000000
            docker compose exec -T -e PYTHONPATH=/app spark-master python3 /app/eval/detection_metrics.py "${@:2}" ;;
  alerts)   # Alert System: doc topic alerts, khu lap, in ra man hinh / ghi tep / webhook
            .venv/bin/python alerting/alert_service.py "${@:2}" ;;
  ch)       # truy van ClickHouse: ./pipeline.sh ch "SELECT count() FROM logs.log_events"
            docker compose exec -T clickhouse clickhouse-client --user app --password app -q "${2:-SELECT source, count() FROM logs.log_events GROUP BY source}" ;;
  gav)      .venv/bin/python lib_log_handler/gav.py ;;      # in dinh nghia khung nhin GAV
  lav)      .venv/bin/python -m lib_log_handler.lav 2>/dev/null ;;   # mo ta nguon LAV va vi du chon nguon
  check)
    echo "== containers";      docker compose ps --format 'table {{.Name}}\t{{.Status}}'
    echo "== kafka topics";    $KT --list
    echo "== clickhouse";      docker compose exec -T clickhouse clickhouse-client --user app --password app -q "SHOW TABLES FROM logs" | tr '\n' ' '; echo
    echo "== postgres tables"; $PSQL -c '\dt'
    echo "== redis";           docker compose exec -T redis redis-cli ping
    echo "== spark workers";   curl -s http://localhost:8080/json/ | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d["workers"]), "worker(s):", [(w["host"], w["state"]) for w in d["workers"]])'
    echo "== kafka-ui";        curl -s -o /dev/null -w 'HTTP %{http_code}\n' http://localhost:8090/actuator/health
    echo "== grafana";         curl -s -o /dev/null -w 'HTTP %{http_code}\n' http://localhost:3000/api/health
    ;;
  *) cat <<'HELP'
Dùng: ./pipeline.sh <lệnh> [tham số]

Hạ tầng
  up | down | clean          dựng / dừng / xóa cả dữ liệu của các dịch vụ Docker
  check | ps | logs [dv]     kiểm tra dịch vụ, xem trạng thái, xem log
  topics | psql | ch "SQL"   liệt kê topic Kafka, mở PostgreSQL, truy vấn ClickHouse

Dữ liệu
  download [2k|BGL|HDFS|OpenStack|all]   tải dữ liệu Loghub
  profile --dataset X                    hồ sơ nguồn
  quality --dataset X                    hồ sơ chất lượng theo từng luật
  gav | lav                              in khung nhìn GAV / mô tả nguồn LAV

Chuẩn bị (làm một lần)
  warmup --dataset X --fraction 0.2      học mẫu known
  train --dataset X --key ALL            huấn luyện Isolation Forest

Chạy luồng
  job1 | job2 [--from-beginning]         bật Spark job 1 / job 2 ở nền
  job1-log | job1-stop | job1-reset      xem log / dừng / xóa checkpoint (tương tự job2-*)
  alerts [--webhook URL]                 bật Alert System
  produce --dataset X --rate N ...       Log Producer phát log lên Kafka
  consume [topic] [n] | count [topic]    xem n message đầu / đếm message của topic
  panel                                  bảng điều khiển web: http://localhost:8501

Đánh giá
  eval-parsing [X]                       độ chính xác trích mẫu trên Loghub-2k
  eval-detect --dataset X [--lines N]    precision / recall phát hiện
HELP
    ;;
esac
