# Lệnh tắt cho pipeline. Dùng: make <target>
COMPOSE = docker compose
PSQL    = $(COMPOSE) exec -T postgres psql -U app -d logs
KT      = $(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092

.PHONY: build up down clean ps logs topics psql check lake download profile replay consume count

build:            ## build image Spark (lần đầu mất vài phút)
	$(COMPOSE) build spark-master

up:               ## dựng toàn bộ hạ tầng
	$(COMPOSE) up -d --build

lake:             ## dựng thêm MinIO (data lake)
	$(COMPOSE) --profile lake up -d

down:             ## dừng, giữ dữ liệu
	$(COMPOSE) down

clean:            ## dừng và xóa toàn bộ volume (Kafka, DB, Redis, Grafana)
	$(COMPOSE) down -v --remove-orphans

ps:
	$(COMPOSE) ps

logs:             ## theo dõi log tất cả dịch vụ
	$(COMPOSE) logs -f --tail=50

topics:           ## liệt kê topic Kafka
	$(KT) --list

psql:             ## mở psql vào DB logs
	$(COMPOSE) exec postgres psql -U app -d logs

check:            ## kiểm tra nhanh bước 1: container, topic, bảng, Redis
	@echo "== containers"; $(COMPOSE) ps --format 'table {{.Name}}\t{{.Status}}'
	@echo "== kafka topics"; $(KT) --list
	@echo "== postgres tables"; $(PSQL) -c '\dt'
	@echo "== redis"; $(COMPOSE) exec -T redis redis-cli ping
	@echo "== spark workers"; curl -s http://localhost:8080/json/ | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d["workers"]), "worker(s) ALIVE:", [w["host"] for w in d["workers"]])'

download:         ## tải dataset: make download DS=BGL (mặc định 2k)
	bash data/download.sh $(or $(DS),2k)

profile:          ## thống kê dataset: make profile ARGS="--dataset BGL --sample"
	.venv/bin/python profiling/profile.py $(ARGS)

replay:           ## phát log lên Kafka: make replay ARGS="--dataset BGL --speed 100"
	.venv/bin/python replayer/replay.py $(ARGS)

consume:          ## xem message đầu topic: make consume TOPIC=raw-logs.bgl N=5
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $(or $(TOPIC),raw-logs.bgl) --from-beginning --max-messages $(or $(N),5) --timeout-ms 10000

count:            ## đếm message trong topic: make count TOPIC=raw-logs.bgl
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $(or $(TOPIC),raw-logs.bgl) | awk -F: '{s+=$$3} END {print "total messages:", s+0}'
