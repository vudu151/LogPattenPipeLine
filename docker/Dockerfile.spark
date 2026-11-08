# Spark 3.5.3 + thư viện Python cần cho job 1 (Drain3, Redis) và job 2 (scikit-learn, PostgreSQL).
FROM apache/spark:3.5.3
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir \
      drain3==0.9.11 \
      redis==5.2.1 \
      psycopg2-binary==2.9.10 \
      pandas==2.0.3 \
      pyarrow==17.0.0 \
      scikit-learn==1.3.2 \
      joblib==1.4.2
# Lưu ý: image apache/spark:3.5.3 dùng Python 3.8, nên pin các bản còn hỗ trợ 3.8.
# Model Isolation Forest phải được train bên trong container này để cùng bản scikit-learn.
# Connector Kafka cho Structured Streaming, để spark-submit không phải tải lúc chạy.
RUN cd /opt/spark/jars \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
WORKDIR /app
USER spark
