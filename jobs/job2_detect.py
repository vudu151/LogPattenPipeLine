"""Spark job 2 (Stream Processor, phan vector dac trung + mo hinh ML):
parsed-logs -> cua so 1 phut (watermark 2 phut) -> vector dac trung -> Isolation Forest -> window_alerts + topic alerts.

Chay (tu may host):  ./pipeline.sh job2          Log: ./pipeline.sh job2-log     UI: http://localhost:4041

Moi nguon mot truy van luong rieng: job2_windows_<SRC>.
  Watermark cua Structured Streaming la toan cuc trong mot truy van. Ba nguon thuoc ba thoi ky khac nhau
  (BGL 2005, HDFS 2008, OpenStack 2017): neu gom chung, su kien cua nguon moi nhat day watermark len va moi su kien
  cua hai nguon con lai bi coi la den tre roi bi loai. Vi vay moi nguon co truy van va watermark rieng.
foreachBatch: features/windowing.py -> mo hinh joblib -> score, reason -> upsert PostgreSQL window_alerts,
              day cua so bat thuong vao topic `alerts` (Alert System va ClickHouse cung doc), ghi pipeline_metrics.
Reason: unknown-template (co dong status=unknown) | anomalous (score >= nguong) | both | normal.
Log da chuan hoa KHONG ghi o day: Storage Writer cua ClickHouse doc thang topic parsed-logs.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/app")
import joblib  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402
from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

from features.windowing import featurize, window_stats  # noqa: E402

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
PG = dict(host="postgres", port=5432, user="app", password="app", dbname="logs")
IN_SCHEMA = T.StructType([
    T.StructField("event_time", T.TimestampType()), T.StructField("ingest_time", T.TimestampType()),
    T.StructField("source", T.StringType()), T.StructField("host", T.StringType()),
    T.StructField("level", T.StringType()), T.StructField("label", T.StringType()),
    T.StructField("template_id", T.StringType()), T.StructField("status", T.StringType()),
])


def load_models(sources):
    models = {}
    for src in sources:
        p = "/app/models/iforest_%s.joblib" % src
        if os.path.exists(p):
            models[src] = joblib.load(p)
            m = models[src]
            print("[job2] model %s: window=%ds key=%s threshold=%.4f features=%d" % (
                src, m["window_s"], m["key"], m["threshold"], m["n_features"]), flush=True)
    if not models:
        sys.exit("khong co model nao trong /app/models (chay ./pipeline.sh train truoc)")
    return models


def write_metrics(stage, n, dur_s, trigger_ms):
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO pipeline_metrics (ts, stage, events_per_sec, batch_duration_ms, trigger_interval_ms)
                       VALUES (now(), %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    (stage, n / max(dur_s, 1e-3), dur_s * 1000, trigger_ms))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--watermark", default="2 minutes", help="do tre toi da cua log (event time) duoc chap nhan")
    ap.add_argument("--sources", default="BGL,HDFS,OpenStack")
    ap.add_argument("--checkpoint", default="/app/lake/checkpoints/job2")
    args = ap.parse_args()
    trigger_ms = float(args.trigger.split()[0]) * 1000
    models = load_models(args.sources.split(","))
    window_s = {m["window_s"] for m in models.values()}
    if len(window_s) != 1:
        sys.exit("cac model phai cung do dai window")
    window_s = window_s.pop()

    spark = (SparkSession.builder.appName("job2_detect").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4").config("spark.ui.port", "4041").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def window_stream(src):
        ev = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", "parsed-logs")
              .option("startingOffsets", "earliest" if args.from_beginning else "latest")
              .option("maxOffsetsPerTrigger", 100000).option("failOnDataLoss", "false").load()
              .select(F.from_json(F.col("value").cast("string"), IN_SCHEMA).alias("e")).select("e.*")
              .filter((F.col("source") == src) & F.col("event_time").isNotNull()))
        key_col = F.col("host") if models[src]["key"] == "host" else F.lit("ALL")
        return (ev.withColumn("key", key_col).withWatermark("event_time", args.watermark)
                .groupBy(F.window("event_time", "%d seconds" % window_s).alias("w"), "source", "key")
                .agg(F.collect_list(F.struct("template_id", "status", "level", "label")).alias("items")))

    def window_batch(df, batch_id):
        t0 = time.time()
        rows = df.select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "source", "key", "items").collect()
        if not rows:
            return
        out = []
        for r in rows:
            items = r["items"]
            st = window_stats([i["template_id"] for i in items], [i["status"] for i in items],
                              [i["level"] for i in items], [i["label"] for i in items])
            st.update({"window_start": r.window_start, "window_end": r.window_end, "source": r.source, "host": r.key})
            out.append(st)
        W = pd.DataFrame(out)
        src = W["source"].iloc[0]; m = models[src]
        W["score"] = -m["model"].score_samples(featurize(W, m["template_ids"]))
        W["is_if"] = W["score"] >= m["threshold"]; W["is_unk"] = W["n_unknown"] > 0
        W["reason"] = np.select([W.is_if & W.is_unk, W.is_unk, W.is_if], ["both", "unknown-template", "anomalous"], "normal")
        W["is_anomaly"] = W["reason"] != "normal"
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO window_alerts (window_start, window_end, source, host, n_events, n_unknown, n_error, entropy,
                    score, is_anomaly, reason, top_templates, label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                ON CONFLICT (window_start, source, host) DO UPDATE SET window_end=EXCLUDED.window_end, n_events=EXCLUDED.n_events,
                    n_unknown=EXCLUDED.n_unknown, n_error=EXCLUDED.n_error, entropy=EXCLUDED.entropy, score=EXCLUDED.score,
                    is_anomaly=EXCLUDED.is_anomaly, reason=EXCLUDED.reason, top_templates=EXCLUDED.top_templates,
                    label=EXCLUDED.label, detected_at=now()""",
                [(w.window_start, w.window_end, w.source, w.host, int(w.n_events), int(w.n_unknown), int(w.n_error),
                  float(w.entropy), float(w.score), bool(w.is_anomaly), w.reason,
                  '{"top": "%s"}' % w.top_templates.replace('"', ""), w.label) for w in W.itertuples()])
        # ---- canh bao: moi cua so bat thuong la mot message tren topic alerts ----
        an = W[W.is_anomaly]
        if len(an):
            now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            msgs = []
            for w in an.itertuples():
                ws = pd.Timestamp(w.window_start).strftime("%Y-%m-%dT%H:%M:%SZ")
                msgs.append((w.source, json.dumps({
                    "alert_id": "win-%s-%s" % (w.source, ws), "type": "window-anomaly",
                    "level": "HIGH" if w.reason in ("unknown-template", "both") else "MEDIUM",
                    "source": w.source, "event_time": ws, "detected_at": now,
                    "title": "%s: cua so %s bat thuong (%s, score %.3f)" % (w.source, ws, w.reason, w.score),
                    "detail": json.dumps({"reason": w.reason, "score": round(float(w.score), 4), "threshold": round(m["threshold"], 4),
                                          "n_events": int(w.n_events), "n_unknown": int(w.n_unknown), "n_error": int(w.n_error),
                                          "top_templates": w.top_templates}, ensure_ascii=False)}, ensure_ascii=False)))
            (spark.createDataFrame(msgs, "key string, value string").write.format("kafka")
             .option("kafka.bootstrap.servers", KAFKA).option("topic", "alerts").save())
        dur = time.time() - t0
        write_metrics("job2_detect", int(W.n_events.sum()), dur, trigger_ms)
        y = (W.label == "alert"); pred = W.is_anomaly
        print("[job2] %s batch=%d windows=%d normal=%d unknown-template=%d anomalous=%d both=%d alerts=%d | label alert=%d tp=%d fp=%d fn=%d | dur=%.1fs" % (
            src, batch_id, len(W), int((W.reason == "normal").sum()), int((W.reason == "unknown-template").sum()),
            int((W.reason == "anomalous").sum()), int((W.reason == "both").sum()), len(an), int(y.sum()),
            int((y & pred).sum()), int((~y & pred).sum()), int((y & ~pred).sum()), dur), flush=True)

    queries = [window_stream(src).writeStream.outputMode("append").foreachBatch(window_batch)
               .trigger(processingTime=args.trigger).option("checkpointLocation", args.checkpoint + "/windows_" + src)
               .queryName("job2_windows_" + src).start() for src in models]
    print("[job2] dang chay %d query. Spark UI: http://localhost:4041" % len(queries), flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
