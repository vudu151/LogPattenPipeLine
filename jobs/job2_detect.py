"""Spark job 2: parsed-logs -> log_history + window 1 phut (watermark 2 phut) -> Isolation Forest -> window_alerts.

Chay (tu may host):  ./pipeline.sh job2          Log: ./pipeline.sh job2-log     UI: http://localhost:4041

Hai streaming query trong mot ung dung:
  job2_history : moi batch COPY cac dong da parse vao log_history (anomaly_score de trong)
  job2_windows : groupBy(window(event_time, 1 phut), source, key) voi watermark 2 phut (log den tre),
                 append mode -> window chi duoc phat ra khi watermark vuot qua window_end.
                 foreachBatch: features/windowing.py -> model joblib -> score, reason -> upsert window_alerts,
                 cap nhat anomaly_score cho dong log trong window bat thuong, ghi pipeline_metrics.
Reason: unknown-template (co dong status=unknown) | anomalous (score >= nguong) | both | normal.
"""
import argparse
import io
import os
import sys
import time

sys.path.insert(0, "/app")
import joblib  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402
from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

from features.windowing import BASE_COLS, featurize, window_stats  # noqa: E402

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
PG = dict(host="postgres", port=5432, user="app", password="app", dbname="logs")
IN_SCHEMA = T.StructType([
    T.StructField("event_time", T.TimestampType()), T.StructField("ingest_time", T.TimestampType()),
    T.StructField("processed_time", T.TimestampType()), T.StructField("source", T.StringType()),
    T.StructField("host", T.StringType()), T.StructField("level", T.StringType()),
    T.StructField("component", T.StringType()), T.StructField("content", T.StringType()),
    T.StructField("raw", T.StringType()), T.StructField("label", T.StringType()),
    T.StructField("template_id", T.StringType()), T.StructField("template", T.StringType()),
    T.StructField("status", T.StringType()), T.StructField("injected", T.BooleanType()),
    T.StructField("seq", T.LongType()),
])
HIST_COLS = ["event_time", "ingest_time", "processed_time", "source", "host", "level", "component", "content",
             "template_id", "status", "label", "raw"]


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


BACKFILL_SQL = """UPDATE log_history h SET anomaly_score = w.score FROM window_alerts w
    WHERE h.anomaly_score IS NULL AND w.is_anomaly AND h.source = w.source AND (w.host = 'ALL' OR h.host = w.host)
      AND h.event_time >= w.window_start AND h.event_time < w.window_end AND h.id > %s"""


def copy_history(pdf):
    """COPY vao log_history, roi gan anomaly_score cho dong nao roi vao window bat thuong DA co (xu ly race voi query 2)."""
    buf = io.StringIO()
    pdf[HIST_COLS].to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N", date_format="%Y-%m-%d %H:%M:%S.%f%z")
    buf.seek(0)
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(id), 0) FROM log_history"); max_id = cur.fetchone()[0]
        cur.copy_from(buf, "log_history", sep="\t", null="\\N", columns=HIST_COLS)
        cur.execute(BACKFILL_SQL, (max_id,))
        return cur.rowcount


def write_metrics(stage, n, dur_s, trigger_ms, p50=None, p95=None):
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO pipeline_metrics (ts, stage, events_per_sec, batch_duration_ms, trigger_interval_ms,
                       latency_p50_ms, latency_p95_ms) VALUES (now(), %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                    (stage, n / max(dur_s, 1e-3), dur_s * 1000, trigger_ms, p50, p95))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--watermark", default="2 minutes", help="do tre toi da cua log (event time) duoc chap nhan")
    ap.add_argument("--sources", default="BGL,HDFS,OpenStack")
    ap.add_argument("--checkpoint", default="/app/lake/checkpoints/job2")
    ap.add_argument("--no-history", action="store_true", help="khong ghi log_history (chi window)")
    args = ap.parse_args()
    trigger_ms = float(args.trigger.split()[0]) * 1000
    models = load_models(args.sources.split(","))
    window_s = {m["window_s"] for m in models.values()}
    if len(window_s) != 1:
        sys.exit("cac model phai cung do dai window")
    window_s = window_s.pop()
    key_expr = {src: m["key"] for src, m in models.items()}

    spark = (SparkSession.builder.appName("job2_detect").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4").config("spark.ui.port", "4041").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def source_stream():
        return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", "parsed-logs")
                .option("startingOffsets", "earliest" if args.from_beginning else "latest")
                .option("maxOffsetsPerTrigger", 100000).option("failOnDataLoss", "false").load()
                .select(F.from_json(F.col("value").cast("string"), IN_SCHEMA).alias("e")).select("e.*"))

    # ---- query 1: log_history ----
    def hist_batch(df, batch_id):
        t0 = time.time(); pdf = df.toPandas()
        if len(pdf) == 0:
            return
        for c in ("event_time", "ingest_time", "processed_time"):
            pdf[c] = pd.to_datetime(pdf[c], utc=True)
        n_scored = copy_history(pdf)
        write_metrics("job2_history", len(pdf), time.time() - t0, trigger_ms)
        print("[job2] history batch=%d rows=%d scored_backfill=%d dur=%.1fs" % (batch_id, len(pdf), n_scored, time.time() - t0), flush=True)

    queries = []
    if not args.no_history:
        queries.append(source_stream().writeStream.foreachBatch(hist_batch).trigger(processingTime=args.trigger)
                       .option("checkpointLocation", args.checkpoint + "/history").queryName("job2_history").start())

    # ---- query 2: windows ----
    ev = source_stream().filter(F.col("event_time").isNotNull())
    key_col = F.when(F.col("source").isin([s for s, k in key_expr.items() if k == "host"]), F.col("host")).otherwise(F.lit("ALL"))
    agg = (ev.withColumn("key", key_col).withWatermark("event_time", args.watermark)
           .groupBy(F.window("event_time", "%d seconds" % window_s).alias("w"), "source", "key")
           .agg(F.collect_list(F.struct("template_id", "status", "level", "label")).alias("items"),
                F.max("ingest_time").alias("last_ingest")))

    def window_batch(df, batch_id):
        t0 = time.time()
        rows = df.select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "source", "key", "items", "last_ingest").collect()
        if not rows:
            return
        out = []
        for r in rows:
            m = models.get(r.source)
            if m is None:
                continue
            items = r["items"]
            st = window_stats([i["template_id"] for i in items], [i["status"] for i in items],
                              [i["level"] for i in items], [i["label"] for i in items])
            st.update({"window_start": r.window_start, "window_end": r.window_end, "source": r.source, "host": r.key})
            out.append(st)
        W = pd.DataFrame(out)
        scores = np.zeros(len(W))
        for src, m in models.items():
            mask = (W["source"] == src).values
            if mask.any():
                scores[mask] = -m["model"].score_samples(featurize(W[mask], m["template_ids"]))
        W["score"] = scores
        W["th"] = W["source"].map({s: m["threshold"] for s, m in models.items()})
        W["is_if"] = W["score"] >= W["th"]; W["is_unk"] = W["n_unknown"] > 0
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
            an = W[W.is_anomaly]
            if len(an) and not args.no_history:
                cur.executemany("""UPDATE log_history SET anomaly_score=%s WHERE source=%s AND (%s='ALL' OR host=%s)
                                   AND event_time >= %s AND event_time < %s""",
                                [(float(w.score), w.source, w.host, w.host, w.window_start, w.window_end) for w in an.itertuples()])
        dur = time.time() - t0
        write_metrics("job2_detect", int(W.n_events.sum()), dur, trigger_ms)
        y = (W.label == "alert"); pred = W.is_anomaly
        tp = int((y & pred).sum()); fp = int((~y & pred).sum()); fn = int((y & ~pred).sum())
        print("[job2] windows batch=%d n=%d normal=%d unknown-template=%d anomalous=%d both=%d | label alert=%d tp=%d fp=%d fn=%d | dur=%.1fs" % (
            batch_id, len(W), int((W.reason == "normal").sum()), int((W.reason == "unknown-template").sum()),
            int((W.reason == "anomalous").sum()), int((W.reason == "both").sum()), int(y.sum()), tp, fp, fn, dur), flush=True)

    queries.append(agg.writeStream.outputMode("append").foreachBatch(window_batch).trigger(processingTime=args.trigger)
                   .option("checkpointLocation", args.checkpoint + "/windows").queryName("job2_windows").start())
    print("[job2] dang chay %d query. Spark UI: http://localhost:4041" % len(queries), flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
