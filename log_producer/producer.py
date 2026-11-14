#!/usr/bin/env python3
"""Log Producer (buoc EXTRACT): doc raw log cua ba he dieu hanh (tep Loghub) va ban len Message Queue (Kafka).

Phat theo dung khoang cach thoi gian goc (nhan speed factor) de mo phong log phat sinh lien tuc.

Vi du:
  python log_producer/producer.py --dataset Linux --speed 100
  python log_producer/producer.py --dataset Linux --sample --speed 50     # file 2k dong de test nhanh
  python log_producer/producer.py --dataset Linux --rate 2000 --limit 200000
  python log_producer/producer.py --dataset Linux --speed 100 --inject log_producer/inject_templates.txt --at 0.5
  python log_producer/producer.py --dataset Linux --speed 100 --late-fraction 0.05 --late-max 20   # log den tre

Message gui len topic raw-logs.<dataset> (key = host):
  {"source": "Linux", "host": "...", "ts": "<event_time ISO UTC>", "ingest_time": "<luc gui>", "seq": 123, "raw": "<dong goc>"}
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lib_log_handler import DEFAULT_FILES, SAMPLE_FILES, SOURCES, format_raw, parse  # noqa: E402

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def load_inject(path):
    lines = []
    with open(path, encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln and not ln.startswith("#"):
                lines.append(ln)
    return lines


class Stats:
    def __init__(self):
        self.sent = 0; self.errors = 0; self.parse_fail = 0; self.injected = 0; self.late = 0
        self.t0 = time.monotonic(); self.last_t = self.t0; self.last_sent = 0
        self.max_behind = 0.0

    def line(self, behind):
        t = time.monotonic()
        rate = (self.sent - self.last_sent) / max(t - self.last_t, 1e-6)
        self.last_t, self.last_sent = t, self.sent
        self.max_behind = max(self.max_behind, behind)
        return ("[producer] sent={:,} rate={:,.0f} ev/s elapsed={:.0f}s behind_schedule={:.1f}s "
                "parse_fail={} injected={} late={} deliver_err={}").format(
            self.sent, rate, t - self.t0, behind, self.parse_fail, self.injected, self.late, self.errors)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", required=True, choices=SOURCES)
    ap.add_argument("--file", help="duong dan file log; mac dinh data/raw/<DATASET>/...")
    ap.add_argument("--sample", action="store_true", help="dung file 2k dong (test nhanh)")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    ap.add_argument("--topic", help="mac dinh raw-logs.<dataset lower>")
    ap.add_argument("--speed", type=float, default=100.0, help="he so tang toc so voi thoi gian goc (mac dinh 100)")
    ap.add_argument("--rate", type=float, help="bo qua timestamp, phat deu N event/giay")
    ap.add_argument("--max-gap", type=float, default=5.0, help="khoang nghi thuc te toi da (giay) khi log goc co lo hong lon")
    ap.add_argument("--limit", type=int, help="chi phat N dong dau")
    ap.add_argument("--skip", type=int, default=0, help="bo qua N dong dau")
    ap.add_argument("--inject", help="file content la de tiem vao stream")
    ap.add_argument("--at", type=float, default=0.5, help="tiem khi tien do dat ti le nay (0..1), can --limit hoac --sample; neu khong co thi tiem sau --at*100000 dong")
    ap.add_argument("--inject-count", type=int, default=50, help="so dong tiem (lap lai cac content trong file)")
    ap.add_argument("--late-fraction", type=float, default=0.0, help="ti le message bi giu lai gui tre (mo phong late-arriving)")
    ap.add_argument("--late-max", type=float, default=10.0, help="do tre toi da (giay thuc) cua message bi giu")
    ap.add_argument("--stats-every", type=float, default=5.0)
    ap.add_argument("--dry-run", action="store_true", help="khong gui Kafka, chi in ra stdout")
    args = ap.parse_args()

    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw")
    path = args.file or os.path.join(root, (SAMPLE_FILES if args.sample else DEFAULT_FILES)[args.dataset])
    if not os.path.exists(path):
        sys.exit("khong thay file {} (chay: ./pipeline.sh download {})".format(path, "2k" if args.sample else args.dataset))
    topic = args.topic or "raw-logs." + args.dataset.lower()
    total_lines = args.limit
    if total_lines is None and args.sample:
        total_lines = 2000
    inject_lines = load_inject(args.inject) if args.inject else []
    inject_after = int(args.at * (total_lines if total_lines else 100000))
    random.seed(42)

    producer = None
    if not args.dry_run:
        if Producer is None:
            sys.exit("thieu confluent-kafka: .venv/bin/pip install -r requirements-host.txt")
        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 20, "batch.num.messages": 10000, "compression.type": "lz4",
            "acks": 1, "queue.buffering.max.messages": 500000,
        })
    st = Stats()

    def on_delivery(err, msg):
        if err is not None:
            st.errors += 1

    def send(msg):
        payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        if producer is None:
            print(payload.decode("utf-8"))
        else:
            while True:
                try:
                    producer.produce(topic, key=(msg["host"] or "").encode("utf-8"), value=payload, callback=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.05)
            producer.poll(0)
        st.sent += 1

    pending_late = []          # [(release_at_monotonic, msg)]
    def flush_late(force=False):
        t = time.monotonic()
        keep = []
        for rel, m in pending_late:
            if force or rel <= t:
                m["ingest_time"] = now_iso(); send(m)
            else:
                keep.append((rel, m))
        pending_late[:] = keep

    print("[producer] file={} topic={} speed={}x rate={} limit={} inject={} bootstrap={}".format(
        path, topic, args.speed, args.rate, total_lines, len(inject_lines), args.bootstrap), flush=True)

    wall0 = None; ev0 = None; last_ev = None; seq = 0; n_orig = 0; next_stats = time.monotonic() + args.stats_every
    injected_done = False
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i < args.skip:
                continue
            if total_lines is not None and n_orig >= total_lines:   # --limit dem dong goc, khong dem dong tiem
                break
            line = line.rstrip("\n")
            if not line:
                continue
            rec = parse(args.dataset, line)
            if rec is None or not rec["event_time"]:    # dong khong tach duoc: van gui di, job 1 se cach ly
                st.parse_fail += 1; rec = None
                ev = last_ev; host = "unparsed"
            else:
                ev = parse_iso(rec["event_time"]); host = rec["host"]
            if ev is None:
                continue
            last_ev = ev

            # --- lich phat ---
            if args.rate:
                target = (wall0 or time.monotonic()) + seq / args.rate
                if wall0 is None: wall0 = time.monotonic()
            else:
                if wall0 is None:
                    wall0, ev0 = time.monotonic(), ev
                target = wall0 + (ev - ev0).total_seconds() / args.speed
            delay = target - time.monotonic()
            if delay > args.max_gap:          # lo hong lon trong log goc: khong cho qua max-gap giay
                wall0 -= (delay - args.max_gap); delay = args.max_gap
            if delay > 0:
                time.sleep(delay)
            behind = max(0.0, -delay)

            msg = {"source": args.dataset, "host": host, "ts": rec["event_time"] if rec else ev.isoformat(),
                   "ingest_time": now_iso(), "seq": seq, "raw": line}
            if args.late_fraction and random.random() < args.late_fraction:
                pending_late.append((time.monotonic() + random.uniform(1.0, args.late_max), msg)); st.late += 1
            else:
                send(msg)
            seq += 1; n_orig += 1
            flush_late()

            # --- tiem template la ---
            if inject_lines and not injected_done and n_orig >= inject_after:
                for k in range(args.inject_count):
                    content = inject_lines[k % len(inject_lines)].replace("retry 3", "retry {}".format(k))
                    raw = format_raw(args.dataset, ev, host, content)
                    send({"source": args.dataset, "host": host, "ts": rec["event_time"] if rec else ev.isoformat(),
                          "ingest_time": now_iso(), "seq": seq, "raw": raw, "injected": True})
                    seq += 1; st.injected += 1
                injected_done = True
                print("[producer] da tiem {} dong template la tai seq={}".format(args.inject_count, seq), flush=True)

            if time.monotonic() >= next_stats:
                print(st.line(behind), flush=True); next_stats = time.monotonic() + args.stats_every

    flush_late(force=True)
    if producer is not None:
        producer.flush(30)
    print(st.line(0.0), flush=True)
    print("[producer] xong. tong gui={:,} parse_fail={} deliver_err={} max_behind={:.1f}s".format(
        st.sent, st.parse_fail, st.errors, st.max_behind), flush=True)


if __name__ == "__main__":
    main()
