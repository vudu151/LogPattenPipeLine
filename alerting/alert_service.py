#!/usr/bin/env python3
"""Alert System: doc topic `alerts`, khu canh bao lap, in ra man hinh, ghi tep va (tuy chon) gui webhook.

  ./pipeline.sh alerts                       # chay o tien canh
  ./pipeline.sh alerts --webhook https://hooks.slack.com/services/...   # gui them toi Slack / Mattermost / Discord (tuong thich {"text": ...})
  ./pipeline.sh alerts --from-beginning --min-level HIGH

Nguon canh bao (do hai Spark job day vao topic alerts):
  new-template    : job 1 thay mot mau log chua tung co (tang 1)            -> HIGH
  window-anomaly  : job 2 cham diem mot cua so la bat thuong (tang 2)       -> HIGH neu co mau la, MEDIUM neu chi do diem
Khu lap: cung (type, source) trong --suppress giay chi bao 1 lan, cac lan sau duoc dem va bao gop.
"""
import argparse
import json
import os
import signal
import sys
import time
import urllib.request

from confluent_kafka import Consumer

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
LEVELS = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
COLOR = {"HIGH": "\033[1;31m", "MEDIUM": "\033[1;33m", "LOW": "\033[0;36m"}
RESET = "\033[0m"


def post_webhook(url, text):
    try:
        req = urllib.request.Request(url, data=json.dumps({"text": text}).encode("utf-8"), headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()
        return True
    except Exception as e:  # noqa: BLE001
        print("[alert] webhook loi: %s" % e, flush=True)
        return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    ap.add_argument("--topic", default="alerts")
    ap.add_argument("--group", default="alert-system")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--min-level", default="MEDIUM", choices=list(LEVELS))
    ap.add_argument("--suppress", type=float, default=30.0, help="khoang khu lap (giay) cho cung (type, source)")
    ap.add_argument("--webhook", help="URL webhook nhan JSON {text: ...}")
    ap.add_argument("--out", default=os.path.join(ROOT, "lake", "logs", "alerts.jsonl"))
    ap.add_argument("--max-seconds", type=float, help="tu thoat sau N giay (dung khi kiem thu)")
    args = ap.parse_args()

    c = Consumer({"bootstrap.servers": args.bootstrap, "group.id": args.group, "enable.auto.commit": True,
                  "auto.offset.reset": "earliest" if args.from_beginning else "latest"})
    c.subscribe([args.topic])
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out = open(args.out, "a", encoding="utf-8")
    last = {}; held = {}; stats = {"received": 0, "notified": 0, "suppressed": 0, "below_level": 0}
    running = [True]
    signal.signal(signal.SIGINT, lambda *_: running.__setitem__(0, False))
    signal.signal(signal.SIGTERM, lambda *_: running.__setitem__(0, False))
    t_end = time.time() + args.max_seconds if args.max_seconds else None
    print("[alert] dang nghe topic %s tren %s (min-level=%s, suppress=%.0fs)" % (args.topic, args.bootstrap, args.min_level, args.suppress), flush=True)

    def notify(a, extra=""):
        line = "[%s] %-14s %s%s" % (a.get("level", "?"), a.get("type", "?"), a.get("title", ""), extra)
        print(COLOR.get(a.get("level"), "") + line + RESET, flush=True)
        a["notified_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        out.write(json.dumps(a, ensure_ascii=False) + "\n"); out.flush()
        if args.webhook:
            post_webhook(args.webhook, line)
        stats["notified"] += 1

    while running[0] and (t_end is None or time.time() < t_end):
        msg = c.poll(1.0)
        now = time.time()
        for k in [k for k, (t0, n, a) in held.items() if now - t0 >= args.suppress]:   # het khoang khu lap: bao gop
            t0, n, a = held.pop(k)
            notify(a, "  (+%d canh bao cung loai trong %.0fs)" % (n, args.suppress))
            last[k] = now
        if msg is None or msg.error():
            continue
        try:
            a = json.loads(msg.value())
        except ValueError:
            continue
        stats["received"] += 1
        if LEVELS.get(a.get("level", "LOW"), 0) < LEVELS[args.min_level]:
            stats["below_level"] += 1
            continue
        k = (a.get("type"), a.get("source"))
        if k in last and now - last[k] < args.suppress:
            stats["suppressed"] += 1
            t0, n, _ = held.get(k, (last[k], 0, a))
            held[k] = (t0, n + 1, a)
            continue
        notify(a); last[k] = now
    for k, (t0, n, a) in held.items():
        notify(a, "  (+%d canh bao cung loai)" % n)
    c.close(); out.close()
    print("[alert] dung. nhan=%(received)d thong bao=%(notified)d khu lap=%(suppressed)d duoi nguong=%(below_level)d" % stats, flush=True)


if __name__ == "__main__":
    main()
