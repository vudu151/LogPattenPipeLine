"""Bang dieu khien demo (Streamlit). Chay: ./pipeline.sh panel  ->  http://localhost:8501

Moi nut goi dung lenh ./pipeline.sh tuong ung, nen ket qua giong het khi go tay.
"""
import os
import signal
import subprocess
import time

import pandas as pd
import streamlit as st

PIPE = os.environ.get("PIPELINE_DIR", os.path.expanduser("~/work/log-pattern-pipeline"))
LOGS = os.path.join(PIPE, "lake", "logs")
os.makedirs(LOGS, exist_ok=True)
PG = "postgresql://app:app@localhost:5432/logs"

st.set_page_config(page_title="Log Pattern Pipeline", page_icon="🧭", layout="wide")
st.markdown("""<style>
div[data-testid="stCode"] pre{font-size:.78rem; max-height:320px; overflow:auto}
.small{color:#777; font-size:.85rem}
</style>""", unsafe_allow_html=True)


# ---------------- tien ich ----------------
def sh(args, timeout=600):
    """Chay ./pipeline.sh <args> va tra ve (ok, output)."""
    r = subprocess.run(["./pipeline.sh"] + args, cwd=PIPE, capture_output=True, text=True, timeout=timeout)
    return r.returncode == 0, (r.stdout + r.stderr).strip()


def run_and_show(label, args, timeout=900):
    with st.status(f"Đang chạy: ./pipeline.sh {' '.join(args)}", expanded=True) as box:
        ok, out = sh(args, timeout)
        st.code(out or "(không có output)", language="text")
        box.update(label=("Xong: " if ok else "Lỗi: ") + label, state="complete" if ok else "error", expanded=not ok)
    return ok


def tail(path, n=25, grep=None):
    if not os.path.exists(path):
        return "(chưa có log)"
    lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
    if grep:
        lines = [l for l in lines if any(g in l for g in grep)]
    return "\n".join(lines[-n:]) or "(trống)"


def container_running(name):
    ok, out = sh(["ps"])
    return name in out and "Up" in out


def job_running(n):
    r = subprocess.run(["docker", "compose", "exec", "-T", "spark-master", "pgrep", "-f", f"job{n}_"], cwd=PIPE, capture_output=True)
    return r.returncode == 0


def replay_pid():
    p = os.path.join(LOGS, "replay.pid")
    if not os.path.exists(p):
        return None
    try:
        pid = int(open(p).read().strip()); os.kill(pid, 0); return pid
    except (ValueError, ProcessLookupError, PermissionError):
        return None


def start_replay(args):
    log = open(os.path.join(LOGS, "replay.log"), "w")
    proc = subprocess.Popen([".venv/bin/python", "replayer/replay.py"] + args, cwd=PIPE, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True)
    open(os.path.join(LOGS, "replay.pid"), "w").write(str(proc.pid))
    return proc.pid


def stop_replay():
    pid = replay_pid()
    if pid:
        os.killpg(os.getpgid(pid), signal.SIGTERM)


@st.cache_resource
def pg_engine():
    import sqlalchemy
    return sqlalchemy.create_engine(PG)


def q(sql):
    try:
        return pd.read_sql(sql, pg_engine())
    except Exception as e:  # noqa: BLE001
        return pd.DataFrame({"lỗi": [str(e)[:200]]})


# ---------------- sidebar: trang thai ----------------
with st.sidebar:
    st.title("🧭 Log Pattern Pipeline")
    st.caption(PIPE)
    if st.button("🔄 Cập nhật trạng thái", use_container_width=True):
        st.rerun()
    ok, out = sh(["ps"])
    ups = [l.split()[0] for l in out.splitlines()[1:] if " Up" in l]
    st.markdown(f"**Container đang chạy: {len(ups)}**")
    st.caption(", ".join(ups) or "không có")
    c1, c2 = st.columns(2)
    st.markdown("**Spark job**")
    st.write(("🟢" if job_running(1) else "⚪") + " job 1 · " + ("🟢" if job_running(2) else "⚪") + " job 2 · " + ("🟢" if replay_pid() else "⚪") + " replayer")
    st.markdown("**Giao diện khác**")
    st.markdown("- [Kafka UI](http://localhost:8090) · topic, message\n- [Spark master](http://localhost:8080) · worker, job\n- [Spark job 1](http://localhost:4040) · [job 2](http://localhost:4041)\n- [Grafana](http://localhost:3000) · dashboard")
    st.markdown("**Hạ tầng**")
    if st.button("▶ Bật hạ tầng (up)", use_container_width=True):
        run_and_show("up", ["up"])
    if st.button("✔ Kiểm tra (check)", use_container_width=True):
        run_and_show("check", ["check"])
    if st.button("■ Tắt hạ tầng (down)", use_container_width=True):
        run_and_show("down", ["down"])

tab_demo, tab_prep, tab_run, tab_res = st.tabs(["🚀 Demo nhanh", "1 · Chuẩn bị", "2 · Chạy", "3 · Kết quả"])

# ---------------- tab demo nhanh ----------------
with tab_demo:
    st.subheader("Kịch bản demo trong một nút")
    st.markdown("Giả định: hạ tầng đã bật, đã warm-up và train (tab Chuẩn bị). Nút này sẽ: bật job 1 và job 2 nếu chưa chạy, rồi phát log có tiêm mẫu lạ.")
    c1, c2, c3 = st.columns(3)
    d_skip = c1.number_input("Bỏ qua N dòng đầu (sau warm-up)", value=1109592, step=10000)
    d_limit = c2.number_input("Phát bao nhiêu dòng", value=60000, step=10000)
    d_rate = c3.number_input("Tốc độ (dòng/giây)", value=2000, step=500)
    if st.button("🚀 Bắt đầu demo", type="primary"):
        with st.status("Đang chuẩn bị demo", expanded=True) as box:
            if not job_running(1):
                st.write("Bật job 1…"); sh(["job1"]); time.sleep(25)
            if not job_running(2):
                st.write("Bật job 2…"); sh(["job2"]); time.sleep(25)
            if replay_pid():
                stop_replay(); time.sleep(1)
            pid = start_replay(["--dataset", "BGL", "--skip", str(int(d_skip)), "--limit", str(int(d_limit)), "--rate", str(int(d_rate)),
                                "--inject", "replayer/inject_templates.txt", "--at", "0.5", "--inject-count", "50", "--stats-every", "5"])
            st.write(f"Replayer đang phát (pid {pid}). Xem tab Kết quả hoặc Kafka UI.")
            box.update(label="Demo đang chạy", state="complete")
    if st.button("⏹ Dừng replayer"):
        stop_replay(); st.success("Đã dừng replayer")

    @st.fragment(run_every=5)
    def live():
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Replayer**"); st.code(tail(os.path.join(LOGS, "replay.log"), 8), language="text")
            st.markdown("**Job 1 (parse, Drain3)**"); st.code(tail(os.path.join(LOGS, "job1.log"), 6, ["[job1]"]), language="text")
            st.markdown("**Job 2 (window, Isolation Forest)**"); st.code(tail(os.path.join(LOGS, "job2.log"), 6, ["[job2]"]), language="text")
        with c2:
            st.markdown("**Số liệu trong PostgreSQL**")
            st.dataframe(q("""SELECT (SELECT count(*) FROM log_history) AS log_history,
                                      (SELECT count(*) FROM log_history WHERE status='unknown') AS dong_unknown,
                                      (SELECT count(*) FROM templates WHERE NOT from_warmup) AS mau_moi,
                                      (SELECT count(*) FROM window_alerts) AS phut,
                                      (SELECT count(*) FROM window_alerts WHERE is_anomaly) AS phut_bat_thuong"""), hide_index=True, use_container_width=True)
            st.markdown("**Mẫu mới phát hiện (unknown)**")
            st.dataframe(q("SELECT template_id, status, occurrences, template FROM templates WHERE NOT from_warmup ORDER BY first_seen DESC LIMIT 8"), hide_index=True, use_container_width=True)
            st.markdown("**Phút bất thường mới nhất**")
            st.dataframe(q("SELECT window_start, n_events, n_unknown, n_error, round(score::numeric,3) AS score, reason, label FROM window_alerts WHERE is_anomaly ORDER BY detected_at DESC LIMIT 8"), hide_index=True, use_container_width=True)
    live()

# ---------------- tab chuan bi ----------------
with tab_prep:
    st.subheader("Chuẩn bị dữ liệu và model (làm một lần)")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Tải dataset**")
        ds = st.selectbox("Dataset", ["2k", "BGL", "HDFS", "OpenStack", "all"])
        if st.button("⬇ Tải"):
            run_and_show("download " + ds, ["download", ds], 3600)
        st.markdown("**Thống kê dataset**")
        if st.button("📊 Profile BGL (mẫu 2k)"):
            run_and_show("profile", ["profile", "--dataset", "BGL", "--sample"])
        if st.button("📊 Profile BGL (đầy đủ, ~1 phút)"):
            run_and_show("profile", ["profile", "--dataset", "BGL"])
    with c2:
        st.markdown("**Warm-up: học mẫu đã biết**")
        frac = st.slider("Tỉ lệ dòng đầu dùng để học", 0.05, 0.5, 0.2, 0.05)
        if st.button("🧠 Warm-up BGL"):
            run_and_show("warmup", ["warmup", "--dataset", "BGL", "--fraction", str(frac)], 1800)
        st.markdown("**Độ chính xác parse (Loghub-2k)**")
        if st.button("🎯 Eval parsing"):
            run_and_show("eval-parsing", ["eval-parsing"])
    with c3:
        st.markdown("**Train Isolation Forest**")
        win = st.selectbox("Độ dài window (giây)", [60, 300], 0)
        key = st.selectbox("Gom theo", ["ALL", "host"], 0)
        thr = st.selectbox("Ngưỡng", ["q0.95", "q0.99", "f1"], 0)
        if st.button("🌲 Train"):
            run_and_show("train", ["train", "--dataset", "BGL", "--key", key, "--window", str(win), "--threshold", thr], 1800)

# ---------------- tab chay ----------------
with tab_run:
    st.subheader("Điều khiển từng phần")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Spark job 1 · parse + Drain3**  " + ("🟢 đang chạy" if job_running(1) else "⚪ dừng"))
        fb1 = st.checkbox("Đọc lại từ đầu topic", key="fb1")
        b1, b2, b3 = st.columns(3)
        if b1.button("▶ Bật job 1"):
            if fb1: sh(["job1-reset"])
            run_and_show("job1", ["job1"] + (["--from-beginning"] if fb1 else []))
        if b2.button("■ Dừng job 1"):
            run_and_show("job1-stop", ["job1-stop"])
        if b3.button("🗑 Xóa checkpoint 1"):
            run_and_show("job1-reset", ["job1-reset"])
        st.code(tail(os.path.join(LOGS, "job1.log"), 12, ["[job1]", "Error", "Exception"]), language="text")

        st.markdown("**Spark job 2 · window + Isolation Forest**  " + ("🟢 đang chạy" if job_running(2) else "⚪ dừng"))
        fb2 = st.checkbox("Đọc lại từ đầu topic", key="fb2")
        b1, b2, b3 = st.columns(3)
        if b1.button("▶ Bật job 2"):
            if fb2: sh(["job2-reset"])
            run_and_show("job2", ["job2"] + (["--from-beginning"] if fb2 else []))
        if b2.button("■ Dừng job 2"):
            run_and_show("job2-stop", ["job2-stop"])
        if b3.button("🗑 Xóa checkpoint 2"):
            run_and_show("job2-reset", ["job2-reset"])
        st.code(tail(os.path.join(LOGS, "job2.log"), 12, ["[job2]", "Error", "Exception"]), language="text")
    with c2:
        st.markdown("**Replayer · phát log lên Kafka**  " + ("🟢 đang phát" if replay_pid() else "⚪ dừng"))
        with st.form("replay"):
            r1, r2 = st.columns(2)
            ds = r1.selectbox("Dataset", ["BGL", "HDFS", "OpenStack"])
            sample = r2.checkbox("Dùng file mẫu 2k")
            mode = r1.radio("Nhịp phát", ["Đều (--rate dòng/giây)", "Theo timestamp gốc (--speed lần)"])
            val = r2.number_input("Giá trị", value=2000, step=100)
            skip = r1.number_input("Bỏ qua N dòng đầu", value=949592, step=10000)
            limit = r2.number_input("Phát tối đa N dòng (0 = hết file)", value=100000, step=10000)
            inject = r1.checkbox("Tiêm mẫu lạ", value=True)
            at = r2.slider("Tiêm ở tiến độ", 0.0, 1.0, 0.5, 0.05)
            late = r1.slider("Tỉ lệ log đến trễ", 0.0, 0.3, 0.0, 0.01)
            go = st.form_submit_button("▶ Bắt đầu phát", type="primary")
        if go:
            if replay_pid(): stop_replay(); time.sleep(1)
            args = ["--dataset", ds, "--skip", str(int(skip)), "--stats-every", "5"]
            if sample: args.append("--sample")
            if limit: args += ["--limit", str(int(limit))]
            args += ["--rate", str(int(val))] if mode.startswith("Đều") else ["--speed", str(int(val))]
            if inject: args += ["--inject", "replayer/inject_templates.txt", "--at", str(at), "--inject-count", "50"]
            if late: args += ["--late-fraction", str(late), "--late-max", "10"]
            pid = start_replay(args)
            st.success(f"Đang phát (pid {pid}): replay.py {' '.join(args)}")
        if st.button("■ Dừng phát"):
            stop_replay(); st.success("Đã dừng")
        st.code(tail(os.path.join(LOGS, "replay.log"), 12), language="text")
        st.markdown("**Kafka**")
        k1, k2 = st.columns(2)
        topic = k1.selectbox("Topic", ["raw-logs.bgl", "parsed-logs", "raw-logs.hdfs", "raw-logs.openstack"])
        if k2.button("Đếm message"):
            ok, out = sh(["count", topic]); st.code(out, language="text")
        if st.button("Xem 3 message đầu"):
            ok, out = sh(["consume", topic, "3"]); st.code(out[:3000], language="text")

# ---------------- tab ket qua ----------------
with tab_res:
    st.subheader("Kết quả trong PostgreSQL")
    if st.button("🔄 Làm mới"):
        st.rerun()
    st.markdown("**Tổng quan**")
    st.dataframe(q("""SELECT (SELECT count(*) FROM templates WHERE from_warmup) AS mau_known_warmup,
                              (SELECT count(*) FROM templates WHERE NOT from_warmup) AS mau_moi,
                              (SELECT count(*) FROM log_history) AS dong_log,
                              (SELECT count(*) FROM log_history WHERE status='unknown') AS dong_unknown,
                              (SELECT count(*) FROM window_alerts) AS phut,
                              (SELECT count(*) FROM window_alerts WHERE is_anomaly) AS phut_bat_thuong,
                              (SELECT count(*) FROM window_alerts WHERE label='alert') AS phut_co_nhan_alert"""), hide_index=True, use_container_width=True)
    st.markdown("**Phút bất thường theo lý do**")
    st.dataframe(q("""SELECT reason, count(*) AS so_phut, sum(n_events) AS so_dong, round(avg(score)::numeric,3) AS diem_tb,
                             count(*) FILTER (WHERE label='alert') AS co_nhan_alert FROM window_alerts GROUP BY 1 ORDER BY 1"""), hide_index=True, use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Mẫu mới (unknown / promoted)**")
        st.dataframe(q("SELECT template_id, status, occurrences, first_seen, template FROM templates WHERE NOT from_warmup ORDER BY first_seen DESC LIMIT 30"), hide_index=True, use_container_width=True)
    with c2:
        st.markdown("**Top phút bất thường theo điểm**")
        st.dataframe(q("SELECT window_start, host, n_events, n_unknown, n_error, round(score::numeric,3) AS score, reason, label, top_templates->>'top' AS top FROM window_alerts WHERE is_anomaly ORDER BY score DESC LIMIT 30"), hide_index=True, use_container_width=True)
    st.markdown("**Log history**")
    f1, f2 = st.columns(2)
    stt = f1.selectbox("Trạng thái", ["tất cả", "unknown", "known"])
    n = f2.slider("Số dòng", 20, 500, 100)
    where = "" if stt == "tất cả" else f"WHERE status='{stt}'"
    st.dataframe(q(f"SELECT event_time, host, level, status, template_id, round(anomaly_score::numeric,3) AS score, left(content,120) AS content FROM log_history {where} ORDER BY id DESC LIMIT {n}"), hide_index=True, use_container_width=True)
    st.markdown("**Hiệu năng (pipeline_metrics, 30 mẫu gần nhất)**")
    st.dataframe(q("SELECT ts, stage, round(events_per_sec) AS ev_s, round(batch_duration_ms) AS batch_ms, round(latency_p50_ms) AS p50_ms, round(latency_p95_ms) AS p95_ms FROM pipeline_metrics ORDER BY ts DESC LIMIT 30"), hide_index=True, use_container_width=True)
