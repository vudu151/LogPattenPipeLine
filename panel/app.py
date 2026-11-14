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
CH = dict(host="localhost", port=8123, username="app", password="app", database="logs")

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


def alert_pid():
    p = os.path.join(LOGS, "alert.pid")
    if not os.path.exists(p):
        return None
    try:
        pid = int(open(p).read().strip()); os.kill(pid, 0); return pid
    except (ValueError, ProcessLookupError, PermissionError):
        return None


def start_alert():
    log = open(os.path.join(LOGS, "alert_service.log"), "w")
    proc = subprocess.Popen([".venv/bin/python", "-u", "alerting/alert_service.py", "--suppress", "20"], cwd=PIPE, stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True)
    open(os.path.join(LOGS, "alert.pid"), "w").write(str(proc.pid))
    return proc.pid


def stop_alert():
    pid = alert_pid()
    if pid:
        os.killpg(os.getpgid(pid), signal.SIGTERM)


def start_replay(args):
    log = open(os.path.join(LOGS, "replay.log"), "w")
    proc = subprocess.Popen([".venv/bin/python", "log_producer/producer.py"] + args, cwd=PIPE, stdout=log, stderr=subprocess.STDOUT,
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
    """Truy van PostgreSQL (sieu du lieu: so mau, cua so, chat luong, hieu nang)."""
    try:
        return pd.read_sql(sql, pg_engine())
    except Exception as e:  # noqa: BLE001
        return pd.DataFrame({"lỗi": [str(e)[:200]]})


@st.cache_resource
def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(**CH)


def ch(sql, params=None):
    """Truy van ClickHouse (kho OLAP: log da chuan hoa, dong bi cach ly, canh bao). Tra ve (DataFrame, so ms)."""
    t0 = time.time()
    try:
        df = ch_client().query_df(sql, parameters=params or {})
    except Exception as e:  # noqa: BLE001
        df = pd.DataFrame({"lỗi": [str(e)[:300]]})
    return df, (time.time() - t0) * 1000


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
    st.write(("🟢" if job_running(1) else "⚪") + " job 1 · " + ("🟢" if job_running(2) else "⚪") + " job 2 · " + ("🟢" if replay_pid() else "⚪") + " producer · " + ("🟢" if alert_pid() else "⚪") + " alert")
    st.markdown("**Giao diện khác**")
    st.markdown("- [Kafka UI](http://localhost:8090) · topic, message\n- [Spark master](http://localhost:8080) · worker, job\n- [Spark job 1](http://localhost:4040) · [job 2](http://localhost:4041)\n- [Grafana](http://localhost:3000) · dashboard\n- [ClickHouse](http://localhost:8123/play?user=app&password=app) · SQL")
    st.markdown("**Hạ tầng**")
    if st.button("▶ Bật hạ tầng (up)", use_container_width=True):
        run_and_show("up", ["up"])
    if st.button("✔ Kiểm tra (check)", use_container_width=True):
        run_and_show("check", ["check"])
    if st.button("■ Tắt hạ tầng (down)", use_container_width=True):
        run_and_show("down", ["down"])

tab_demo, tab_prep, tab_run, tab_res = st.tabs(["🚀 Demo nhanh", "1 · Chuẩn bị", "2 · Chạy", "3 · Tìm kiếm và kết quả"])

# ---------------- tab demo nhanh ----------------
with tab_demo:
    st.subheader("Kịch bản demo trong một nút")
    st.markdown("Giả định: hạ tầng đã bật, đã warm-up và train (tab Chuẩn bị). Nút này sẽ: bật job 1, job 2 và Alert System nếu chưa chạy, rồi phát log có chèn mẫu lạ.")
    WARMUP_SKIP = {"Linux": 5113, "Mac": 23456, "Windows": 1000000}      # số dòng đã dùng cho warm-up 20%
    c0, c1, c2, c3 = st.columns(4)
    d_ds = c0.selectbox("Hệ điều hành", ["Linux", "Mac", "Windows"])
    d_skip = c1.number_input("Bỏ qua N dòng đầu (sau warm-up)", value=WARMUP_SKIP[d_ds], step=1000)
    d_limit = c2.number_input("Phát bao nhiêu dòng", value=20000, step=5000)
    d_rate = c3.number_input("Tốc độ (dòng/giây)", value=500, step=100)
    if st.button("🚀 Bắt đầu demo", type="primary"):
        with st.status("Đang chuẩn bị demo", expanded=True) as box:
            if not job_running(1):
                st.write("Bật job 1…"); sh(["job1"]); time.sleep(25)
            if not job_running(2):
                st.write("Bật job 2…"); sh(["job2"]); time.sleep(25)
            if not alert_pid():
                st.write("Bật Alert System…"); start_alert()
            if replay_pid():
                stop_replay(); time.sleep(1)
            pid = start_replay(["--dataset", d_ds, "--skip", str(int(d_skip)), "--limit", str(int(d_limit)), "--rate", str(int(d_rate)),
                                "--inject", "log_producer/inject_templates.txt", "--at", "0.5", "--inject-count", "50", "--stats-every", "5"])
            st.write(f"Log Producer đang phát (pid {pid}). Xem tab Kết quả hoặc Kafka UI.")
            box.update(label="Demo đang chạy", state="complete")
    if st.button("⏹ Dừng producer"):
        stop_replay(); st.success("Đã dừng producer")

    @st.fragment(run_every=5)
    def live():
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Log Producer**"); st.code(tail(os.path.join(LOGS, "replay.log"), 6), language="text")
            st.markdown("**Job 1 (lib-log-handler, Drain3)**"); st.code(tail(os.path.join(LOGS, "job1.log"), 5, ["[job1]"]), language="text")
            st.markdown("**Job 2 (cửa sổ, Isolation Forest)**"); st.code(tail(os.path.join(LOGS, "job2.log"), 5, ["[job2]"]), language="text")
            st.markdown("**Alert System**"); st.code(tail(os.path.join(LOGS, "alert_service.log"), 8), language="text")
        with c2:
            st.markdown("**Kho OLAP (ClickHouse)**")
            df, ms = ch("""SELECT source, count() AS dong, countIf(status = 'unknown') AS dong_unknown,
                                  uniqExact(template_id) AS so_mau, countIf(quality_flags != '') AS dong_co_co
                           FROM logs.log_events GROUP BY source ORDER BY source""")
            st.dataframe(df, hide_index=True, use_container_width=True); st.caption("truy vấn %.0f ms" % ms)
            st.markdown("**Mẫu mới phát hiện (unknown)**")
            st.dataframe(q("SELECT source, template_id, occurrences, template FROM templates WHERE NOT from_warmup ORDER BY first_seen DESC LIMIT 8"), hide_index=True, use_container_width=True)
            st.markdown("**Cảnh báo mới nhất**")
            df, _ = ch("SELECT detected_at, level, type, title FROM logs.alerts ORDER BY detected_at DESC LIMIT 8")
            st.dataframe(df, hide_index=True, use_container_width=True)
    live()

# ---------------- tab chuan bi ----------------
with tab_prep:
    st.subheader("Chuẩn bị dữ liệu và model (làm một lần)")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Tải dataset**")
        ds = st.selectbox("Dataset", ["2k", "Linux", "Mac", "Windows", "all"])
        if st.button("⬇ Tải"):
            run_and_show("download " + ds, ["download", ds], 3600)
        st.markdown("**Thống kê dataset**")
        p_ds = st.selectbox("Nguồn cần xem", ["Linux", "Mac", "Windows"], key="p_ds")
        if st.button("📊 Hồ sơ nguồn (mẫu 2k)"):
            run_and_show("profile", ["profile", "--dataset", p_ds, "--sample"])
        if st.button("🧪 Hồ sơ chất lượng (200.000 dòng đầu)"):
            run_and_show("quality", ["quality", "--dataset", p_ds, "--limit", "200000"])
    with c2:
        st.markdown("**Warm-up: học mẫu đã biết**")
        frac = st.slider("Tỉ lệ dòng đầu dùng để học", 0.05, 0.5, 0.2, 0.05)
        w_ds = st.selectbox("Nguồn", ["Linux", "Mac", "Windows"], key="w_ds")
        if st.button("🧠 Warm-up"):
            run_and_show("warmup", ["warmup", "--dataset", w_ds, "--fraction", str(frac)], 1800)
        st.markdown("**Độ chính xác parse (Loghub-2k)**")
        if st.button("🎯 Eval parsing"):
            run_and_show("eval-parsing", ["eval-parsing"])
    with c3:
        st.markdown("**Train Isolation Forest**")
        win = st.selectbox("Độ dài window (giây)", [60, 300], 0)
        key = st.selectbox("Gom theo", ["ALL", "host"], 0)
        thr = st.selectbox("Ngưỡng", ["q0.95", "q0.99", "f1"], 0)
        t_ds = st.selectbox("Nguồn", ["Linux", "Mac", "Windows"], key="t_ds")
        if st.button("🌲 Train"):
            run_and_show("train", ["train", "--dataset", t_ds, "--key", key, "--window", str(win), "--threshold", thr], 1800)

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
        st.markdown("**Log Producer · phát log lên Kafka**  " + ("🟢 đang phát" if replay_pid() else "⚪ dừng"))
        with st.form("replay"):
            r1, r2 = st.columns(2)
            ds = r1.selectbox("Dataset", ["Linux", "Mac", "Windows"])
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
            if inject: args += ["--inject", "log_producer/inject_templates.txt", "--at", str(at), "--inject-count", "50"]
            if late: args += ["--late-fraction", str(late), "--late-max", "10"]
            pid = start_replay(args)
            st.success(f"Đang phát (pid {pid}): producer.py {' '.join(args)}")
        if st.button("■ Dừng phát"):
            stop_replay(); st.success("Đã dừng")
        st.code(tail(os.path.join(LOGS, "replay.log"), 12), language="text")
        st.markdown("**Alert System**  " + ("🟢 đang chạy" if alert_pid() else "⚪ dừng"))
        a1, a2 = st.columns(2)
        if a1.button("▶ Bật Alert System"):
            start_alert(); st.success("Đã bật")
        if a2.button("■ Dừng Alert System"):
            stop_alert(); st.success("Đã dừng")
        st.code(tail(os.path.join(LOGS, "alert_service.log"), 8), language="text")
        st.markdown("**Kafka**")
        k1, k2 = st.columns(2)
        topic = k1.selectbox("Topic", ["raw-logs.linux", "parsed-logs", "raw-logs.mac", "raw-logs.windows", "rejected-logs", "alerts"])
        if k2.button("Đếm message"):
            ok, out = sh(["count", topic]); st.code(out, language="text")
        if st.button("Xem 3 message đầu"):
            ok, out = sh(["consume", topic, "3"]); st.code(out[:3000], language="text")

# ---------------- tab ket qua ----------------
with tab_res:
    st.subheader("Tìm kiếm log trong kho OLAP (ClickHouse)")
    f1, f2, f3, f4 = st.columns([1, 1, 2, 1])
    src = f1.selectbox("Nguồn", ["tất cả", "Windows", "Linux", "Mac"])
    stt = f2.selectbox("Trạng thái", ["tất cả", "unknown", "known"])
    kw = f3.text_input("Từ khóa trong nội dung", "")
    n = f4.slider("Số dòng", 20, 500, 100)
    conds = ["1"]; params = {}
    if src != "tất cả": conds.append("source = {src:String}"); params["src"] = src
    if stt != "tất cả": conds.append("status = {stt:String}"); params["stt"] = stt
    if kw.strip(): conds.append("positionCaseInsensitive(content, {kw:String}) > 0"); params["kw"] = kw.strip()
    df, ms = ch("SELECT event_time, source, host, severity, status, template_id, quality_flags, substring(content, 1, 140) AS content "
                "FROM logs.log_events WHERE " + " AND ".join(conds) + " ORDER BY processed_time DESC LIMIT %d" % n, params)
    st.dataframe(df, hide_index=True, use_container_width=True); st.caption("truy vấn %.0f ms" % ms)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Tổng hợp theo nguồn và mức log chung**")
        df, ms = ch("SELECT source, severity, count() AS dong FROM logs.log_events GROUP BY source, severity ORDER BY source, dong DESC")
        st.dataframe(df, hide_index=True, use_container_width=True); st.caption("truy vấn %.0f ms" % ms)
        st.markdown("**Dòng bị cách ly (rejected_logs)**")
        df, _ = ch("SELECT source, quality_flags, count() AS dong FROM logs.rejected_logs GROUP BY source, quality_flags ORDER BY dong DESC")
        st.dataframe(df, hide_index=True, use_container_width=True)
    with c2:
        st.markdown("**Cảnh báo (alerts)**")
        df, _ = ch("SELECT detected_at, level, type, source, title FROM logs.alerts ORDER BY detected_at DESC LIMIT 30")
        st.dataframe(df, hide_index=True, use_container_width=True)
    st.markdown("**Viết lại truy vấn theo mô tả nguồn LAV**")
    from lib_log_handler import lav
    l1, l2 = st.columns(2)
    fam = l1.selectbox("Họ hệ điều hành", ["bất kỳ", "unix", "windows"])
    nat = l2.multiselect("Thuộc tính phải có sẵn trong dòng log", ["host", "pid", "level"])
    w = {}
    if fam != "bất kỳ": w["os_family"] = fam
    if nat: w["native"] = nat
    st.write("Nguồn trả lời được:", lav.select_sources(["event_time", "host", "pid", "content"], w) or "không có")
    st.code(lav.rewrite(["event_time", "host", "pid", "content"], w), language="sql")

    st.subheader("Siêu dữ liệu trong PostgreSQL")
    if st.button("🔄 Làm mới"):
        st.rerun()
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Cửa sổ theo nguồn và lý do**")
        st.dataframe(q("""SELECT source, reason, count(*) AS so_cua_so, count(*) FILTER (WHERE label='alert') AS co_dong_chen
                          FROM window_alerts GROUP BY 1,2 ORDER BY 1,2"""), hide_index=True, use_container_width=True)
        st.markdown("**Chất lượng dữ liệu theo luật (data_quality)**")
        st.dataframe(q("SELECT source, rule, action, sum(n) AS dong FROM data_quality GROUP BY 1,2,3 ORDER BY 1,2"), hide_index=True, use_container_width=True)
    with c2:
        st.markdown("**Mẫu mới (unknown / promoted)**")
        st.dataframe(q("SELECT source, template_id, status, occurrences, template FROM templates WHERE NOT from_warmup ORDER BY first_seen DESC LIMIT 30"), hide_index=True, use_container_width=True)
        st.markdown("**Hiệu năng (pipeline_metrics, 20 mẫu gần nhất)**")
        st.dataframe(q("SELECT ts, stage, round(events_per_sec) AS ev_s, round(batch_duration_ms) AS batch_ms, round(latency_p95_ms) AS p95_ms FROM pipeline_metrics ORDER BY ts DESC LIMIT 20"), hide_index=True, use_container_width=True)
