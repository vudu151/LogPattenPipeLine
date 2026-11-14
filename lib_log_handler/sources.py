"""So dang ky nguon (source catalog): moi thong tin RIENG cua tung nguon nam o day, mot cho duy nhat.

gav.py dung cac hang so nay trong phep bien doi (mui gio, mien thoi gian de suy ra nam, ten may mac dinh);
lav.py dung de mo ta nguon; normalizer.py dung de kiem tra mien gia tri.
Them mot nguon moi = them mot muc o day + mot khung nhin trong gav.py.
"""
SOURCE_INFO = {
    "Windows": {
        "os_family": "windows",
        "note": "Windows 7, nhat ky Component Based Servicing (CBS.log)",
        "tz_offset_hours": 8,                       # gio trong dong log la gio dia phuong UTC+8 (dong CSI ghi kem gio UTC)
        "time_range": ("2016-09-01", "2017-05-31"), # mien thoi gian hop le cua nguon
        "default_host": "WIN-CBS-01",               # CBS.log khong ghi ten may: gan mot ten co dinh cho ca nguon
        "native": ["event_time", "component", "level", "content"],          # thuoc tinh LogEvent co san trong dong log
        "levels": {"Info", "Warning", "Error"},     # danh muc muc log hop le
    },
    "Linux": {
        "os_family": "unix",
        "note": "May chu Linux (Red Hat), syslog /var/log/messages",
        "tz_offset_hours": 0,                       # syslog khong ghi mui gio: gia dinh UTC
        "time_range": ("2005-06-01", "2006-03-31"), # syslog khong ghi nam: nam duoc suy ra tu mien nay
        "default_host": None,
        "native": ["event_time", "host", "component", "pid", "content"],
        "levels": None,                             # nguon khong co muc log
    },
    "Mac": {
        "os_family": "unix",
        "note": "MacBook Pro, macOS system.log",
        "tz_offset_hours": -7,                      # may o mang truong UC Berkeley (airbears2, calvisitor): gio PDT = UTC-7
        "time_range": ("2017-07-01", "2017-07-31"),
        "default_host": None,
        "native": ["event_time", "host", "component", "pid", "content"],
        "levels": None,
        # mot may nhung 38 ten: moi lan doi mang, DHCP cap ten moi. Dua ve mot ten chuan.
        "host_alias_pattern": r"^(calvisitor|airbears2)-\d+-\d+-\d+-\d+$",
        "canonical_host": "authorMacBook-Pro",
    },
}
SOURCES = list(SOURCE_INFO)

# Tep du lieu cua tung nguon (duong dan tuong doi so voi data/raw)
DEFAULT_FILES = {s: "%s/%s.log" % (s, s) for s in SOURCES}          # tep phat lai / danh gia
WARMUP_FILES = dict(DEFAULT_FILES)                                   # tep hoc mau known (phan dau cua cung tep)
SAMPLE_FILES = {s: "%s/%s_2k.log" % (s, s) for s in SOURCES}        # mau Loghub-2k co dap an
