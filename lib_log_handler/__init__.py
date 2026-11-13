"""lib-log-handler: thu vien tich hop va chuan hoa log dung chung cho producer, warm-up, stream processor va danh gia.

    schema.py      luoc do trung gian LogEvent
    gav.py         anh xa GAV: wrapper tung nguon, khai bao tuong minh, apply() = khai trien khung nhin
    lav.py         mo ta nguon kieu LAV, chon nguon va viet lai truy van
    normalizer.py  bo luat chat luong (giu / sua / cach ly) va tu dien ma muc log
    labels.py      ghep nhan tu tep nhan rieng theo khoa (lien ket ban ghi theo dinh danh)
    sources.py     so dang ky tep du lieu

API chinh:
    parse(source, line)   -> dict thuoc tinh LogEvent do wrapper sinh ra, hoac None
    handle(source, line)  -> (action, flags, event): wrapper -> ghep nhan -> normalizer; action in {ok, repair, reject}
"""
from lib_log_handler import gav, labels, lav, normalizer
from lib_log_handler.gav import SOURCES, format_raw
from lib_log_handler.sources import DEFAULT_FILES, SAMPLE_FILES, WARMUP_FILES

__version__ = "1.0.0"
__all__ = ["parse", "handle", "SOURCES", "format_raw", "DEFAULT_FILES", "SAMPLE_FILES", "WARMUP_FILES", "gav", "lav", "normalizer", "labels"]


def parse(source, line):
    """Wrapper GAV cua nguon `source` ap dung len mot dong log."""
    return gav.apply(source, line)


def handle(source, line):
    """Toan bo buoc xu ly mot dong: wrapper GAV -> ghep nhan theo khoa -> kiem tra chat luong va chuan hoa."""
    return normalizer.check(source, labels.enrich(source, gav.apply(source, line)))
