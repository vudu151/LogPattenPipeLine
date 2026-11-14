"""lib-log-handler: thu vien tich hop va chuan hoa log dung chung cho producer, warm-up, stream processor va danh gia.

    sources.py     so dang ky nguon: mui gio, mien thoi gian, ten may mac dinh, tep du lieu
    schema.py      luoc do trung gian LogEvent
    gav.py         anh xa GAV: wrapper tung nguon, khai bao tuong minh, apply() = khai trien khung nhin  (buoc Transform)
    lav.py         mo ta nguon kieu LAV, chon nguon va viet lai truy van
    normalizer.py  bo luat chat luong (giu / sua / cach ly)

API chinh:
    parse(source, line)   -> dict thuoc tinh LogEvent do khung nhin GAV sinh ra, hoac None
    handle(source, line)  -> (action, flags, event): khung nhin GAV -> normalizer; action in {ok, repair, reject}
"""
from lib_log_handler import gav, lav, normalizer
from lib_log_handler.gav import SOURCES, format_raw
from lib_log_handler.sources import DEFAULT_FILES, SAMPLE_FILES, SOURCE_INFO, WARMUP_FILES

__version__ = "2.0.0"
__all__ = ["parse", "handle", "SOURCES", "SOURCE_INFO", "format_raw", "DEFAULT_FILES", "SAMPLE_FILES", "WARMUP_FILES",
           "gav", "lav", "normalizer"]


def parse(source, line):
    """Khung nhin GAV cua nguon `source` ap dung len mot dong log."""
    return gav.apply(source, line)


def handle(source, line):
    """Toan bo buoc xu ly mot dong: khung nhin GAV -> kiem tra chat luong va chuan hoa."""
    return normalizer.check(source, gav.apply(source, line))
