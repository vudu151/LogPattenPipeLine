"""Mo ta nguon kieu LAV (Local-as-View): moi nguon duoc dinh nghia la mot khung nhin tren luoc do trung gian.

    S_BGL(event_time, host, level, component, content, label)  ⊆  σ[source='BGL'](LogEvent)          (the gioi mo)

Khac voi GAV (noi cach TINH LogEvent tu nguon), mo ta LAV noi mot nguon CHUA GI theo ngon ngu cua luoc do trung gian:
thuoc tinh nao co, thuc the la gi, nhan o muc nao, mien thoi gian nao. Viet lai truy van trong LAV = tra loi truy van
bang khung nhin: voi mot truy van tren LogEvent, tim cac nguon co the dong gop cau tra loi. Vi cac khung nhin o day chi
la phep chon va chieu tren mot quan he, viec viet lai rut ve kiem tra bao ham thuoc tinh va tinh thoa duoc cua dieu kien.
Them mot nguon moi chi can them mot mo ta, khong phai sua dinh nghia LogEvent (uu diem cua LAV neu trong bai giang).
"""
from typing import Dict, List, Optional, Set

from lib_log_handler.schema import WRAPPER_ATTRS


class SourceView:
    def __init__(self, source, provides, entity, label_granularity, label_origin, time_range, note=""):
        self.source = source                    # hang so trong dieu kien chon: source = '<ten>'
        self.provides = set(provides)           # cac thuoc tinh LogEvent ma nguon cung cap
        self.entity = entity                    # y nghia cua thuoc tinh host
        self.label_granularity = label_granularity   # nhan gan theo: line | entity
        self.label_origin = label_origin        # inline (trong dong log) | file (tep nhan rieng, ghep theo khoa)
        self.time_range = time_range            # (tu, den) ISO date: mien thoi gian nguon bao phu
        self.note = note

    def definition(self):
        return "S_%s(%s) ⊆ σ[source='%s' ∧ event_time ∈ [%s, %s]](LogEvent)" % (
            self.source, ", ".join(a for a in WRAPPER_ATTRS if a in self.provides), self.source, self.time_range[0], self.time_range[1])


SOURCE_VIEWS = {
    "BGL": SourceView("BGL", WRAPPER_ATTRS, entity="node", label_granularity="line", label_origin="inline",
                      time_range=("2005-06-01", "2006-02-01"), note="Sieu may tinh Blue Gene/L"),
    "HDFS": SourceView("HDFS", WRAPPER_ATTRS, entity="block", label_granularity="entity", label_origin="file",
                       time_range=("2008-11-01", "2008-12-31"), note="He tep phan tan Hadoop"),
    "OpenStack": SourceView("OpenStack", WRAPPER_ATTRS, entity="instance", label_granularity="entity", label_origin="file",
                            time_range=("2017-05-01", "2017-06-30"), note="Nen tang dam may Nova"),
}


def select_sources(attributes=(), where=None):
    # type: (List[str], Optional[Dict]) -> List[str]
    """Tra ve cac nguon co the dong gop cau tra loi cho truy van  π[attributes] σ[where] (LogEvent).

    where: dict dieu kien; khoa dac biet: source, entity, label_granularity, label_origin, time_from, time_to.
    Mot nguon bi loai khi thieu thuoc tinh duoc hoi hoac khi dieu kien mau thuan voi mo ta cua nguon (khong thoa duoc).
    """
    where = where or {}
    out = []
    for name, v in SOURCE_VIEWS.items():
        if not set(attributes) <= v.provides | {"source", "severity", "quality_flags", "template_id", "template", "status",
                                                 "ingest_time", "processed_time", "raw"}:
            continue
        if "source" in where and where["source"] != name:
            continue
        if any(k in where and where[k] != getattr(v, k) for k in ("entity", "label_granularity", "label_origin")):
            continue
        if "time_from" in where and where["time_from"] > v.time_range[1]:
            continue
        if "time_to" in where and where["time_to"] < v.time_range[0]:
            continue
        out.append(name)
    return out


def rewrite(attributes, where=None, table="logs.log_events"):
    # type: (List[str], Optional[Dict], str) -> str
    """Viet lai truy van tren LogEvent thanh SQL tren kho OLAP, chi quet cac nguon lien quan (hop cac truy van con)."""
    where = where or {}
    srcs = select_sources(attributes, where)
    if not srcs:
        return "-- khong nguon nao tra loi duoc truy van nay"
    conds = []
    if "time_from" in where: conds.append("event_time >= '%s'" % where["time_from"])
    if "time_to" in where: conds.append("event_time < '%s'" % where["time_to"])
    extra = "".join(" AND " + c for c in conds)
    return "\nUNION ALL\n".join("SELECT %s FROM %s WHERE source = '%s'%s" % (", ".join(attributes) or "*", table, s, extra) for s in srcs)


if __name__ == "__main__":
    for v in SOURCE_VIEWS.values():
        print(v.definition()); print("   thuc the=%s, nhan theo %s (%s)" % (v.entity, v.label_granularity, v.label_origin))
    print("\nQ1: nguon nao danh gia duoc o muc dong?         ->", select_sources(["event_time", "label"], {"label_granularity": "line"}))
    print("Q2: nguon nao co log trong thang 11/2008?       ->", select_sources(["content"], {"time_from": "2008-11-01", "time_to": "2008-12-01"}))
    print("Q3: nguon nao can ghep nhan tu tep rieng?       ->", select_sources(["label"], {"label_origin": "file"}))
    print("\nViet lai Q2 thanh SQL:\n" + rewrite(["event_time", "host", "content"], {"time_from": "2008-11-01", "time_to": "2008-12-01"}))
