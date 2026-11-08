"""Phan loai known/unknown ben trong Spark executor (goi tu mapInPandas cua job 1).

Moi Python worker giu cache rieng (module-level) gom:
  - cay Drain warm-up nap tu Redis, chi dung de TRA CUU (khong hoc them, khong ghi nguoc Redis)
  - mot cay Drain "novel" hoc cac dong khong khop warm-up de gom chung thanh template moi
  - tap promoted tai lai moi 30 giay

Tai sao thiet ke nhu vay: Drain co trang thai; neu moi executor tu hoc va ghi vao cung mot snapshot
thi cay se lech nhau va ghi de nhau. Cay warm-up dong bang + template_id bam theo noi dung giup moi
executor cho cung mot ket qua voi log da biet.
"""
import time

from drain3.redis_persistence import RedisPersistence

from registry.common import REDIS_HOST, REDIS_PORT, key, new_miner, redis_client, template_id

_CACHE = {}
PROMOTED_REFRESH_S = 30


class SourceClassifier:
    def __init__(self, source):
        self.source = source
        self.base = new_miner(RedisPersistence(REDIS_HOST, REDIS_PORT, 0, None, False, key("snapshot", source)))
        self.base.persistence_handler = None            # dong bang: khong bao gio ghi nguoc snapshot
        self.has_warmup = len(self.base.drain.clusters) > 0
        self.novel = new_miner()
        self.novel_ids = {}                             # cluster_id cua cay novel -> template_id co dinh
        self.promoted = set(); self.promoted_at = 0.0
        self.r = redis_client()

    def _refresh_promoted(self):
        now = time.time()
        if now - self.promoted_at > PROMOTED_REFRESH_S:
            self.promoted = {m.decode() for m in self.r.smembers(key("promoted", self.source))}
            self.promoted_at = now

    def classify(self, content):
        """Tra ve (template_id, template, status)."""
        d = self.base.drain
        masked = self.base.masker.mask(content)
        tokens = d.get_content_as_tokens(masked)
        c = d.tree_search(d.root_node, tokens, d.sim_th, False)   # cung nguong sim_th nhu khi hoc
        if c is not None:
            tpl = c.get_template()
            return template_id(self.source, tpl, "K"), tpl, "known"
        res = self.novel.add_log_message(content)
        cid = res["cluster_id"]
        tid = self.novel_ids.get(cid)
        if tid is None:                                  # id co dinh theo template LAN DAU thay
            tid = template_id(self.source, res["template_mined"], "U")
            self.novel_ids[cid] = tid
        self._refresh_promoted()
        return tid, res["template_mined"], ("known" if tid in self.promoted else "unknown")


def get(source):
    clf = _CACHE.get(source)
    if clf is None:
        clf = _CACHE[source] = SourceClassifier(source)
    return clf
