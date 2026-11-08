"""Ham dung chung cho registry: cau hinh Drain3, sinh template_id, ket noi Redis.

Quy uoc khoa Redis:
  drain3:<SRC>:warmup     snapshot cay Drain hoc trong warm-up (chi doc khi streaming)
  known:<SRC>             SET template_id hoc trong warm-up
  promoted:<SRC>          SET template_id unknown da lap lai du nhieu lan -> coi la known
  warmup:<SRC>:lines      so dong goc da dung cho warm-up (replayer dung --skip bang so nay)
"""
import hashlib
import os

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

HERE = os.path.dirname(os.path.abspath(__file__))
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def miner_config():
    cfg = TemplateMinerConfig()
    cfg.load(os.path.join(HERE, "drain3.ini"))
    cfg.profiling_enabled = False
    return cfg


def new_miner(persistence=None):
    return TemplateMiner(persistence_handler=persistence, config=miner_config())


def template_id(source, template, prefix):
    """ID theo noi dung: cung template -> cung id tren moi executor. prefix K = known (warm-up), U = unknown."""
    return prefix + hashlib.md5((source + "|" + template).encode("utf-8")).hexdigest()[:11]


def redis_client():
    import redis
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)


def key(kind, source):
    return {"snapshot": "drain3:%s:warmup", "known": "known:%s", "promoted": "promoted:%s",
            "lines": "warmup:%s:lines"}[kind] % source
