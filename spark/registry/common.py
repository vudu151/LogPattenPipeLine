"""Ham dung chung cho registry: cau hinh Drain3, sinh template_id, ket noi Redis.

Quy uoc khoa Redis:
  drain3:<SRC>:warmup     snapshot cay Drain hoc trong warm-up (chi doc khi streaming)
  known:<SRC>             SET template_id hoc trong warm-up
  promoted:<SRC>          SET template_id unknown da lap lai du nhieu lan -> coi la known
  warmup:<SRC>:lines      so dong goc da dung cho warm-up (Log Producer dung --skip bang so nay)
"""
import hashlib
import os

from drain3 import TemplateMiner
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig

HERE = os.path.dirname(os.path.abspath(__file__))
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


# Tham so Drain rieng cho tung he dieu hanh, chon bang cach do Grouping Accuracy tren mau Loghub-2k.
_B, _E = r"((?<=[^A-Za-z0-9])|^)", r"((?=[^A-Za-z0-9])|$)"
MASKS = {"ip": _B + r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?)" + _E, "hex": _B + r"(0x[a-fA-F0-9]+)" + _E}
DRAIN_PARAMS = {
    "Windows": {"sim_th": 0.7, "depth": 5, "masks": ["ip", "hex"]},
    "Linux": {"sim_th": 0.39, "depth": 5, "masks": ["ip"]},
    "Mac": {"sim_th": 0.6, "depth": 6, "masks": ["ip"]},
}


def miner_config(source=None):
    cfg = TemplateMinerConfig()
    cfg.load(os.path.join(HERE, "drain3.ini"))
    cfg.profiling_enabled = False
    p = DRAIN_PARAMS.get(source)
    if p:
        cfg.drain_sim_th = p["sim_th"]; cfg.drain_depth = p["depth"]
        cfg.masking_instructions = [MaskingInstruction(MASKS[m], "*") for m in p["masks"]]
    return cfg


def new_miner(persistence=None, source=None):
    return TemplateMiner(persistence_handler=persistence, config=miner_config(source))


def template_id(source, template, prefix):
    """ID theo noi dung: cung template -> cung id tren moi executor. prefix K = known (warm-up), U = unknown."""
    return prefix + hashlib.md5((source + "|" + template).encode("utf-8")).hexdigest()[:11]


def redis_client():
    import redis
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)


def key(kind, source):
    return {"snapshot": "drain3:%s:warmup", "known": "known:%s", "promoted": "promoted:%s",
            "lines": "warmup:%s:lines"}[kind] % source
