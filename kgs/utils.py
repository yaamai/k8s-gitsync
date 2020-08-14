import logging
import os
from subprocess import PIPE
from subprocess import Popen


def cmd_exec(cmd, stdin=None):
    proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    outs, errs = proc.communicate(stdin)
    return outs, errs, proc.returncode


def _safe_get(d: dict, *args: str, default=None):
    r = d
    for k in args:
        if k not in r:
            return default
        r = r[k]
    return r


def probe_k8s():
    _, _, rc = cmd_exec(["kubectl", "version"])
    if rc == 0:
        return True
    return False


def get_logger(name):
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y/%m/%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)

    loglevel = os.environ.get("KGS_LOG_LEVEL", None)
    if loglevel is not None:
        logger.setLevel(getattr(logging, loglevel))
    else:
        logger.setLevel(logging.INFO)

    return logger
