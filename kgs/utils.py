import asyncio
import logging
import os
import re
from pathlib import Path
from subprocess import PIPE
from subprocess import Popen
from typing import Callable
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple


async def async_cmd_exec(cmd, stdin=None):
    proc = await asyncio.create_subprocess_exec(
        cmd[0], cmd[1:], stdout=asyncio.subprocess.PIPE, stdin=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    outs, errs = await proc.communicate(stdin)
    return outs, errs, proc.returncode


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


def probe_k8s() -> Tuple[str, bool]:
    _, errs, rc = cmd_exec(["kubectl", "version"])
    if rc == 0:
        return "", True
    return errs.decode().strip(), False


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


def get_files_in_samedir(paths: Iterable[Path], filepath: Path, condition: Callable[[str], bool]) -> List[Path]:
    dir_files = [p for p in paths if list(p.parents) == list(filepath.parents)]
    return [p for p in dir_files if condition(str(p))]


def get_files_in_samedir_pattern(paths: Iterable[Path], filepath: Path, pattern: re.Pattern, name: str) -> List[Path]:
    def _(path: str) -> bool:
        m = pattern.match(path)
        return m is not None and m.group(1) == name

    return get_files_in_samedir(paths, filepath, _)


def unwrap_any(*args: Optional[re.Match]) -> re.Match:
    for opt in args:
        if opt:
            return opt
    # this function expect least one of args must have value
    raise Exception()
