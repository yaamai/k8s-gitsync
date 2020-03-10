import os
import logging


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


def command_result_debug(logger, cmd, outs, errs):
    if os.environ.get("KGS_LOG_NO_DECODE"):
        logger.debug(f"executed: {cmd}")
        logger.debug(f"stdout: {outs}")
        logger.debug(f"stderr: {errs}")
    else:
        logger.debug(f"executed: {cmd}")
        logger.debug(f"stdout: {outs.decode()}")
        logger.debug(f"stderr: {errs.decode()}")
