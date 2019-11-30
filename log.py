import os
import logging


def getLogger(name):
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y/%m/%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)

    loglevel = os.environ.get("KGS_LOGLEVEL", None)
    if loglevel is not None:
        logger.setLevel(getattr(logging, loglevel))
    else:
        logger.setLevel(logging.INFO)

    return logger
