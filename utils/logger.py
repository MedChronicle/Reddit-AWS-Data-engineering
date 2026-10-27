"""Tiny logging helper so every module logs the same way (and honours LOG_LEVEL)."""
from __future__ import annotations

import logging
import sys

from utils.constants import LOG_LEVEL

_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    # Inside Airflow the root logger already has handlers (they end up in the task log).
    # When run standalone (tests, scripts) add a console handler so output is not lost.
    if not logging.getLogger().handlers and not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
    return logger
