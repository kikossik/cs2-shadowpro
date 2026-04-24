"""Shared logging setup for all services and pipeline steps.

Usage:
    from backend.log import get_logger
    log = get_logger("INGEST")
    log.info("processing %s", match_id)
"""
from __future__ import annotations

import logging
import sys


def get_logger(tag: str) -> logging.Logger:
    """Return a logger that prefixes every line with [TAG] and timestamps."""
    logger = logging.getLogger(tag)
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        f"%(asctime)s [{tag}] %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger
