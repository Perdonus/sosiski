from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from config import (
    LOG_CARDS_FILE,
    LOG_DIR,
    LOG_GIVEAWAY_FILE,
    LOG_KAZIK_FILE,
    LOG_LEVEL,
    LOG_RUNTIME_FILE,
)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _ensure_parent(LOG_RUNTIME_FILE)
    _ensure_parent(LOG_KAZIK_FILE)
    _ensure_parent(LOG_CARDS_FILE)
    _ensure_parent(LOG_GIVEAWAY_FILE)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    level = getattr(logging, str(LOG_LEVEL).upper(), logging.INFO)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    runtime_handler = RotatingFileHandler(
        LOG_RUNTIME_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    runtime_handler.setFormatter(formatter)
    root.addHandler(runtime_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    for name, path in {
        "kazik": LOG_KAZIK_FILE,
        "cards": LOG_CARDS_FILE,
        "giveaway": LOG_GIVEAWAY_FILE,
    }.items():
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(level)
        handler = RotatingFileHandler(
            path,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = True
