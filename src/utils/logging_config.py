from __future__ import annotations

import logging
from typing import Optional


def setup_logging(level: int = logging.INFO, name: str = "pipeline") -> logging.Logger:
    """
    Create (or return) a console logger used across the project.

    Tests expect:
      logger = setup_logging()
      assert logger.name == "pipeline"
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # avoid duplicate logs if root logger is configured

    # Add handler only once
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        handler.setLevel(level)
        logger.addHandler(handler)

    return logger
