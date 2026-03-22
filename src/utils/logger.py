"""
Centralized logging utility for the BGD project.

This module provides a reusable logger configuration that writes logs to:
1) stdout (console)
2) a dedicated log file

Usage:
    from .logger import get_logger

    logger = get_logger(__name__)
    logger.info("Pipeline started")
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Default log storage
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "bgd.log"

# Reusable formatter
LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _normalize_level(level: int | str) -> int:
    """
    Normalize string/int log level to logging module constants.

    Args:
        level: Level name (e.g. "INFO") or logging constant (e.g. logging.INFO).

    Returns:
        Integer logging level.
    """
    if isinstance(level, int):
        return level

    if isinstance(level, str):
        level_upper = level.upper().strip()
        mapping = {
            "CRITICAL": logging.CRITICAL,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "WARN": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "NOTSET": logging.NOTSET,
        }
        return mapping.get(level_upper, logging.INFO)

    return logging.INFO


def configure_logging(
    *,
    logger_name: str = "bgd",
    level: int | str = "INFO",
    log_file: str | Path = LOG_FILE,
) -> logging.Logger:
    """
    Configure and return a logger with stdout + file handlers.

    This function is idempotent for a given logger name:
    if handlers already exist, they are reused.

    Args:
        logger_name: Name of the logger to configure.
        level: Logging level (str or int).
        log_file: Output path for file logs.

    Returns:
        Configured `logging.Logger` instance.
    """
    resolved_level = _normalize_level(level)
    logger = logging.getLogger(logger_name)
    logger.setLevel(resolved_level)
    logger.propagate = False

    # Avoid adding duplicate handlers on repeated imports/calls
    if logger.handlers:
        for handler in logger.handlers:
            handler.setLevel(resolved_level)
        return logger

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

    # stdout handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(resolved_level)
    stream_handler.setFormatter(formatter)

    # file handler (append mode)
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setLevel(resolved_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str, *, level: int | str = "INFO") -> logging.Logger:
    """
    Get a child logger under the root `bgd` logger namespace.

    Ensures root logger is configured once, then returns `bgd.<name>` logger.

    Args:
        name: Logger suffix, typically `__name__`.
        level: Logging level for initialization.

    Returns:
        Configured child logger.
    """
    root = configure_logging(logger_name="bgd", level=level)
    child_name = f"{root.name}.{name}" if name else root.name
    child = logging.getLogger(child_name)
    child.setLevel(_normalize_level(level))
    child.propagate = True
    return child
