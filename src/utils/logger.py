"""
Logging utilities for SQL Reviewer.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


def setup_logger(
    name: str,
    log_file: Optional[Path] = None,
    level: str = "INFO",
    console_output: bool = True,
    file_output: bool = True
) -> logging.Logger:
    """
    Setup a logger with console and file handlers.

    Args:
        name: Logger name
        log_file: Path to log file (optional)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        console_output: Enable console output
        file_output: Enable file output

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if file_output and log_file:
        # Create log directory if not exists
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def create_log_file_path(
    base_dir: Path,
    phase_name: str,
    logs_dir_name: str = "logs",
    timestamp: Optional[datetime] = None
) -> Path:
    """
    Create a log file path with dynamic naming based on phase and timestamp.

    Args:
        base_dir: Base directory (usually project_root)
        phase_name: Name of the phase (e.g., "fetch", "parse", "metadata")
        logs_dir_name: Name of logs directory (default: "logs")
        timestamp: Optional datetime object (defaults to current time)

    Returns:
        Path to log file in format: {logs_dir_name}/{phase_name}_{YYYYMMDD_HHMMSS}.log
    """
    if timestamp is None:
        timestamp = datetime.now()

    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    log_filename = f"{phase_name}_{timestamp_str}.log"

    # Ensure logs directory exists
    logs_dir = base_dir / logs_dir_name
    logs_dir.mkdir(parents=True, exist_ok=True)

    return logs_dir / log_filename


def get_logger(name: str) -> logging.Logger:
    """
    Get an existing logger or create a new one with default settings.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Return a basic logger if none exists
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.INFO)
    return logger
