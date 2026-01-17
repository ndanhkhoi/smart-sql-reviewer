"""
Helper utilities for SQL Reviewer.
"""

import re
from pathlib import Path
from typing import Any, Dict
from datetime import datetime, timedelta


def sanitize_filename(name: str) -> str:
    """
    Sanitize string for use in filename.

    Args:
        name: String to sanitize

    Returns:
        Sanitized string safe for filenames
    """
    # Replace invalid characters with underscore
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, "_")
    # Also replace multiple spaces with single underscore
    name = re.sub(r'\s+', '_', name)
    return name


def get_time_range_hours_ago(hours: int) -> tuple[int, int]:
    """
    Calculate time range from X hours ago to now.

    Args:
        hours: Number of hours ago

    Returns:
        Tuple of (from_ms, to_ms) - milliseconds since epoch
    """
    to_time = datetime.now()
    from_time = to_time - timedelta(hours=hours)

    # Convert to milliseconds since epoch
    from_ms = int(from_time.timestamp() * 1000)
    to_ms = int(to_time.timestamp() * 1000)

    return from_ms, to_ms


def format_duration(nanos: int) -> str:
    """
    Format duration from nanoseconds to human-readable string.

    Args:
        nanos: Duration in nanoseconds

    Returns:
        Formatted duration string
    """
    seconds = nanos / 1_000_000_000
    if seconds < 1:
        return f"{seconds * 1000:.2f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = seconds / 60
        return f"{minutes:.2f}m"


def format_number(num: int) -> str:
    """
    Format number with thousand separators.

    Args:
        num: Number to format

    Returns:
        Formatted number string
    """
    return f"{num:,}"


def ensure_dir(path: Path) -> Path:
    """
    Ensure directory exists, create if not.

    Args:
        path: Directory path

    Returns:
        Path object
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def print_separator(char: str = "=", length: int = 70):
    """
    Print a separator line.

    Args:
        char: Character to use for separator
        length: Length of separator
    """
    print(char * length)


def print_section(title: str, char: str = "=", length: int = 70):
    """
    Print a section header with separator.

    Args:
        title: Section title
        char: Separator character
        length: Line length
    """
    print_separator(char, length)
    print(title)
    print_separator(char, length)


def safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Safely get value from dictionary.

    Args:
        data: Dictionary to get value from
        key: Key to look up
        default: Default value if key not found

    Returns:
        Value or default
    """
    return data.get(key, default)
