"""
Metadata utilities for SQL Reviewer.
Shared functions for metadata extraction and processing.
"""

from typing import Any, Optional
from datetime import datetime
from dataclasses import asdict


def clean_value(value: Any) -> Any:
    """
    Clean database values for JSON serialization.

    Args:
        value: Value to clean

    Returns:
        Cleaned value (None or converted to string)
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def dataclass_to_dict(obj: Any) -> dict:
    """
    Convert dataclass instance to dictionary, cleaning None values.

    Args:
        obj: Dataclass instance

    Returns:
        Dictionary representation
    """
    if hasattr(obj, '__dataclass_fields__'):
        return asdict(obj)
    return obj


def build_dsn(host: str, port: int, service_name: Optional[str] = None,
              sid: Optional[str] = None) -> str:
    """
    Build Oracle Data Source Name (DSN).

    Args:
        host: Database host
        port: Database port
        service_name: Oracle service name
        sid: Oracle SID

    Returns:
        DSN string

    Raises:
        ValueError: If neither service_name nor sid is provided
    """
    if service_name:
        return f"{host}:{port}/{service_name}"
    elif sid:
        return f"{host}:{port}:{sid}"
    else:
        raise ValueError("Either service_name or sid must be provided")


def safe_int(value: Any) -> Optional[int]:
    """
    Safely convert value to int.

    Args:
        value: Value to convert

    Returns:
        Integer or None if conversion fails
    """
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
