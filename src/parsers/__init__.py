"""
SQL parsers for SQL Reviewer.
"""

from src.parsers.sql_parser import (
    SQLTableExtractor,
    parse_sql_files,
)

__all__ = [
    "SQLTableExtractor",
    "parse_sql_files",
]
