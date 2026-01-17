#!/usr/bin/env python3
"""
Parse SQL files and extract table references.
Usage: python scripts/parse_sql.py
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import yaml
from src.parsers.sql_parser import SQLTableExtractor
from src.utils.logger import setup_logger, create_log_file_path


def load_env_vars(env_file: Path = None):
    """Load environment variables from .env file."""
    if env_file is None:
        env_file = project_root / ".env"

    if not env_file.exists():
        return

    with open(env_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()


def load_config(config_file: Path) -> dict:
    """Load configuration from YAML file."""
    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    # Load environment variables from .env file
    load_env_vars()

    # Load config
    config_file = project_root / "config" / "config.yaml"
    config = load_config(config_file)

    # Setup logger with dynamic file naming
    logs_dir_name = config.get('output', {}).get('logs_dir', 'logs')
    log_file = create_log_file_path(project_root, "parse", logs_dir_name)
    logger = setup_logger(
        "SQLParser",
        log_file=log_file,
        level=config.get('logging', {}).get('level', 'INFO'),
        console_output=config.get('logging', {}).get('console_output', True),
        file_output=True
    )

    logger.info(f"Log file: {log_file}")

    # Get paths from centralized output config
    output_config = config.get('output', {})
    base_dir = project_root / output_config.get('base_dir', 'outputs')
    sql_dir = base_dir / output_config.get('sql_dir', 'fetchers/sql')
    parse_dir = base_dir / output_config.get('parse_dir', 'parse')

    logger.info(f"SQL directory: {sql_dir}")
    logger.info(f"Parse output directory: {parse_dir}")

    # Check if SQL directory exists
    if not sql_dir.exists():
        logger.error(f"SQL directory not found: {sql_dir}")
        logger.info("Run fetcher first to get SQL files")
        return 1

    # Get parser config
    parser_config = config.get('parser', {})
    dialect = parser_config.get('dialect', 'oracle')
    file_pattern = parser_config.get('file_pattern', '*.sql')

    # Initialize extractor with config for parallel processing
    extractor = SQLTableExtractor(dialect=dialect, config=config)

    # Parse all SQL files
    logger.info(f"Parsing SQL files with pattern: {file_pattern}")
    logger.info(f"Cleaning parse output directory: {parse_dir}")
    logger.info(f"=" * 60)
    results = extractor.parse_sql_directory(
        sql_dir=sql_dir,
        output_dir=parse_dir,
        pattern=file_pattern,
        clean_output=True  # Always clean output folder before parsing
    )

    # Get summary
    total_files = len(results)
    total_tables = sum(len(tables) for tables in results.values())

    # Get all unique tables
    all_tables = extractor.get_all_tables(sql_dir, file_pattern)

    logger.info(f"=" * 60)
    logger.info(f"Parse Summary:")
    logger.info(f"  Total files parsed: {total_files}")
    logger.info(f"  Total table references: {total_tables}")
    logger.info(f"  Unique tables: {len(all_tables)}")
    logger.info(f"  Results saved to: {parse_dir}")
    logger.info(f"=" * 60)

    # Print unique tables by schema
    tables_by_schema = {}
    for table in all_tables:
        schema = table['schema'] or '(default)'
        if schema not in tables_by_schema:
            tables_by_schema[schema] = []
        tables_by_schema[schema].append(table['table'])

    logger.info(f"\nUnique tables by schema:")
    for schema in sorted(tables_by_schema.keys()):
        tables = sorted(tables_by_schema[schema])
        logger.info(f"  {schema}: {len(tables)} tables")
        for table in tables[:10]:  # Show first 10
            logger.info(f"    - {table}")
        if len(tables) > 10:
            logger.info(f"    ... and {len(tables) - 10} more")

    return 0


if __name__ == "__main__":
    sys.exit(main())
