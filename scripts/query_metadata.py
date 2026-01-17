#!/usr/bin/env python3
"""
Query Oracle database metadata for tables extracted from parse output.
Usage: python scripts/query_metadata.py
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import yaml
from src.metadata.oracle_metadata_fetcher import OracleMetadataFetcher
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
    log_file = create_log_file_path(project_root, "metadata", logs_dir_name)
    logger = setup_logger(
        "MetadataQuery",
        log_file=log_file,
        level=config.get('logging', {}).get('level', 'INFO'),
        console_output=config.get('logging', {}).get('console_output', True),
        file_output=True
    )

    logger.info(f"Log file: {log_file}")

    # Get paths from centralized output config
    output_config = config.get('output', {})
    base_dir = project_root / output_config.get('base_dir', 'outputs')
    parse_dir = base_dir / output_config.get('parse_dir', 'parse')
    metadata_dir = base_dir / output_config.get('metadata_dir', 'metadata')

    logger.info(f"Parse input directory: {parse_dir}")
    logger.info(f"Metadata output directory: {metadata_dir}")

    # Check if parse directory exists
    if not parse_dir.exists():
        logger.error(f"Parse directory not found: {parse_dir}")
        logger.info("Run parse_sql.py first to generate parse output files")
        return 1

    # Get metadata config
    metadata_config = config.get('oracle', {}).get('metadata', {})
    default_schema = metadata_config.get('default_schema', '')

    if default_schema:
        logger.info(f"Default schema: {default_schema}")
    else:
        logger.warning("No default schema configured")

    # Initialize fetcher
    fetcher = OracleMetadataFetcher(config)

    try:
        # Create connection pool
        logger.info("Connecting to Oracle database...")
        fetcher.create_pool()

        # Query metadata from parse directory
        logger.info(f"=" * 60)
        logger.info(f"Querying metadata from parse output")
        logger.info(f"Cleaning metadata output directory: {metadata_dir}")
        logger.info(f"=" * 60)

        results = fetcher.query_metadata_from_parse_dir(
            parse_dir=parse_dir,
            output_dir=metadata_dir,
            pattern="*.json",
            clean_output=True  # Always clean output folder before querying
        )

        # Print summary
        logger.info(f"\nDetailed Summary:")
        for filename, result in results.items():
            if result["status"] == "success":
                schemas_str = ", ".join(result["schemas"])
                logger.info(f"  {filename}:")
                logger.info(f"    - Schemas: {schemas_str}")
                logger.info(f"    - Tables: {result['tables']}")
                logger.info(f"    - Views: {result['views']}")
            else:
                logger.info(f"  {filename}: FAILED - {result.get('error', 'Unknown error')}")

        logger.info(f"\n" + "=" * 60)
        logger.info(f"✓ Metadata query completed successfully!")
        logger.info(f"=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Failed to query metadata: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    finally:
        # Close connection pool
        fetcher.close_pool()


if __name__ == "__main__":
    sys.exit(main())
