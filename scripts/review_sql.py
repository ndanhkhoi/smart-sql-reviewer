#!/usr/bin/env python3
"""
SQL Reviewer - Review SQL using Z.ai API

Entry point script for reviewing SQL queries using Z.ai API.
This is Step 4 of the SQL Reviewer pipeline.

Prerequisites:
- Step 1: Fetch SQL from Glowroot (outputs/fetchers/sql/*.sql)
- Step 2: Parse SQL to extract table names (outputs/parse/*.json)
- Step 3: Query metadata from Oracle (outputs/metadata/*.json)
"""

import sys
import os
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from src.reviewers import ZAiSQLReviewer
from src.utils import print_section


def load_env_vars(env_file: Path = None):
    """
    Load environment variables from .env file.

    Args:
        env_file: Path to .env file (default: project_root/.env)
    """
    if env_file is None:
        env_file = Path(__file__).parent.parent / ".env"

    if not env_file.exists():
        return

    with open(env_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            # Parse KEY=VALUE
            if '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()


def load_config(config_file: str) -> dict:
    """
    Load configuration from YAML file.

    Args:
        config_file: Path to configuration file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found!")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def check_prerequisites(config: dict) -> bool:
    """
    Check if prerequisites are met.

    Args:
        config: Configuration dictionary

    Returns:
        True if prerequisites are met, False otherwise
    """
    output_config = config.get("output", {})
    base_output = Path(output_config.get("base_dir", "outputs"))
    sql_dir = base_output / output_config.get("sql_dir", "fetchers/sql")
    metadata_dir = base_output / output_config.get("metadata_dir", "metadata")

    print_section("CHECKING PREREQUISITES")

    # Check if SQL files exist
    sql_files = list(sql_dir.glob("*.sql"))
    if not sql_files:
        print(f"✗ No SQL files found in {sql_dir}")
        print("\nPlease run Step 1 first:")
        print("  python scripts/fetch_sql.py --all")
        return False
    print(f"✓ Found {len(sql_files)} SQL files")

    # Check if metadata files exist
    metadata_files = list(metadata_dir.glob("*.json"))
    if not metadata_files:
        print(f"⚠ No metadata files found in {metadata_dir}")
        print("\nNote: Review will proceed without metadata.")
        print("      For better results, run Step 3 first:")
        print("  python scripts/query_metadata.py")
    else:
        print(f"✓ Found {len(metadata_files)} metadata files")

    print()
    return True


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="SQL Reviewer - Review SQL queries using Z.ai API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Review all SQL files (requires API key in .env)
  %(prog)s

  # Review and clean previous review results
  %(prog)s --clean

  # Review only first 2 files (for testing)
  %(prog)s -n 2

  # Review only specific files by name/pattern
  %(prog)s --files query1 --files report_sql

  # Review files matching a pattern (case-insensitive)
  %(prog)s --files test

  # Use custom config
  %(prog)s -c custom_config.yaml

Prerequisites:
  - ZAI_API_KEY environment variable must be set in .env file
  - SQL files should exist in outputs/fetchers/sql/
  - Optional: Metadata files in outputs/metadata/ for better reviews

Pipeline:
  Step 1: python scripts/fetch_sql.py --all
  Step 2: python scripts/parse_sql.py
  Step 3: python scripts/query_metadata.py
  Step 4: python scripts/review_sql.py
        """
    )

    parser.add_argument(
        "-c", "--config",
        default="config/config.yaml",
        help="Path to configuration file (default: config/config.yaml)"
    )

    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean review directory before reviewing"
    )

    parser.add_argument(
        "-n", "--limit",
        type=int,
        metavar="N",
        help="Only review the first N SQL files (useful for testing)"
    )

    parser.add_argument(
        "--files",
        action="append",
        metavar="FILENAME",
        help="Review only specific SQL files by name/pattern. "
             "Can be used multiple times. Supports partial matching (case-insensitive). "
             "Example: --files query1 --files report_sql"
    )

    args = parser.parse_args()

    # Load environment variables from .env file
    load_env_vars()

    # Load configuration
    config = load_config(args.config)

    # Check prerequisites
    if not check_prerequisites(config):
        sys.exit(1)

    # Check for API key
    if not os.getenv("ZAI_API_KEY"):
        print("Error: ZAI_API_KEY environment variable not set!")
        print("\nPlease add your Z.ai API key to the .env file:")
        print("  ZAI_API_KEY=your_api_key_here")
        sys.exit(1)

    # Prepare full config for reviewer
    reviewer_config = {
        "output": config["output"],
        "logging": config["logging"],
        "review": config.get("review", {})
    }

    # Run reviewer
    try:
        reviewer = ZAiSQLReviewer(reviewer_config)
        reviewer.run(clean_output=args.clean, limit=args.limit, files_to_review=args.files)
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
