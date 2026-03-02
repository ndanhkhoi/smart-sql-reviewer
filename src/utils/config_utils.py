"""
Configuration utilities for SQL Reviewer.
Shared functions for loading configuration and environment variables.
"""

import os
import sys
from pathlib import Path
import yaml


def load_env_vars(env_file: Path = None):
    """
    Load environment variables from .env file.

    Args:
        env_file: Path to .env file (default: project_root/.env)
    """
    if env_file is None:
        project_root = get_project_root()
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


def load_config(config_file: Path = None) -> dict:
    """
    Load configuration from YAML file.

    Args:
        config_file: Path to config YAML file (default: project_root/config/config.yaml)

    Returns:
        Configuration dictionary

    Exits:
        With error code 1 if file not found or YAML parsing fails
    """
    if config_file is None:
        project_root = get_project_root()
        config_file = project_root / "config" / "config.yaml"

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found!")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def get_project_root() -> Path:
    """
    Get the project root directory.

    Returns:
        Path to project root directory
    """
    # Try to find project root from current file location
    current_file = Path(__file__).resolve()
    # src/utils/config_utils.py -> src/utils -> src -> project_root
    return current_file.parent.parent.parent
