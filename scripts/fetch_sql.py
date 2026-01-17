#!/usr/bin/env python3
"""
SQL Reviewer - Fetch SQL from Glowroot APM

Entry point script for fetching SQL queries from Glowroot.
This is Step 1 of the SQL Reviewer pipeline.

Transactions are auto-discovered from Glowroot API - no need to configure them manually.
"""

import sys
import os
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from src.fetchers import GlowrootSQLFetcher
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


def list_agents(config: dict):
    """List all configured agents."""
    agents = config.get("glowroot", {}).get("agents", [])

    if not agents:
        print("No agents configured in config file.")
        return

    print_section("CONFIGURED AGENTS")
    print(f"\nTotal agents: {len(agents)}")
    print("Transactions are auto-discovered from Glowroot API\n")

    for idx, agent in enumerate(agents, 1):
        agent_id = agent.get("agent_id", "Unknown")
        print(f"  [{idx}] {agent_id}")

    print()


def interactive_select_agent(config: dict) -> str:
    """
    Interactively select agent.

    Args:
        config: Configuration dictionary

    Returns:
        Selected agent_id or "ALL" for all agents, None if cancelled
    """
    agents = config.get("glowroot", {}).get("agents", [])

    if not agents:
        print("No agents configured in config file.")
        return None

    print_section("SQL Reviewer - Interactive Mode")

    # Select agent
    print("\nSelect Agent:")
    print("  [0] All agents")
    for idx, agent in enumerate(agents, 1):
        agent_id = agent.get("agent_id", "Unknown")
        print(f"  [{idx}] {agent_id}")

    while True:
        try:
            choice = input("\nEnter agent number (or 'q' to quit): ").strip()

            if choice.lower() == 'q':
                return None

            agent_idx = int(choice)

            if agent_idx == 0:
                # All agents
                return "ALL"
            elif 1 <= agent_idx <= len(agents):
                selected_agent = agents[agent_idx - 1]
                return selected_agent["agent_id"]
            else:
                print(f"Invalid choice. Please enter a number between 0 and {len(agents)}")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\n\nCancelled.")
            return None


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="SQL Reviewer - Fetch SQL queries from Glowroot APM (with auto-discovery)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  %(prog)s

  # List all configured agents
  %(prog)s --list

  # Fetch all agents (transactions auto-discovered)
  %(prog)s --all

  # Fetch specific agent
  %(prog)s --agent cto-common-service

  # Fetch with custom time range
  %(prog)s --all --hours 12

  # Use custom config
  %(prog)s -c custom_config.yaml --all

Note: Transactions are automatically discovered from Glowroot API.
      No need to configure them manually.
        """
    )

    parser.add_argument(
        "-c", "--config",
        default="config/config.yaml",
        help="Path to configuration file (default: config/config.yaml)"
    )

    parser.add_argument(
        "-l", "--list",
        action="store_true",
        help="List all configured agents"
    )

    parser.add_argument(
        "-a", "--all",
        action="store_true",
        help="Fetch all agents (transactions auto-discovered)"
    )

    parser.add_argument(
        "--agent",
        metavar="AGENT_ID",
        help="Filter by agent ID"
    )

    parser.add_argument(
        "--hours",
        type=int,
        metavar="HOURS",
        help="Override hours_ago from config"
    )

    args = parser.parse_args()

    # Load environment variables from .env file
    load_env_vars()

    # Load configuration
    config = load_config(args.config)

    # Override hours_ago if specified
    if args.hours:
        config["glowroot"]["hours_ago"] = args.hours

    # List mode
    if args.list:
        list_agents(config)
        return

    # Prepare full config for fetcher (merge glowroot, output, logging)
    fetcher_config = {
        **config["glowroot"],
        "output": config["output"],
        "logging": config["logging"]
    }

    # All mode
    if args.all:
        print("Fetching all agents (transactions will be auto-discovered)...\n")
        fetcher = GlowrootSQLFetcher(fetcher_config)
        fetcher.run()
        return

    # Filter mode
    if args.agent:
        print(f"Fetching agent: {args.agent} (transactions will be auto-discovered)...\n")
        fetcher = GlowrootSQLFetcher(fetcher_config)
        fetcher.run_filtered(args.agent, None)
        return

    # Interactive mode (default)
    agent_filter = interactive_select_agent(config)

    if agent_filter is None:
        # User cancelled
        print("\nNo selection made. Exiting.")
        return
    elif agent_filter == "ALL":
        # User selected all agents
        print("Fetching all agents (transactions will be auto-discovered)...\n")
        fetcher = GlowrootSQLFetcher(fetcher_config)
        fetcher.run()
    else:
        # User selected specific agent
        print(f"Fetching agent: {agent_filter} (transactions will be auto-discovered)...\n")
        fetcher = GlowrootSQLFetcher(fetcher_config)
        fetcher.run_filtered(agent_filter, None)


if __name__ == "__main__":
    main()
