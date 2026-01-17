#!/usr/bin/env python3
"""
Clean all outputs and logs from the SQL Reviewer project.
Usage: python scripts/clean.py [--all] [--outputs] [--logs]
"""

import sys
import os
import shutil
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def load_config():
    """Load configuration to get output directories."""
    import yaml
    config_file = project_root / "config" / "config.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def clean_outputs(config, dry_run=False):
    """Clean output directories but preserve .gitkeep files."""
    output_config = config.get('output', {})
    base_dir = project_root / output_config.get('base_dir', 'outputs')

    print("\n" + "=" * 70)
    print("Cleaning Outputs")
    print("=" * 70)

    total_files = 0

    # Clean all subdirectories in outputs, but preserve .gitkeep files
    if base_dir.exists():
        for item in base_dir.iterdir():
            if item.is_dir():
                # Get all files in this directory (not .gitkeep)
                files_to_delete = [f for f in item.rglob('*') if f.is_file() and f.name != '.gitkeep']

                file_count = len(files_to_delete)

                if dry_run:
                    if file_count > 0:
                        print(f"  Would delete: {file_count} files in {item.name}/")
                        # Show some examples
                        for f in files_to_delete[:3]:
                            print(f"    - {f.name}")
                        if file_count > 3:
                            print(f"    ... and {file_count - 3} more")
                else:
                    # Delete all non-.gitkeep files
                    for f in files_to_delete:
                        f.unlink()

                    # Re-create .gitkeep if it doesn't exist
                    gitkeep_file = item / '.gitkeep'
                    if not gitkeep_file.exists():
                        gitkeep_file.write_text('# Keep this directory in git\n')

                    if file_count > 0:
                        print(f"  ✓ Deleted: {file_count} files in {item.name}/ (preserved .gitkeep)")

                total_files += file_count

    if dry_run:
        print(f"\n  Total: {total_files} files (dry run)")
    else:
        print(f"\n  ✓ Total: {total_files} files deleted (directories preserved)")

    return total_files


def clean_logs(config, dry_run=False):
    """Clean log files."""
    logs_dir_name = config.get('output', {}).get('logs_dir', 'logs')
    logs_dir = project_root / logs_dir_name

    print("\n" + "=" * 70)
    print("Cleaning Logs")
    print("=" * 70)

    total_files = 0

    if logs_dir.exists():
        log_files = list(logs_dir.glob('*.log'))

        for log_file in log_files:
            if dry_run:
                print(f"  Would delete: {log_file.name}")
            else:
                log_file.unlink()
                print(f"  ✓ Deleted: {log_file.name}")

            total_files += 1

    if dry_run:
        print(f"\n  Total: {total_files} log files (dry run)")
    else:
        print(f"\n  ✓ Total: {total_files} log files deleted")

    return total_files


def clean_all(config, dry_run=False):
    """Clean both outputs and logs."""
    output_files = clean_outputs(config, dry_run)
    log_files = clean_logs(config, dry_run)

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    if dry_run:
        print(f"Would delete:")
        print(f"  - {output_files} output files (directories preserved)")
        print(f"  - {log_files} log files")
        print(f"\nTotal: {output_files + log_files} files (dry run)")
    else:
        print(f"Deleted:")
        print(f"  - {output_files} output files (directories preserved)")
        print(f"  - {log_files} log files")
        print(f"\n✓ Total: {output_files + log_files} files deleted")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Clean SQL Reviewer outputs and logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/clean.py --all        Clean everything (outputs + logs)
  python scripts/clean.py --outputs    Clean only outputs
  python scripts/clean.py --logs       Clean only logs
  python scripts/clean.py --all --dry-run  Preview what would be deleted
        """
    )

    parser.add_argument('--all', action='store_true', help='Clean both outputs and logs')
    parser.add_argument('--outputs', action='store_true', help='Clean only outputs')
    parser.add_argument('--logs', action='store_true', help='Clean only logs')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')

    args = parser.parse_args()

    # Default to --all if no specific option provided
    if not (args.all or args.outputs or args.logs):
        args.all = True

    # Load config
    config = load_config()

    if args.dry_run:
        print("\n" + "=" * 70)
        print("DRY RUN MODE - No files will be deleted")
        print("=" * 70)

    # Execute cleaning
    if args.all:
        clean_all(config, args.dry_run)
    elif args.outputs:
        clean_outputs(config, args.dry_run)
    elif args.logs:
        clean_logs(config, args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
