"""
Glowroot SQL Fetcher - Fetches SQL queries from Glowroot APM tool.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os

import requests

from ..utils import (
    setup_logger,
    sanitize_filename,
    get_time_range_hours_ago,
    ensure_dir,
    print_section,
    format_duration,
    format_number,
    create_log_file_path
)


class GlowrootSQLFetcher:
    """
    Fetches SQL queries from Glowroot APM for specified agents and transactions.

    Attributes:
        config: Configuration dictionary
        base_url: Base URL for Glowroot API
        hours_ago: Number of hours ago to fetch queries from
        agents: List of agents and their transactions
        sql_dir: Directory to save SQL files
        sql_info_dir: Directory to save query info files
        logger: Logger instance
        stats: Statistics about the fetch operation
    """

    def __init__(self, config: Dict):
        """
        Initialize the fetcher with configuration.

        Args:
            config: Configuration dictionary containing:
                - base_url: Base URL for Glowroot API
                - hours_ago: Number of hours ago to fetch queries
                - agents: List of agents (transactions auto-discovered)
                - transaction_discovery: Settings for auto-discovery
                - output: Output configuration
                - logging: Logging configuration
        """
        self.config = config
        # Read base_url from environment variable or config
        self.base_url = os.getenv("GLOWROOT_BASE_URL", config.get("base_url", "http://apm.his4.local")).rstrip("/")
        self.hours_ago = config.get("hours_ago", 24)
        self.agents = config.get("agents", [])

        # Transaction discovery config
        discovery_config = config.get("transaction_discovery", {})
        self.initial_limit = discovery_config.get("initial_limit", 200)
        self.limit_increment = discovery_config.get("limit_increment", 200)
        self.max_limit = discovery_config.get("max_limit", 5000)

        # Setup output directories from centralized config
        output_config = config.get("output", {})
        base_output = Path(output_config.get("base_dir", "outputs"))
        self.sql_dir = base_output / output_config.get("sql_dir", "fetchers/sql")
        self.sql_info_dir = base_output / output_config.get("sql_info_dir", "fetchers/sql_info")

        # Setup logging with dynamic file naming
        # logs_dir is relative to project root
        project_root = base_output.parent
        logs_dir_name = output_config.get("logs_dir", "logs")
        self.log_file = create_log_file_path(project_root, "fetch", logs_dir_name)
        self.logger = setup_logger(
            "GlowrootSQLFetcher",
            log_file=self.log_file,
            level=config["logging"].get("level", "INFO"),
            console_output=config["logging"].get("console_output", True),
            file_output=config["logging"].get("file_output", True)
        )

        self.logger.info(f"Log file: {self.log_file}")

        # Parallel processing config
        parallel_config = config.get("parallel", {})
        self.parallel_enabled = parallel_config.get("enabled", False)
        self.max_workers = parallel_config.get("max_workers", 10)

        # Statistics with thread lock for parallel processing
        self.stats = {
            "total_transactions": 0,
            "total_queries": 0,
            "full_text_fetched": 0,
            "truncated_queries": 0,
            "failed_queries": 0,
            "files_written": 0,
            "empty_transactions": 0,  # Track transactions with no queries
            "saved_queries": set()  # Track unique queries by filename to detect duplicates
        }
        self.stats_lock = threading.Lock()

    def _fetch_transaction_summaries(
        self,
        agent_id: str,
        from_ms: int,
        to_ms: int
    ) -> List[Dict]:
        """
        Fetch all transaction summaries for an agent using pagination.

        Args:
            agent_id: Agent ID in Glowroot
            from_ms: Start time in milliseconds since epoch
            to_ms: End time in milliseconds since epoch

        Returns:
            List of transaction dictionaries with keys:
            - transactionName: str
            - transactionType: str (default: "Web")
            - totalDurationNanos: float
            - totalCpuNanos: float
            - totalAllocatedBytes: float
            - transactionCount: int
        """
        all_transactions = []
        limit = self.initial_limit

        while limit <= self.max_limit:
            params = {
                "agent-rollup-id": agent_id,
                "transaction-type": "Web",
                "from": from_ms,
                "to": to_ms,
                "sort-order": "total-time",
                "limit": limit
            }

            url = f"{self.base_url}/backend/transaction/summaries?{urlencode(params, quote_via=quote)}"

            self.logger.debug(f"Fetching transaction summaries with limit={limit}")
            self.logger.debug(f"URL: {url}")

            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                data = response.json()

                transactions = data.get("transactions", [])
                more_available = data.get("moreAvailable", False)

                self.logger.info(f"Fetched {len(transactions)} transactions (moreAvailable: {more_available})")

                # Add default transactionType to each transaction
                for tran in transactions:
                    tran["transactionType"] = "Web"

                all_transactions.extend(transactions)

                if not more_available:
                    self.logger.info(f"Fetched all {len(all_transactions)} transactions for agent {agent_id}")
                    break

                # Increase limit for next request
                limit += self.limit_increment

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Failed to fetch transaction summaries: {e}")
                break

        return all_transactions

    def _fetch_queries(
        self,
        agent_id: str,
        transaction_type: str,
        transaction_name: str,
        from_ms: int,
        to_ms: int
    ) -> List[Dict]:
        """
        Fetch queries from Glowroot API for a specific transaction.

        Args:
            agent_id: Agent ID in Glowroot
            transaction_type: Transaction type (e.g., "Web")
            transaction_name: Transaction name/endpoint
            from_ms: Start time in milliseconds since epoch
            to_ms: End time in milliseconds since epoch

        Returns:
            List of query dictionaries
        """
        params = {
            "agent-rollup-id": agent_id,
            "transaction-type": transaction_type,
            "transaction-name": transaction_name,
            "from": from_ms,
            "to": to_ms
        }

        url = f"{self.base_url}/backend/transaction/queries?{urlencode(params, quote_via=quote)}"

        self.logger.info(f"Fetching queries for: {agent_id} / {transaction_name}")
        self.logger.debug(f"URL: {url}")

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch queries: {e}")
            return []

    def _fetch_full_query_text(self, agent_id: str, sha1: str) -> Optional[str]:
        """
        Fetch full query text by SHA1 hash.

        Args:
            agent_id: Agent ID in Glowroot
            sha1: SHA1 hash of the full query text

        Returns:
            Full query text or None if failed
        """
        params = {
            "agent-rollup-id": agent_id,
            "full-text-sha1": sha1
        }

        url = f"{self.base_url}/backend/transaction/full-query-text?{urlencode(params)}"

        self.logger.debug(f"Fetching full query text for SHA1: {sha1}")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("fullText")
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Failed to fetch full query text for SHA1 {sha1}: {e}")
            return None

    def _save_query(
        self,
        sql_text: str,
        agent_id: str,
        transaction_name: str,
        query_num: int,
        query_info: Dict
    ) -> bool:
        """
        Save query to SQL file and query info to JSON file.

        Args:
            sql_text: Full SQL query text
            agent_id: Agent ID
            transaction_name: Transaction name
            query_num: Query number (for filename)
            query_info: Query information dictionary

        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # Sanitize names for filename
            safe_agent = sanitize_filename(agent_id)
            safe_tran = sanitize_filename(transaction_name)

            # Generate filenames
            sql_filename = f"{safe_agent}__{safe_tran}__{query_num}.sql"
            info_filename = f"{safe_agent}__{safe_tran}__{query_num}.json"

            # Check for duplicate filename (thread-safe)
            with self.stats_lock:
                if sql_filename in self.stats["saved_queries"]:
                    self.logger.warning(f"Duplicate filename detected: {sql_filename} - skipping write")
                    return False
                # Mark as saved BEFORE writing to prevent race conditions
                self.stats["saved_queries"].add(sql_filename)

            sql_path = self.sql_dir / sql_filename
            info_path = self.sql_info_dir / info_filename

            # Write SQL file
            with open(sql_path, "w", encoding="utf-8") as f:
                f.write(sql_text)
            self.logger.debug(f"Saved SQL: {sql_filename}")

            # Write info file
            info_data = {
                "agent_id": agent_id,
                "transaction_name": transaction_name,
                "transaction_type": query_info.get("transaction_type"),
                "query_number": query_num,
                "query_type": query_info.get("queryType"),
                "total_duration_nanos": query_info.get("totalDurationNanos"),
                "execution_count": query_info.get("executionCount"),
                "total_rows": query_info.get("totalRows"),
                "full_query_text_sha1": query_info.get("fullQueryTextSha1"),
                "timestamp": datetime.now().isoformat()
            }

            with open(info_path, "w", encoding="utf-8") as f:
                json.dump(info_data, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"Saved info: {info_filename}")

            self.stats["files_written"] += 2
            return True

        except IOError as e:
            self.logger.error(f"Failed to save query files: {e}")
            with self.stats_lock:
                self.stats["failed_queries"] += 1
            return False

    def _increment_stat(self, stat_name: str, value: int = 1):
        """Thread-safe stat increment."""
        with self.stats_lock:
            self.stats[stat_name] += value

    def _fetch_and_save_single_query(
        self,
        agent_id: str,
        query: Dict,
        transaction_type: str,
        transaction_name: str,
        query_idx: int
    ) -> Tuple[bool, str]:
        """
        Fetch full text and save a single query (thread-safe).

        Args:
            agent_id: Agent ID
            query: Query dictionary from Glowroot
            transaction_type: Transaction type
            transaction_name: Transaction name
            query_idx: Query index for logging

        Returns:
            Tuple of (success: bool, message: str)
        """
        query_type = query.get("queryType")
        truncated_text = query.get("truncatedQueryText", "")
        full_sha1 = query.get("fullQueryTextSha1")

        self._increment_stat("total_queries")

        # Get full SQL text
        if full_sha1:
            full_text = self._fetch_full_query_text(agent_id, full_sha1)
            if full_text:
                sql_text = full_text
                self._increment_stat("full_text_fetched")
                self.logger.debug(f"Query {query_idx}: Fetched full text ({len(sql_text)} chars)")
                success = True
                message = f"Fetched full text ({len(sql_text)} chars)"
            else:
                sql_text = truncated_text
                self._increment_stat("truncated_queries")
                self.logger.warning(f"Query {query_idx}: Using truncated text (full fetch failed)")
                success = False
                message = "Using truncated text (full fetch failed)"
        else:
            sql_text = truncated_text
            self._increment_stat("truncated_queries")
            self.logger.debug(f"Query {query_idx}: No SHA1, using truncated text ({len(sql_text)} chars)")
            success = False
            message = f"No SHA1, using truncated text ({len(sql_text)} chars)"

        # Add transaction_type to query info for saving
        query["transaction_type"] = transaction_type

        # Save query
        if self._save_query(sql_text, agent_id, transaction_name, query_idx, query):
            return (True, message)
        else:
            return (False, "Failed to save query")

    def _process_transaction(
        self,
        agent_id: str,
        transaction_type: str,
        transaction_name: str,
        from_ms: int,
        to_ms: int
    ):
        """
        Process a single transaction and fetch all its queries.

        Args:
            agent_id: Agent ID
            transaction_type: Transaction type
            transaction_name: Transaction name
            from_ms: Start time in milliseconds
            to_ms: End time in milliseconds
        """
        self.logger.info(f"Processing: {agent_id} | {transaction_type} | {transaction_name}")

        # Fetch queries
        queries = self._fetch_queries(agent_id, transaction_type, transaction_name, from_ms, to_ms)

        if not queries:
            self.logger.warning(f"No queries found for {transaction_name}")
            with self.stats_lock:
                self.stats["empty_transactions"] += 1
            return

        total_queries = len(queries)
        self.logger.info(f"Found {total_queries} queries")

        if self.parallel_enabled and total_queries > 1:
            self.logger.info(f"Parallel processing enabled with {self.max_workers} workers")
            self._process_queries_parallel(agent_id, transaction_type, transaction_name, queries)
        else:
            self.logger.info("Sequential processing")
            self._process_queries_sequential(agent_id, transaction_type, transaction_name, queries)

        self._increment_stat("total_transactions")

    def _process_queries_sequential(
        self,
        agent_id: str,
        transaction_type: str,
        transaction_name: str,
        queries: List[Dict]
    ):
        """Process queries sequentially."""
        for idx, query in enumerate(queries, 1):
            success, message = self._fetch_and_save_single_query(
                agent_id, query, transaction_type, transaction_name, idx
            )
            if success:
                self.logger.info(f"[{idx}/{len(queries)}] ✓ {message}")
            else:
                self.logger.warning(f"[{idx}/{len(queries)}] ⚠ {message}")

    def _process_queries_parallel(
        self,
        agent_id: str,
        transaction_type: str,
        transaction_name: str,
        queries: List[Dict]
    ):
        """Process queries in parallel using ThreadPoolExecutor."""
        total_queries = len(queries)
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="glowroot_fetch") as executor:
            # Submit all tasks
            future_to_query = {
                executor.submit(
                    self._fetch_and_save_single_query,
                    agent_id, query, transaction_type, transaction_name, idx
                ): idx
                for idx, query in enumerate(queries, 1)
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_query):
                idx = future_to_query[future]
                try:
                    success, message = future.result()
                    results[idx] = (success, message)

                    if success:
                        self.logger.info(f"[{idx}/{total_queries}] ✓ {message}")
                    else:
                        self.logger.warning(f"[{idx}/{total_queries}] ⚠ {message}")

                except Exception as e:
                    results[idx] = (False, str(e))
                    self.logger.error(f"[{idx}/{total_queries}] ✗ Error: {e}")

        # Log completion
        success_count = sum(1 for success, _ in results.values() if success)
        self.logger.info(f"Completed: {success_count}/{total_queries} queries processed successfully")

    def _cleanup_output(self):
        """
        Clean output directories before fetching.

        Removes all files from SQL and SQL info directories.
        Cleanup is always performed by default.
        """
        self.logger.info("Cleaning output directories...")

        # Clean SQL directory
        if self.sql_dir.exists():
            files = list(self.sql_dir.glob("*.sql"))
            for file in files:
                file.unlink()
            self.logger.info(f"Cleaned {len(files)} SQL files")

        # Clean SQL info directory
        if self.sql_info_dir.exists():
            files = list(self.sql_info_dir.glob("*.json"))
            for file in files:
                file.unlink()
            self.logger.info(f"Cleaned {len(files)} info files")

    def run(self):
        """
        Main execution method - fetches all queries from all configured agents.

        Creates output directories, calculates time range, auto-discovers transactions,
        processes all agents and transactions, and prints summary statistics.
        """
        print_section("SQL Reviewer - Glowroot Fetcher")

        # Create output directories
        ensure_dir(self.sql_dir)
        ensure_dir(self.sql_info_dir)

        # Cleanup output if enabled
        self._cleanup_output()

        self.logger.info(f"Output directory: {self.sql_dir.parent.absolute()}")

        # Get time range
        from_ms, to_ms = get_time_range_hours_ago(self.hours_ago)
        from_time_str = datetime.fromtimestamp(from_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        to_time_str = datetime.fromtimestamp(to_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

        self.logger.info(f"Time range: {from_time_str} to {to_time_str}")
        self.logger.info(f"Hours ago: {self.hours_ago}")
        self.logger.info(f"Agents configured: {len(self.agents)}")
        print("-" * 70)

        # Process each agent and auto-discover transactions
        for agent in self.agents:
            agent_id = agent["agent_id"]

            self.logger.info(f"\n" + "=" * 70)
            self.logger.info(f"Agent: {agent_id}")
            self.logger.info("=" * 70)

            # Auto-discover transactions
            self.logger.info(f"Discovering transactions for {agent_id}...")
            transactions = self._fetch_transaction_summaries(agent_id, from_ms, to_ms)

            if not transactions:
                self.logger.warning(f"No transactions found for agent {agent_id}")
                continue

            # Deduplicate transactions by transaction name (API returns duplicates)
            original_count = len(transactions)
            unique_transactions = {}
            for tran in transactions:
                tran_name = tran.get("transactionName")
                if tran_name not in unique_transactions:
                    unique_transactions[tran_name] = tran

            transactions = list(unique_transactions.values())
            duplicates_removed = original_count - len(transactions)
            if duplicates_removed > 0:
                self.logger.info(f"Removed {duplicates_removed} duplicate transactions")

            self.logger.info(f"Processing {len(transactions)} unique transactions")

            # Process each transaction
            for idx, tran in enumerate(transactions, 1):
                tran_type = tran.get("transactionType", "Web")
                tran_name = tran.get("transactionName")

                self.logger.info(f"\n[{idx}/{len(transactions)}] Processing: {tran_type} | {tran_name}")
                self._process_transaction(agent_id, tran_type, tran_name, from_ms, to_ms)

        # Print summary
        self._print_summary()

    def run_filtered(self, agent_filter: Optional[str] = None, transaction_filter: Optional[str] = None):
        """
        Run fetcher with optional filters for specific agent or transaction.

        Args:
            agent_filter: Filter by agent ID (None = all agents)
            transaction_filter: Filter by transaction name (None = all transactions)
                               Note: transaction_filter requires agent_filter
        """
        print_section("SQL Reviewer - Glowroot Fetcher")

        # Create output directories
        ensure_dir(self.sql_dir)
        ensure_dir(self.sql_info_dir)

        # Cleanup output if enabled
        self._cleanup_output()

        self.logger.info(f"Output directory: {self.sql_dir.parent.absolute()}")

        # Get time range
        from_ms, to_ms = get_time_range_hours_ago(self.hours_ago)
        from_time_str = datetime.fromtimestamp(from_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        to_time_str = datetime.fromtimestamp(to_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

        self.logger.info(f"Time range: {from_time_str} to {to_time_str}")
        self.logger.info(f"Hours ago: {self.hours_ago}")

        # Log filters
        if agent_filter:
            self.logger.info(f"Agent filter: {agent_filter}")
        if transaction_filter:
            self.logger.info(f"Transaction filter: {transaction_filter}")

        print("-" * 70)

        # Filter agents
        agents_to_process = self.agents
        if agent_filter:
            agents_to_process = [a for a in self.agents if a["agent_id"] == agent_filter]
            if not agents_to_process:
                self.logger.error(f"Agent '{agent_filter}' not found in configuration")
                return

        # Process each agent
        for agent in agents_to_process:
            agent_id = agent["agent_id"]

            self.logger.info(f"\n" + "=" * 70)
            self.logger.info(f"Agent: {agent_id}")
            self.logger.info("=" * 70)

            # Auto-discover transactions
            self.logger.info(f"Discovering transactions for {agent_id}...")
            transactions = self._fetch_transaction_summaries(agent_id, from_ms, to_ms)

            if not transactions:
                self.logger.warning(f"No transactions found for agent {agent_id}")
                continue

            # Deduplicate transactions by transaction name (API returns duplicates)
            original_count = len(transactions)
            unique_transactions = {}
            for tran in transactions:
                tran_name = tran.get("transactionName")
                if tran_name not in unique_transactions:
                    unique_transactions[tran_name] = tran

            transactions = list(unique_transactions.values())
            duplicates_removed = original_count - len(transactions)
            if duplicates_removed > 0:
                self.logger.info(f"Removed {duplicates_removed} duplicate transactions")

            # Filter transactions if specified
            if transaction_filter:
                transactions = [t for t in transactions if t.get("transactionName") == transaction_filter]
                if not transactions:
                    self.logger.warning(f"Transaction '{transaction_filter}' not found for agent '{agent_id}'")
                    continue

            self.logger.info(f"Processing {len(transactions)} unique transactions")

            # Process each transaction
            for idx, tran in enumerate(transactions, 1):
                tran_type = tran.get("transactionType", "Web")
                tran_name = tran.get("transactionName")

                self.logger.info(f"\n[{idx}/{len(transactions)}] Processing: {tran_type} | {tran_name}")
                self._process_transaction(agent_id, tran_type, tran_name, from_ms, to_ms)

        # Print summary
        self._print_summary()

    def _print_summary(self):
        """Print execution summary with statistics."""
        print_section("SUMMARY")
        self.logger.info(f"Total transactions processed: {self.stats['total_transactions']}")
        self.logger.info(f"Empty transactions (no queries): {self.stats['empty_transactions']}")
        self.logger.info(f"Total queries found: {format_number(self.stats['total_queries'])}")
        self.logger.info(f"Unique queries saved: {format_number(len(self.stats['saved_queries']))}")
        self.logger.info(f"Full text queries: {format_number(self.stats['full_text_fetched'])}")
        self.logger.info(f"Truncated queries: {format_number(self.stats['truncated_queries'])}")
        self.logger.info(f"Failed queries: {format_number(self.stats['failed_queries'])}")
        self.logger.info(f"Files written: {format_number(self.stats['files_written'])}")

        # Calculate duplicate queries
        duplicate_queries = self.stats['total_queries'] - len(self.stats['saved_queries'])
        if duplicate_queries > 0:
            self.logger.warning(f"Duplicate queries detected: {format_number(duplicate_queries)}")

        self.logger.info(f"SQL directory: {self.sql_dir.absolute()}")
        self.logger.info(f"Info directory: {self.sql_info_dir.absolute()}")
        print("=" * 70)
