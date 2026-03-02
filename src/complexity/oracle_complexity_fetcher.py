"""
Oracle SQL Complexity Fetcher for SQL Reviewer.
Calls stored procedure sp_analyze_sql to get column and table counts.
"""

import oracledb
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
import json
from pathlib import Path
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils.logger import get_logger
from src.utils.metadata_utils import build_dsn


@dataclass
class SQLComplexityResult:
    """Result of SQL complexity analysis."""
    agent: str
    transaction: str
    queryNumber: int
    sqlQuery: str
    operation: str
    columnCount: int
    tableCount: int
    error: Optional[str] = None
    failedStep: Optional[str] = None


class OracleComplexityFetcher:
    """
    Fetch SQL complexity information using sp_analyze_sql stored procedure.

    Usage:
        fetcher = OracleComplexityFetcher(config)
        fetcher.create_pool()
        results = fetcher.analyze_sql_files(sql_dir, sql_info_dir)
        fetcher.close_pool()
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize fetcher with configuration.

        Args:
            config: Configuration dictionary containing oracle settings
        """
        self.config = config
        self.oracle_config = config.get('oracle', {})
        self.logger = get_logger(self.__class__.__name__)

        # Get connection parameters from environment variables or config
        self.host = os.getenv('ORACLE_HOST', self.oracle_config.get('host', 'localhost'))
        self.port = int(os.getenv('ORACLE_PORT', str(self.oracle_config.get('port', 1521))))
        self.service_name = os.getenv('ORACLE_SERVICE_NAME', self.oracle_config.get('service_name'))
        self.sid = os.getenv('ORACLE_SID', self.oracle_config.get('sid'))

        # Get auth from environment variables or config
        self.user = os.getenv('ORACLE_USER') or self.oracle_config.get('user')
        self.password = os.getenv('ORACLE_PASSWORD') or self.oracle_config.get('password')

        if not self.user:
            raise ValueError("ORACLE_USER environment variable or oracle.user config is required")
        if not self.password:
            raise ValueError("ORACLE_PASSWORD environment variable or oracle.password config is required")

        # Pool settings
        pool_config = self.oracle_config.get('pool', {})
        self.pool_min = pool_config.get('min', 1)
        self.pool_max = pool_config.get('max', 5)
        self.pool_increment = pool_config.get('increment', 1)

        # Parallel processing settings
        parallel_config = self.oracle_config.get('metadata', {}).get('parallel', {})
        self.parallel_enabled = parallel_config.get('enabled', True)
        self.max_workers = parallel_config.get('max_workers', 10)

        self._pool = None

    def _get_dsn(self) -> str:
        """Build Data Source Name."""
        return build_dsn(self.host, self.port, self.service_name, self.sid)

    def create_pool(self):
        """Create connection pool."""
        if not self.user or not self.password:
            raise ValueError(
                "Oracle credentials not provided. "
                "Set in config.yaml or ORACLE_USER/ORACLE_PASSWORD environment variables."
            )

        dsn = self._get_dsn()
        self._pool = oracledb.create_pool(
            user=self.user,
            password=self.password,
            dsn=dsn,
            min=self.pool_min,
            max=self.pool_max,
            increment=self.pool_increment,
        )
        self.logger.info(f"Created Oracle connection pool to {dsn}")

    def close_pool(self):
        """Close connection pool."""
        if self._pool:
            self._pool.close()
            self.logger.info("Oracle connection pool closed")
            self._pool = None

    @contextmanager
    def get_connection(self):
        """Get connection from pool."""
        if not self._pool:
            raise RuntimeError("Connection pool not created. Call create_pool() first.")
        conn = self._pool.acquire()
        try:
            yield conn
        finally:
            self._pool.release(conn)

    def _parse_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Parse filename to extract agent and transaction.

        Filename format: {agent}___{transaction}__{query_number}.{ext}
        Example: cto-billing-service___duyet_duyetketoan__5.sql

        Args:
            filename: Name of the file

        Returns:
            Dict with agent, transaction, query_number or None if invalid format
        """
        # Remove extension
        name_without_ext = Path(filename).stem

        # Match pattern: agent___transaction__query_number
        pattern = r'^([^_]+(?:_[^_]+)*?)___([^_]+(?:_[^_]+)*)__(\d+)$'
        match = re.match(pattern, name_without_ext)

        if not match:
            self.logger.warning(f"Could not parse filename: {filename}")
            return None

        agent = match.group(1)
        transaction = match.group(2).replace('_', '/')
        query_number = int(match.group(3))

        return {
            "agent": agent,
            "transaction": f"/{transaction}",
            "query_number": query_number
        }

    def _read_sql_file(self, sql_file: Path) -> Optional[str]:
        """
        Read SQL from file.

        Args:
            sql_file: Path to SQL file

        Returns:
            SQL string or None if error
        """
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception as e:
            self.logger.error(f"Failed to read SQL file {sql_file}: {e}")
            return None

    def _read_sql_info_file(self, info_file: Path) -> Optional[Dict[str, Any]]:
        """
        Read SQL info from JSON file.

        Args:
            info_file: Path to SQL info JSON file

        Returns:
            Dict with info or None if error
        """
        try:
            with open(info_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to read SQL info file {info_file}: {e}")
            return None

    def _call_sp_analyze_sql(self, conn, sql: str) -> Dict[str, Any]:
        """
        Call stored procedure sp_analyze_sql using callproc.

        Args:
            conn: Oracle connection
            sql: SQL query to analyze

        Returns:
            Dict with operation, num_columns, num_tables, or error info
        """
        cursor = conn.cursor()

        # Define output parameter for CLOB
        result_clob = cursor.var(oracledb.CLOB)

        try:
            # Call stored procedure directly using callproc
            # his_admin_app.sp_analyze_sql(p_sql IN CLOB, p_result OUT CLOB)
            cursor.callproc(
                'his_admin_app.sp_analyze_sql',
                [sql, result_clob]
            )

            # Get result from CLOB
            clob_value = result_clob.getvalue()
            if clob_value:
                result_str = clob_value.read()
                return json.loads(result_str)
            else:
                return {
                    "error": "No result returned from stored procedure",
                    "num_columns": -1,
                    "num_tables": -1
                }

        except Exception as e:
            self.logger.debug(f"Error calling sp_analyze_sql: {e}")
            return {
                "error": str(e),
                "num_columns": -1,
                "num_tables": -1
            }
        finally:
            cursor.close()

    def _analyze_single_sql(
        self,
        sql_file: Path,
        info_file: Path,
        idx: int,
        total: int
    ) -> Optional[SQLComplexityResult]:
        """
        Analyze a single SQL file for complexity.

        Args:
            sql_file: Path to SQL file
            info_file: Path to SQL info JSON file
            idx: Index for logging
            total: Total count for logging

        Returns:
            SQLComplexityResult or None if error
        """
        try:
            self.logger.info(f"[{idx}/{total}] Analyzing: {sql_file.name}")

            # Parse filename
            file_info = self._parse_filename(sql_file.name)
            if not file_info:
                return None

            # Read SQL
            sql_query = self._read_sql_file(sql_file)
            if not sql_query:
                return None

            # Read SQL info (for additional metadata if needed)
            sql_info = self._read_sql_info_file(info_file) if info_file.exists() else None

            # Call stored procedure
            with self.get_connection() as conn:
                sp_result = self._call_sp_analyze_sql(conn, sql_query)

            # Create result
            result = SQLComplexityResult(
                agent=file_info["agent"],
                transaction=file_info["transaction"],
                queryNumber=file_info["query_number"],
                sqlQuery=sql_query,
                operation=sp_result.get("operation", "UNKNOWN"),
                columnCount=sp_result.get("num_columns", -1),
                tableCount=sp_result.get("num_tables", -1),
                error=sp_result.get("error"),
                failedStep=sp_result.get("failed_step")
            )

            # Log result
            if result.error:
                self.logger.warning(
                    f"[{idx}/{total}] ✗ {result.agent} | {result.transaction} | "
                    f"Query #{result.queryNumber}: ERROR - {result.error}"
                )
            else:
                self.logger.info(
                    f"[{idx}/{total}] ✓ {result.agent} | {result.transaction} | "
                    f"Query #{result.queryNumber}: "
                    f"{result.operation} | {result.columnCount} columns | {result.tableCount} tables"
                )

            return result

        except Exception as e:
            self.logger.error(f"[{idx}/{total}] ✗ Error analyzing {sql_file.name}: {e}")
            return None

    def analyze_sql_files(
        self,
        sql_dir: Path,
        sql_info_dir: Path,
        output_file: Optional[Path] = None,
        pattern: str = "*.sql"
    ) -> List[Dict[str, Any]]:
        """
        Analyze all SQL files in directory.

        Args:
            sql_dir: Directory containing SQL files
            sql_info_dir: Directory containing SQL info JSON files
            output_file: Optional path to save results as JSON
            pattern: File pattern to match (default: "*.sql")

        Returns:
            List of result dictionaries
        """
        sql_dir = Path(sql_dir)
        sql_info_dir = Path(sql_info_dir)

        # Get all SQL files
        sql_files = sorted(list(sql_dir.glob(pattern)))
        total_files = len(sql_files)

        self.logger.info(f"Found {total_files} SQL files in {sql_dir}")
        self.logger.info(f"=" * 60)
        self.logger.info(f"Analyzing SQL complexity using sp_analyze_sql")
        self.logger.info(f"=" * 60)

        results = []

        if self.parallel_enabled and total_files > 1:
            self.logger.info(f"Parallel processing enabled with {self.max_workers} workers")
            results = self._analyze_parallel(sql_files, sql_info_dir, total_files)
        else:
            self.logger.info("Sequential processing")
            results = self._analyze_sequential(sql_files, sql_info_dir, total_files)

        # Save to file if specified
        if output_file:
            self._save_results(results, output_file)

        # Print summary
        self._print_summary(results, total_files)

        return results

    def _analyze_sequential(
        self,
        sql_files: List[Path],
        sql_info_dir: Path,
        total_files: int
    ) -> List[Dict[str, Any]]:
        """Analyze SQL files sequentially."""
        results = []

        for idx, sql_file in enumerate(sql_files, start=1):
            info_file = sql_info_dir / f"{sql_file.stem}.json"
            result = self._analyze_single_sql(sql_file, info_file, idx, total_files)
            if result:
                results.append(self._result_to_dict(result))

        return results

    def _analyze_parallel(
        self,
        sql_files: List[Path],
        sql_info_dir: Path,
        total_files: int
    ) -> List[Dict[str, Any]]:
        """Analyze SQL files in parallel using ThreadPoolExecutor."""
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="complexity") as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(
                    self._analyze_single_sql,
                    sql_file,
                    sql_info_dir / f"{sql_file.stem}.json",
                    idx,
                    total_files
                ): sql_file
                for idx, sql_file in enumerate(sql_files, start=1)
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    if result:
                        results.append(self._result_to_dict(result))
                except Exception as e:
                    sql_file = future_to_file[future]
                    self.logger.error(f"Error processing {sql_file.name}: {e}")

        # Sort by filename for consistent output
        results.sort(key=lambda x: (x.get("agent", ""), x.get("transaction", ""), x.get("queryNumber", 0)))

        return results

    def _result_to_dict(self, result: SQLComplexityResult) -> Dict[str, Any]:
        """Convert SQLComplexityResult to dictionary."""
        return {
            "agent": result.agent,
            "transaction": result.transaction,
            "queryNumber": result.queryNumber,
            "sqlQuery": result.sqlQuery,
            "operation": result.operation,
            "columnCount": result.columnCount,
            "tableCount": result.tableCount,
            "error": result.error,
            "failedStep": result.failedStep
        }

    def _save_results(self, results: List[Dict[str, Any]], output_file: Path):
        """Save results to JSON file."""
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Results saved to {output_file}")

    def _print_summary(self, results: List[Dict[str, Any]], total_files: int):
        """Print summary of analysis results."""
        success_count = sum(1 for r in results if not r.get("error"))
        error_count = sum(1 for r in results if r.get("error"))

        # Count by operation type
        operation_counts = {}
        for r in results:
            op = r.get("operation", "UNKNOWN")
            operation_counts[op] = operation_counts.get(op, 0) + 1

        # Calculate statistics (only valid values, not None or -1)
        total_tables = sum(r.get("tableCount", 0) for r in results if r.get("tableCount") is not None and r.get("tableCount", 0) > 0)
        total_columns = sum(r.get("columnCount", 0) for r in results if r.get("columnCount") is not None and r.get("columnCount", 0) > 0)

        avg_tables = total_tables / success_count if success_count > 0 else 0
        avg_columns = total_columns / success_count if success_count > 0 else 0

        self.logger.info(f"=" * 60)
        self.logger.info(f"Summary:")
        self.logger.info(f"  Total files: {total_files}")
        self.logger.info(f"  Successful: {success_count}")
        self.logger.info(f"  Failed: {error_count}")

        if operation_counts:
            self.logger.info(f"\nOperation Types:")
            for op, count in sorted(operation_counts.items()):
                self.logger.info(f"  {op}: {count}")

        self.logger.info(f"\nComplexity Statistics:")
        self.logger.info(f"  Total tables accessed: {total_tables}")
        self.logger.info(f"  Total columns: {total_columns}")
        self.logger.info(f"  Avg tables per query: {avg_tables:.2f}")
        self.logger.info(f"  Avg columns per query: {avg_columns:.2f}")
        self.logger.info(f"=" * 60)
