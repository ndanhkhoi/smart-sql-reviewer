"""
SQL Parser for extracting table references from SQL queries.
Uses sqlglot to parse SQL and extract schema.table information.
"""

import sqlglot
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
import json
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils.logger import get_logger


class SQLTableExtractor:
    """
    Extract table references from SQL queries using sqlglot.

    Supports Oracle 19c dialect.
    """

    def __init__(self, dialect: str = "oracle", config: Optional[Dict] = None):
        """
        Initialize SQL table extractor.

        Args:
            dialect: SQL dialect (default: "oracle" for Oracle 19c)
            config: Optional configuration dict for parallel processing
        """
        self.dialect = dialect
        self.logger = get_logger(self.__class__.__name__)

        # Load parallel config if provided
        self.parallel_enabled = False
        self.max_workers = 10

        if config:
            parser_config = config.get('parser', {})
            parallel_config = parser_config.get('parallel', {})
            self.parallel_enabled = parallel_config.get('enabled', False)
            self.max_workers = parallel_config.get('max_workers', 10)

    def _extract_tables_from_ast(self, ast_node) -> Set[tuple]:
        """
        Recursively extract table references from SQL AST.

        Args:
            ast_node: SQLGlot AST node

        Returns:
            Set of (schema, table) tuples
        """
        tables = set()

        # Use sqlglot's built-in find_all to find all table references
        for table_node in ast_node.find_all(sqlglot.exp.Table):
            table_name = None
            schema_name = None

            # Get table name from table.this or table.name
            if hasattr(table_node, 'this') and table_node.this:
                table_name = str(table_node.this)
            elif hasattr(table_node, 'name'):
                table_name = str(table_node.name)

            # Get schema name from table.db (db is the schema in sqlglot)
            # table.db is a string directly, not an object
            if hasattr(table_node, 'db') and table_node.db:
                schema_name = str(table_node.db)

            # Only add if we have a table name
            if table_name:
                tables.add((schema_name, table_name))

        return tables

    def _preprocess_sql(self, sql: str) -> str:
        """
        Preprocess SQL to handle edge cases that sqlglot can't parse.

        Args:
            sql: SQL query string

        Returns:
            Preprocessed SQL string
        """
        import re

        # Fix LIKE expressions with parenthesized concatenation patterns
        # Patterns handled:
        # 1. like ('%'||...||'%') - full pattern with both prefix and suffix
        # 2. like ('%'||...) - only prefix
        # 3. like (...||'%') - only suffix
        # Example: column like ('%'||upper(?)||'%')
        # Example: column like ('%' || upper(?))
        # This causes sqlglot to fail with "Required keyword: 'this' missing for Like"
        # Solution: Remove the outer parentheses

        def fix_like_parens(match):
            like_keyword = match.group(1)
            inner_content = match.group(2)  # Already without the outer parens
            return f'{like_keyword} {inner_content}'

        # Pattern 1: Full pattern with both prefix and suffix: LIKE ('%'||...||'%')
        pattern1 = r'\b(LIKE)\s*\(\s*(\'\%\'\s*\|\|\s*.*?\s*\|\|\s*\'\%\')\s*\)'
        sql = re.sub(pattern1, fix_like_parens, sql, flags=re.IGNORECASE | re.DOTALL)

        # Pattern 2: Only prefix: LIKE ('%'||...) - ends without ||
        # This handles: like ('%' || upper(?))
        pattern2 = r'\b(LIKE)\s*\(\s*(\'\%\'\s*\|\|\s*[^\)]+)\s*\)'
        sql = re.sub(pattern2, fix_like_parens, sql, flags=re.IGNORECASE | re.DOTALL)

        # Pattern 3: Only suffix: LIKE (...||'%') - starts without ||
        # This handles: like (upper(?) || '%')
        pattern3 = r'\b(LIKE)\s*\(\s*([^\']+\s*\|\|\s*\'\%\')\s*\)'
        sql = re.sub(pattern3, fix_like_parens, sql, flags=re.IGNORECASE | re.DOTALL)

        return sql

    def extract_tables(self, sql: str) -> List[Dict[str, str]]:
        """
        Extract table references from a SQL query.

        Args:
            sql: SQL query string

        Returns:
            List of dicts with keys 'schema' and 'table'
            Returns empty list if no tables found or not a table-related query
        """
        if not sql or not sql.strip():
            return []

        # Skip ALTER SESSION commands - they don't contain table references
        sql_upper = sql.strip().upper()
        if sql_upper.startswith('ALTER SESSION') or sql_upper.startswith('ALTER\tSESSION'):
            return []

        try:
            # Preprocess SQL to handle edge cases
            sql = self._preprocess_sql(sql)

            # Parse SQL with Oracle dialect
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # Extract tables from AST
            tables = self._extract_tables_from_ast(parsed)

            # Convert to list of dicts
            result = []
            seen = set()  # For deduplication

            for schema, table in tables:
                # Create unique key for deduplication
                key = (schema or '', table.lower())
                if key not in seen:
                    seen.add(key)
                    result.append({
                        'schema': schema or '',
                        'table': table
                    })

            self.logger.debug(f"Extracted {len(result)} tables from SQL")
            return result

        except Exception as e:
            self.logger.error(f"Failed to parse SQL: {e}")
            # Return empty list on parse error
            return []

    def parse_sql_file(self, sql_file: Path) -> List[Dict[str, str]]:
        """
        Parse a single SQL file and extract table references.

        Args:
            sql_file: Path to SQL file

        Returns:
            List of dicts with keys 'schema' and 'table'
        """
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            tables = self.extract_tables(sql_content)
            self.logger.debug(f"Parsed {sql_file.name}: found {len(tables)} tables")
            return tables

        except Exception as e:
            self.logger.error(f"Failed to parse file {sql_file}: {e}")
            return []

    def parse_sql_directory(
        self,
        sql_dir: Path,
        output_dir: Path,
        pattern: str = "*.sql",
        clean_output: bool = False
    ) -> Dict[str, List[Dict[str, str]]]:
        """
        Parse all SQL files in a directory and save results.

        Args:
            sql_dir: Directory containing SQL files
            output_dir: Directory to save parsed results
            pattern: File pattern to match (default: "*.sql")
            clean_output: Clean output directory before parsing (default: False)

        Returns:
            Dict mapping filename to list of tables
        """
        sql_dir = Path(sql_dir)
        output_dir = Path(output_dir)

        # Clean output directory if requested
        if clean_output and output_dir.exists():
            self.logger.info(f"Cleaning output directory: {output_dir}")
            shutil.rmtree(output_dir)
            self.logger.info(f"Output directory cleaned")

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get all SQL files
        sql_files = sorted(list(sql_dir.glob(pattern)))
        total_files = len(sql_files)
        self.logger.info(f"Found {total_files} SQL files in {sql_dir}")

        if self.parallel_enabled and total_files > 1:
            self.logger.info(f"Parallel processing enabled with {self.max_workers} workers")
            return self._parse_parallel(sql_files, output_dir, total_files)
        else:
            self.logger.info("Sequential processing")
            return self._parse_sequential(sql_files, output_dir, total_files)

    def _parse_single_file(self, sql_file: Path, output_dir: Path, idx: int, total: int) -> Tuple[str, List[Dict[str, str]]]:
        """
        Parse a single SQL file (thread-safe).

        Args:
            sql_file: Path to SQL file
            output_dir: Directory to save parsed result
            idx: File index for logging
            total: Total number of files for logging

        Returns:
            Tuple of (filename, tables_list)
        """
        try:
            # Show progress
            self.logger.info(f"[{idx}/{total}] Parsing: {sql_file.name}")

            # Parse SQL file
            tables = self.parse_sql_file(sql_file)

            # Save to JSON file with same name
            output_file = output_dir / f"{sql_file.stem}.json"

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(tables, f, indent=2, ensure_ascii=False)

            self.logger.info(f"[{idx}/{total}] ✓ Parsed {len(tables)} tables → {output_file.name}")

            return (sql_file.name, tables)

        except Exception as e:
            self.logger.error(f"[{idx}/{total}] ✗ Error parsing {sql_file.name}: {e}")
            return (sql_file.name, [])

    def _parse_sequential(self, sql_files: List[Path], output_dir: Path, total_files: int) -> Dict[str, List[Dict[str, str]]]:
        """Parse SQL files sequentially (original behavior)."""
        results = {}

        for idx, sql_file in enumerate(sql_files, start=1):
            filename, tables = self._parse_single_file(sql_file, output_dir, idx, total_files)
            results[filename] = tables

        # Summary
        self._print_summary(results, total_files, output_dir)
        return results

    def _parse_parallel(self, sql_files: List[Path], output_dir: Path, total_files: int) -> Dict[str, List[Dict[str, str]]]:
        """Parse SQL files in parallel using ThreadPoolExecutor."""
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="sql_parser") as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self._parse_single_file, sql_file, output_dir, idx, total_files): sql_file
                for idx, sql_file in enumerate(sql_files, start=1)
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                sql_file = future_to_file[future]
                try:
                    filename, tables = future.result()
                    results[filename] = tables
                except Exception as e:
                    results[sql_file.name] = []
                    self.logger.error(f"Error processing {sql_file.name}: {e}")

        # Summary
        self._print_summary(results, total_files, output_dir)
        return results

    def _print_summary(self, results: Dict[str, List[Dict[str, str]]], total_files: int, output_dir: Path):
        """Print summary of parsing results."""
        total_tables = sum(len(tables) for tables in results.values())

        self.logger.info(f"=" * 60)
        self.logger.info(f"✓ Successfully parsed {len(results)}/{total_files} SQL files")
        self.logger.info(f"✓ Total table references: {total_tables}")
        self.logger.info(f"✓ Results saved to: {output_dir}")
        self.logger.info(f"=" * 60)

    def get_all_tables(
        self,
        sql_dir: Path,
        pattern: str = "*.sql"
    ) -> List[Dict[str, str]]:
        """
        Get all unique tables from all SQL files in a directory.

        Args:
            sql_dir: Directory containing SQL files
            pattern: File pattern to match (default: "*.sql")

        Returns:
            List of unique tables with schema information
        """
        sql_dir = Path(sql_dir)
        sql_files = list(sql_dir.glob(pattern))

        all_tables = {}
        seen = set()

        for sql_file in sql_files:
            tables = self.parse_sql_file(sql_file)

            for table_info in tables:
                key = (table_info['schema'], table_info['table'].lower())
                if key not in seen:
                    seen.add(key)
                    all_tables[key] = table_info

        result = list(all_tables.values())
        self.logger.info(f"Found {len(result)} unique tables across {len(sql_files)} files")
        return result


def parse_sql_files(
    sql_dir: Path,
    output_dir: Path,
    config: Optional[Dict] = None,
    clean_output: bool = False
) -> Dict[str, List[Dict[str, str]]]:
    """
    Convenience function to parse SQL files and save results.

    Args:
        sql_dir: Directory containing SQL files
        output_dir: Directory to save parsed results
        config: Optional configuration dict
        clean_output: Clean output directory before parsing (default: False)

    Returns:
        Dict mapping filename to list of tables
    """
    extractor = SQLTableExtractor(dialect="oracle", config=config)
    return extractor.parse_sql_directory(sql_dir, output_dir, clean_output=clean_output)


if __name__ == "__main__":
    # Example usage
    from pathlib import Path

    # Parse SQL files
    sql_dir = Path("outputs/fetchers/sql")
    output_dir = Path("outputs/fetchers/parse")

    extractor = SQLTableExtractor(dialect="oracle")

    # Parse all files and save results
    results = extractor.parse_sql_directory(sql_dir, output_dir)

    # Get all unique tables
    all_tables = extractor.get_all_tables(sql_dir)

    print(f"\nTotal unique tables found: {len(all_tables)}")
    print("\nTables:")
    for table in sorted(all_tables, key=lambda x: (x['schema'], x['table'])):
        schema_prefix = f"{table['schema']}." if table['schema'] else ""
        print(f"  - {schema_prefix}{table['table']}")
