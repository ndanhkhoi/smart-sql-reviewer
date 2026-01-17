"""
Oracle Metadata Fetcher for SQL Reviewer.
Extracts database metadata from Oracle 19c with connection pooling.
"""

import oracledb
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
import json
from pathlib import Path
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil

from src.utils.logger import get_logger
from src.utils.metadata_utils import (
    clean_value, dataclass_to_dict, build_dsn, safe_int
)


@dataclass
class ColumnMetadata:
    """Metadata of a single column."""
    columnName: str
    dataType: str
    dataLength: Optional[int] = None
    dataPrecision: Optional[int] = None
    dataScale: Optional[int] = None
    nullable: Optional[str] = None
    columnId: Optional[int] = None
    defaultValue: Optional[str] = None


@dataclass
class ConstraintMetadata:
    """Metadata of a single constraint."""
    constraintName: str
    constraintType: str
    status: Optional[str] = None
    validated: Optional[str] = None
    referencedConstraint: Optional[str] = None
    deleteRule: Optional[str] = None
    columns: Optional[str] = None
    searchCondition: Optional[str] = None


@dataclass
class IndexMetadata:
    """Metadata of a single index."""
    indexName: str
    indexType: Optional[str] = None
    uniqueness: Optional[str] = None
    tablespace: Optional[str] = None
    status: Optional[str] = None
    columns: Optional[str] = None


@dataclass
class TableMetadata:
    """Metadata of a single table."""
    tableName: str
    columns: List[Dict[str, Any]]
    tablespace: Optional[str] = None
    numRows: Optional[int] = None
    avgRowLength: Optional[int] = None
    lastAnalyzed: Optional[str] = None
    isTemporary: Optional[str] = None
    isPartitioned: Optional[str] = None
    constraints: Optional[List[Dict[str, Any]]] = None
    indexes: Optional[List[Dict[str, Any]]] = None


@dataclass
class ViewMetadata:
    """Metadata of a single view."""
    viewName: str
    textLength: Optional[int] = None
    viewDefinition: Optional[str] = None
    columns: Optional[List[Dict[str, Any]]] = None


@dataclass
class SchemaStatistics:
    """Aggregate statistics of a schema."""
    totalTables: int
    totalViews: int
    totalSequences: int
    totalProcedures: int


@dataclass
class SchemaMetadata:
    """Metadata of a schema."""
    tables: List[Dict[str, Any]]
    views: List[Dict[str, Any]]
    statistics: Dict[str, Any]


class OracleMetadataFetcher:
    """
    Fetch metadata from Oracle 19c with connection pooling.

    Usage:
        fetcher = OracleMetadataFetcher(config)
        fetcher.create_pool()
        metadata = fetcher.extract_metadata(tables)
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
        self.metadata_config = self.oracle_config.get('metadata', {})
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

        # Metadata settings
        self.default_schema = self.metadata_config.get('default_schema', '')
        self.case_insensitive = self.metadata_config.get('case_insensitive', True)

        # Parallel query settings
        parallel_config = self.metadata_config.get('parallel', {})
        self.parallel_enabled = parallel_config.get('enabled', False)
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

    def _get_table_metadata(self, conn, schema: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a single table."""
        cursor = conn.cursor()

        # Get table basic info
        table_query = """
            SELECT TABLE_NAME, TABLESPACE_NAME, NUM_ROWS, AVG_ROW_LEN,
                   LAST_ANALYZED, TEMPORARY, PARTITIONED
            FROM ALL_TABLES
            WHERE OWNER = :schema AND TABLE_NAME = :table_name
        """
        cursor.execute(table_query, schema=schema.upper(), table_name=table_name.upper())
        row = cursor.fetchone()

        if not row:
            cursor.close()
            return None

        table_meta = TableMetadata(
            tableName=row[0],
            tablespace=row[1],
            numRows=safe_int(row[2]),
            avgRowLength=safe_int(row[3]),
            lastAnalyzed=clean_value(row[4]),
            isTemporary=row[5],
            isPartitioned=row[6],
            columns=[],
            constraints=[],
            indexes=[],
        )

        # Get columns
        # Note: Cannot select DATA_DEFAULT from ALL_TAB_COLUMNS (LONG datatype causes ORA-00997)
        col_query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,
                   DATA_SCALE, NULLABLE, COLUMN_ID
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
        """
        cursor.execute(col_query, schema=schema.upper(), table_name=table_name.upper())

        for col_row in cursor:
            col = ColumnMetadata(
                columnName=col_row[0],
                dataType=col_row[1],
                dataLength=safe_int(col_row[2]),
                dataPrecision=safe_int(col_row[3]),
                dataScale=safe_int(col_row[4]),
                nullable=col_row[5],
                columnId=safe_int(col_row[6]),
                defaultValue=None,  # Skip DATA_DEFAULT (LONG datatype not supported)
            )
            table_meta.columns.append(dataclass_to_dict(col))

        # Get constraints
        # Note: Cannot select SEARCH_CONDITION from ALL_CONSTRAINTS (LONG datatype causes ORA-00997)
        cons_query = """
            SELECT c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, c.STATUS, c.VALIDATED,
                   c.R_CONSTRAINT_NAME, c.DELETE_RULE,
                   LISTAGG(cc.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY cc.POSITION) as columns
            FROM ALL_CONSTRAINTS c
            LEFT JOIN ALL_CONS_COLUMNS cc ON c.OWNER = cc.OWNER
                AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            WHERE c.OWNER = :schema AND c.TABLE_NAME = :table_name
            GROUP BY c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, c.STATUS, c.VALIDATED,
                     c.R_CONSTRAINT_NAME, c.DELETE_RULE
        """
        cursor.execute(cons_query, schema=schema.upper(), table_name=table_name.upper())

        for cons_row in cursor:
            cons = ConstraintMetadata(
                constraintName=cons_row[0],
                constraintType=cons_row[1],
                status=cons_row[2],
                validated=cons_row[3],
                referencedConstraint=cons_row[4],
                deleteRule=cons_row[5],
                columns=cons_row[6],
                searchCondition=None,  # Skip SEARCH_CONDITION (LONG datatype not supported)
            )
            table_meta.constraints.append(dataclass_to_dict(cons))

        # Get indexes
        idx_query = """
            SELECT i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS, i.TABLESPACE_NAME,
                   i.STATUS, LISTAGG(ic.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY ic.COLUMN_POSITION) as columns
            FROM ALL_INDEXES i
            LEFT JOIN ALL_IND_COLUMNS ic ON i.OWNER = ic.INDEX_OWNER
                AND i.INDEX_NAME = ic.INDEX_NAME
            WHERE i.TABLE_OWNER = :schema AND i.TABLE_NAME = :table_name
            GROUP BY i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS, i.TABLESPACE_NAME, i.STATUS
        """
        cursor.execute(idx_query, schema=schema.upper(), table_name=table_name.upper())

        for idx_row in cursor:
            idx = IndexMetadata(
                indexName=idx_row[0],
                indexType=idx_row[1],
                uniqueness=idx_row[2],
                tablespace=idx_row[3],
                status=idx_row[4],
                columns=idx_row[5],
            )
            table_meta.indexes.append(dataclass_to_dict(idx))

        cursor.close()
        return dataclass_to_dict(table_meta)

    def _get_view_metadata(self, conn, schema: str, view_name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a single view."""
        cursor = conn.cursor()

        # Note: Cannot select TEXT column from ALL_VIEWS (LONG datatype causes ORA-00997)
        # Use DBMS_METADATA.GET_DDL if view definition is needed
        view_query = """
            SELECT VIEW_NAME, TEXT_LENGTH
            FROM ALL_VIEWS
            WHERE OWNER = :schema AND VIEW_NAME = :view_name
        """
        cursor.execute(view_query, schema=schema.upper(), view_name=view_name.upper())
        row = cursor.fetchone()

        if not row:
            cursor.close()
            return None

        view_meta = ViewMetadata(
            viewName=row[0],
            textLength=safe_int(row[1]),
            viewDefinition=None,  # Skip TEXT column (LONG datatype not supported)
            columns=[],
        )

        # Get view columns
        col_query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,
                   DATA_SCALE, NULLABLE, COLUMN_ID
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema AND TABLE_NAME = :view_name
            ORDER BY COLUMN_ID
        """
        cursor.execute(col_query, schema=schema.upper(), view_name=view_name.upper())

        for col_row in cursor:
            col = ColumnMetadata(
                columnName=col_row[0],
                dataType=col_row[1],
                dataLength=safe_int(col_row[2]),
                dataPrecision=safe_int(col_row[3]),
                dataScale=safe_int(col_row[4]),
                nullable=col_row[5],
                columnId=safe_int(col_row[6]),
            )
            view_meta.columns.append(dataclass_to_dict(col))

        cursor.close()
        return dataclass_to_dict(view_meta)

    def _get_schema_statistics(self, conn, schema: str) -> Dict[str, Any]:
        """Get statistics for a schema."""
        cursor = conn.cursor()

        stats = SchemaStatistics(
            totalTables=0,
            totalViews=0,
            totalSequences=0,
            totalProcedures=0,
        )

        # Count tables
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :schema", schema=schema.upper()
        )
        stats.totalTables = cursor.fetchone()[0]

        # Count views
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_VIEWS WHERE OWNER = :schema", schema=schema.upper()
        )
        stats.totalViews = cursor.fetchone()[0]

        # Count sequences
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = :schema",
            schema=schema.upper(),
        )
        stats.totalSequences = cursor.fetchone()[0]

        # Count procedures/functions
        cursor.execute(
            """SELECT COUNT(*) FROM ALL_PROCEDURES
               WHERE OWNER = :schema AND OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')""",
            schema=schema.upper(),
        )
        stats.totalProcedures = cursor.fetchone()[0]

        cursor.close()
        return dataclass_to_dict(stats)

    def extract_metadata(self, tables: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Extract metadata for given list of tables with their schemas.

        Args:
            tables: List of dicts with keys 'schema' and 'table'

        Returns:
            Dict with schema as key, containing tables, views, and statistics
        """
        if not self._pool:
            raise RuntimeError("Connection pool not created. Call create_pool() first.")

        # Group tables by schema (use default schema if not provided)
        schemas = {}
        for item in tables:
            schema = item.get("schema", "").strip()
            if not schema and self.default_schema:
                schema = self.default_schema
                self.logger.debug(f"Using default schema '{schema}' for table {item['table']}")

            if not schema:
                self.logger.warning(f"No schema specified for table {item['table']} and no default schema configured")
                continue

            table = item["table"]
            if schema not in schemas:
                schemas[schema] = {"tables": set()}
            schemas[schema]["tables"].add(table)

        self.logger.info(f"Extracting metadata for {len(tables)} objects in {len(schemas)} schemas")
        result = {}

        with self.get_connection() as conn:
            for schema, items in schemas.items():
                self.logger.debug(f"Processing schema: {schema}")
                schema_data = {"tables": [], "views": [], "statistics": {}}

                # Extract metadata for each table
                for table_name in items["tables"]:
                    # Check if it's a table or view
                    cursor = conn.cursor()
                    cursor.execute(
                        """SELECT OBJECT_TYPE FROM ALL_OBJECTS
                           WHERE OWNER = :schema AND OBJECT_NAME = :table_name
                           AND OBJECT_TYPE IN ('TABLE', 'VIEW')""",
                        schema=schema.upper(),
                        table_name=table_name.upper(),
                    )
                    row = cursor.fetchone()
                    cursor.close()

                    if not row:
                        self.logger.warning(f"Object {schema}.{table_name} not found")
                        continue

                    obj_type = row[0]

                    if obj_type == "TABLE":
                        metadata = self._get_table_metadata(conn, schema, table_name)
                        if metadata:
                            schema_data["tables"].append(metadata)
                            self.logger.debug(f"  - Table: {table_name}")
                    elif obj_type == "VIEW":
                        metadata = self._get_view_metadata(conn, schema, table_name)
                        if metadata:
                            schema_data["views"].append(metadata)
                            self.logger.debug(f"  - View: {table_name}")

                # Get schema statistics
                schema_data["statistics"] = self._get_schema_statistics(conn, schema)
                result[schema] = schema_data

        self.logger.info(f"Metadata extraction completed: {len(result)} schemas")
        return result

    def save_metadata(self, metadata: Dict[str, Any], output_file: Path):
        """
        Save metadata to JSON file.

        Args:
            metadata: Metadata dictionary
            output_file: Path to output file
        """
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

        self.logger.info(f"Metadata saved to {output_file}")

    def extract_metadata_from_parse_file(self, parse_file: Path) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from a parse output file.

        Args:
            parse_file: Path to parse JSON file

        Returns:
            Metadata dictionary or None if error
        """
        if not self._pool:
            raise RuntimeError("Connection pool not created. Call create_pool() first.")

        # Read parse file
        try:
            with open(parse_file, 'r', encoding='utf-8') as f:
                tables_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to read parse file {parse_file}: {e}")
            return None

        if not tables_data:
            self.logger.debug(f"Empty parse file {parse_file} - returning empty metadata")
            return {}  # Return empty dict for empty parse files

        # Extract metadata
        try:
            metadata = self.extract_metadata(tables_data)
            return metadata
        except Exception as e:
            self.logger.error(f"Failed to extract metadata for {parse_file}: {e}")
            return None

    def _query_single_file(self, parse_file: Path, output_dir: Path, idx: int, total: int) -> Tuple[str, Dict[str, Any]]:
        """
        Query metadata for a single parse file (thread-safe).

        Args:
            parse_file: Path to parse JSON file
            output_dir: Directory to save metadata file
            idx: File index for logging
            total: Total number of files for logging

        Returns:
            Tuple of (filename, result_dict)
        """
        try:
            # Show progress
            self.logger.info(f"[{idx}/{total}] Querying metadata: {parse_file.name}")

            # Extract metadata
            metadata = self.extract_metadata_from_parse_file(parse_file)

            # Save to file with same name
            output_file = output_dir / parse_file.name

            if metadata is not None:  # Check for None (error), empty dict is valid
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

                # Count total tables/views
                total_tables = sum(len(schema_data.get("tables", [])) for schema_data in metadata.values())
                total_views = sum(len(schema_data.get("views", [])) for schema_data in metadata.values())

                result = {
                    "status": "success",
                    "tables": total_tables,
                    "views": total_views,
                    "schemas": list(metadata.keys())
                }

                # Log message includes empty files
                if total_tables == 0 and total_views == 0:
                    self.logger.info(f"[{idx}/{total}] ✓ Empty metadata → {output_file.name}")
                else:
                    self.logger.info(f"[{idx}/{total}] ✓ Queried {total_tables} tables, {total_views} views → {output_file.name}")

                return (parse_file.name, result)
            else:
                result = {
                    "status": "failed",
                    "error": "Failed to extract metadata"
                }
                self.logger.warning(f"[{idx}/{total}] ✗ Failed to query metadata for {parse_file.name}")

                return (parse_file.name, result)

        except Exception as e:
            result = {
                "status": "failed",
                "error": str(e)
            }
            self.logger.error(f"[{idx}/{total}] ✗ Error querying {parse_file.name}: {e}")

            return (parse_file.name, result)

    def query_metadata_from_parse_dir(
        self,
        parse_dir: Path,
        output_dir: Path,
        pattern: str = "*.json",
        clean_output: bool = False
    ) -> Dict[str, Any]:
        """
        Query metadata for all parse output files in a directory.

        Args:
            parse_dir: Directory containing parse JSON files
            output_dir: Directory to save metadata files
            pattern: File pattern to match (default: "*.json")
            clean_output: Clean output directory before querying (default: False)

        Returns:
            Dict mapping filename to metadata result
        """
        parse_dir = Path(parse_dir)
        output_dir = Path(output_dir)

        # Clean output directory if requested
        if clean_output and output_dir.exists():
            self.logger.info(f"Cleaning output directory: {output_dir}")
            shutil.rmtree(output_dir)
            self.logger.info(f"Output directory cleaned")

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get all parse files
        parse_files = sorted(list(parse_dir.glob(pattern)))
        total_files = len(parse_files)
        self.logger.info(f"Found {total_files} parse files in {parse_dir}")

        if self.parallel_enabled and total_files > 1:
            self.logger.info(f"Parallel processing enabled with {self.max_workers} workers")
            return self._query_parallel(parse_files, output_dir, total_files)
        else:
            self.logger.info("Sequential processing")
            return self._query_sequential(parse_files, output_dir, total_files)

    def _query_sequential(self, parse_files: List[Path], output_dir: Path, total_files: int) -> Dict[str, Any]:
        """Query metadata sequentially (original behavior)."""
        results = {}

        for idx, parse_file in enumerate(parse_files, start=1):
            filename, result = self._query_single_file(parse_file, output_dir, idx, total_files)
            results[filename] = result

        # Summary
        self._print_summary(results, total_files, output_dir)
        return results

    def _query_parallel(self, parse_files: List[Path], output_dir: Path, total_files: int) -> Dict[str, Any]:
        """Query metadata in parallel using ThreadPoolExecutor."""
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="metadata_query") as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self._query_single_file, parse_file, output_dir, idx, total_files): parse_file
                for idx, parse_file in enumerate(parse_files, start=1)
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                parse_file = future_to_file[future]
                try:
                    filename, result = future.result()
                    results[filename] = result
                except Exception as e:
                    results[parse_file.name] = {
                        "status": "failed",
                        "error": str(e)
                    }
                    self.logger.error(f"Error processing {parse_file.name}: {e}")

        # Summary
        self._print_summary(results, total_files, output_dir)
        return results

    def _print_summary(self, results: Dict[str, Any], total_files: int, output_dir: Path = None):
        """Print summary of metadata query results."""
        success_count = sum(1 for r in results.values() if r["status"] == "success")
        total_tables = sum(r.get("tables", 0) for r in results.values())
        total_views = sum(r.get("views", 0) for r in results.values())

        self.logger.info(f"=" * 60)
        self.logger.info(f"✓ Successfully queried {success_count}/{total_files} files")
        self.logger.info(f"✓ Total metadata: {total_tables} tables, {total_views} views")
        if output_dir:
            self.logger.info(f"✓ Results saved to: {output_dir}")
        self.logger.info(f"=" * 60)
