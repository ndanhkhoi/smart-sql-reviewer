# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQL Reviewer is a 5-step pipeline tool for automated SQL performance analysis in the HIS 4.0 (Hospital Information System) project. It extracts SQL queries from Java/Spring Boot applications monitored by Glowroot APM, analyzes them using AI (Z.ai API), and generates optimization reports.

## Core Architecture

The system follows a linear pipeline pattern where each step consumes the output of the previous step:

1. **Fetch SQL** (`scripts/fetch_sql.py` + `src/fetchers/glowroot_fetcher.py`) - Queries Glowroot APM to extract SQL queries and execution metrics. Outputs `.sql` and `.json` files to `outputs/fetchers/sql/` and `outputs/fetchers/sql_info/`

2. **Parse SQL** (`scripts/parse_sql.py` + `src/parsers/sql_parser.py`) - Uses sqlglot to parse SQL and extract table names. Outputs to `outputs/parse/`

3. **Query Metadata** (`scripts/query_metadata.py` + `src/metadata/oracle_metadata_fetcher.py`) - Connects to Oracle Database to retrieve schema metadata (columns, indexes, constraints). Outputs to `outputs/metadata/`

4. **Review SQL** (`scripts/review_sql.py` + `src/reviewers/zai_reviewer.py`) - Sends SQL + metadata to Z.ai API for performance analysis. Outputs to `outputs/review/`

5. **Generate Report** (`scripts/generate_report.py`) - Uses Jinja2 templates to create interactive HTML report with Chart.js visualizations

## Running the Pipeline

### Full Pipeline Run
```bash
python scripts/fetch_sql.py --all && \
python scripts/parse_sql.py && \
python scripts/query_metadata.py && \
python scripts/review_sql.py && \
python scripts/generate_report.py
```

### Individual Steps
```bash
# Step 1: Fetch from specific agents
python scripts/fetch_sql.py --agent cto-billing-service
python scripts/fetch_sql.py --all --hours 12  # Override time window

# Step 4: Review with options
python scripts/review_sql.py --clean --limit 5  # Clean old results, test with 5 files
python scripts/review_sql.py --files billing    # Review specific files by pattern

# Clean outputs
python scripts/clean.py --all --dry-run  # Preview cleanup
```

## Configuration

All configuration is centralized in `config/config.yaml`:

- **glowroot.hours_ago**: Time window for data extraction (default: 48 hours)
- **glowroot.agents**: List of agent IDs to monitor (auto-discovers transactions)
- **oracle.metadata.default_schema**: Default Oracle schema (default: "his_data_danquy")
- **review.model**: AI model to use (default: "glm-4.6")
- **Parallel processing**: Most steps use ThreadPoolExecutor with configurable workers (10 for most steps, 3 for AI API calls)

Environment variables (set in `.env`):
- `GLOWROOT_BASE_URL` - Glowroot APM endpoint
- `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE_NAME`, `ORACLE_USER`, `ORACLE_PASSWORD` - Oracle connection
- `ZAI_API_KEY`, `ZAI_API_URL` - Z.ai API credentials

## Code Organization

- **Entry points**: `scripts/*.py` - Command-line interface for each pipeline step
- **Core logic**: `src/{fetchers,parsers,metadata,reviewers}/*.py` - Modular implementations
- **Utilities**: `src/utils/` - Shared helpers for logging, metadata processing
- **Templates**: `templates/report_template.html` - Jinja2 template for HTML report
- **AI prompts**: `resources/review_prompt.txt` - System prompt for Z.ai API

## Key Implementation Details

### Agent-Based Organization
Data is organized by Glowroot agent (e.g., `cto-billing-service`). Each agent has subdirectories in outputs for SQL, parsed data, metadata, and reviews.

### Parallel Processing Pattern
Most steps use `ThreadPoolExecutor` with a fixed worker count. Statistics are collected thread-safe using callbacks. Configuration for parallelism is in `config.yaml` under each component's `parallel` section.

### Error Handling
- AI API calls retry up to 10 times with exponential backoff (configurable in `review.max_retries`)
- Oracle uses connection pooling (min: 10, max: 20 connections)
- Comprehensive logging to both console and `logs/` directory

### Vietnamese Language Support
The entire system is designed for Vietnamese output:
- AI analysis prompts are in Vietnamese
- HTML report UI is in Vietnamese
- Issue descriptions and recommendations are in Vietnamese

## Output Structure

```
outputs/
├── fetchers/
│   ├── sql/           # Raw SQL queries (*.sql)
│   └── sql_info/      # Query execution metrics (*.json)
├── parse/             # Parsed table names (*.json)
├── metadata/          # Oracle schema metadata (*.json)
├── review/            # AI analysis results (*.json)
└── sql_review_report.html  # Final report
```

Each output file corresponds to a single SQL query, with filenames derived from the query signature/timestamp.

## Dependencies

Core Python packages (see `requirements.txt`):
- **sqlglot** - SQL parsing (Oracle dialect)
- **oracledb** - Oracle database connectivity
- **requests** - HTTP client for Glowroot and Z.ai APIs
- **jinja2** - HTML template engine
- **pyyaml** - Configuration parsing
- **rich** - Enhanced console output

## Testing Individual Components

```bash
# Test fetch with specific agent
python scripts/fetch_sql.py --agent cto-billing-service --hours 1

# Test review with limited files
python scripts/review_sql.py --clean --limit 3

# Test specific file patterns
python scripts/review_sql.py --files query1 --files billing
```
