#!/usr/bin/env python3
"""
SQL Review Report Generator

Uses Jinja2 template engine to generate beautiful HTML reports
with modern design system from UI/UX Pro Max.

Requirements:
    pip install jinja2

Usage:
    python generate_report.py [outputs_dir] [output_file] [template_name]
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape


# ============================================================================
# Configuration
# ============================================================================

TEMPLATE_NAME = "report_template.html"
DEFAULT_OUTPUT = "sql_review_report.html"


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class ParsedFilename:
    """Result of parsing a filename in format: agent___transaction__index.ext"""
    agent: str
    transaction: str
    index: int

    @classmethod
    def from_string(cls, filename: str) -> Optional["ParsedFilename"]:
        """Parse filename and return ParsedFilename or None if invalid"""
        name = Path(filename).stem
        match = re.match(r'^(.+)___(.+)__(\d+)$', name)
        if match:
            return cls(
                agent=match.group(1),
                transaction=match.group(2),
                index=int(match.group(3))
            )
        return None


@dataclass
class SQLRecord:
    """Complete SQL review record merged from all phases"""
    agent_id: str
    transaction_name: str
    transaction_type: Optional[str] = None
    query_number: Optional[int] = None
    query_type: Optional[str] = None
    total_duration_nanos: Optional[float] = None
    execution_count: Optional[int] = None
    total_rows: Optional[int] = None
    timestamp: Optional[str] = None
    sql_query: Optional[str] = None
    tables_used: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)
    issues: List[Dict[str, Any]] = field(default_factory=list)

    # Computed fields
    total_duration_s: Optional[float] = None
    avg_duration_ms: Optional[float] = None
    tables: List[Dict[str, Any]] = field(default_factory=list)
    max_severity: Optional[str] = None


# ============================================================================
# Data Loading
# ============================================================================

class DataLoader:
    """Load and merge data from all phases"""

    def __init__(self, outputs_dir: Path):
        self.outputs_dir = Path(outputs_dir)
        self.records: Dict[tuple, Dict[str, Any]] = {}

    def load_all(self) -> List[SQLRecord]:
        """Load data from all phases and return merged SQL records"""
        print("📂 Loading data from all phases...")

        self._load_sql_info()
        self._load_sql_queries()
        self._load_parse_data()
        self._load_metadata()
        self._load_reviews()

        print(f"✓ Loaded {len(self.records)} SQL records")
        return self._merge_and_enrich()

    def _load_sql_info(self):
        """Load SQL info (metadata about query execution)"""
        sql_info_dir = self.outputs_dir / "fetchers" / "sql_info"
        if not sql_info_dir.exists():
            return

        for file_path in sql_info_dir.glob("*.json"):
            parsed = ParsedFilename.from_string(file_path.name)
            if not parsed:
                continue

            key = (parsed.agent, parsed.transaction, parsed.index)
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_info = json.load(f)

            if key not in self.records:
                self.records[key] = {}

            self.records[key].update({
                'agent_id': sql_info.get('agent_id'),
                'transaction_name': sql_info.get('transaction_name'),
                'transaction_type': sql_info.get('transaction_type'),
                'query_number': sql_info.get('query_number'),
                'query_type': sql_info.get('query_type'),
                'total_duration_nanos': sql_info.get('total_duration_nanos'),
                'execution_count': sql_info.get('execution_count'),
                'total_rows': sql_info.get('total_rows'),
                'timestamp': sql_info.get('timestamp'),
            })

    def _load_sql_queries(self):
        """Load SQL query text"""
        sql_dir = self.outputs_dir / "fetchers" / "sql"
        if not sql_dir.exists():
            return

        for file_path in sql_dir.glob("*.sql"):
            parsed = ParsedFilename.from_string(file_path.name)
            if not parsed:
                continue

            key = (parsed.agent, parsed.transaction, parsed.index)
            with open(file_path, 'r', encoding='utf-8') as f:
                self.records.setdefault(key, {})['sql_query'] = f.read().strip()

    def _load_parse_data(self):
        """Load parse data (schema/table info)"""
        parse_dir = self.outputs_dir / "parse"
        if not parse_dir.exists():
            return

        for file_path in parse_dir.glob("*.json"):
            parsed = ParsedFilename.from_string(file_path.name)
            if not parsed:
                continue

            key = (parsed.agent, parsed.transaction, parsed.index)
            with open(file_path, 'r', encoding='utf-8') as f:
                parse_data = json.load(f)

            self.records.setdefault(key, {})['tables_used'] = parse_data

    def _load_metadata(self):
        """Load metadata (table schema, columns, indexes)"""
        metadata_dir = self.outputs_dir / "metadata"
        if not metadata_dir.exists():
            return

        for file_path in metadata_dir.glob("*.json"):
            parsed = ParsedFilename.from_string(file_path.name)
            if not parsed:
                continue

            key = (parsed.agent, parsed.transaction, parsed.index)
            with open(file_path, 'r', encoding='utf-8') as f:
                self.records.setdefault(key, {})['metadata'] = json.load(f)

    def _load_reviews(self):
        """Load review results"""
        review_dir = self.outputs_dir / "review"
        if not review_dir.exists():
            return

        for file_path in review_dir.glob("*.json"):
            parsed = ParsedFilename.from_string(file_path.name)
            if not parsed:
                continue

            key = (parsed.agent, parsed.transaction, parsed.index)
            with open(file_path, 'r', encoding='utf-8') as f:
                review = json.load(f)

            self.records.setdefault(key, {})['summary'] = review.get('summary', {})
            self.records.setdefault(key, {})['issues'] = review.get('issues', [])

    def _merge_and_enrich(self) -> List[SQLRecord]:
        """Merge all data and compute derived fields"""
        result = []

        for (agent, transaction, index), data in self.records.items():
            # Ensure summary exists with defaults
            if 'summary' not in data or not data['summary']:
                data['summary'] = {
                    'performance_score': 10,
                    'complexity_score': 0,
                    'total_issues': 0,
                    'by_severity': {'critical': 0, 'high': 0, 'medium': 0, 'low': 0},
                    'by_category': {'performance': 0, 'nplus1': 0, 'hibernate': 0, 'code_quality': 0, 'index': 0},
                    'overall_assessment': 'Không có đánh giá',
                    'priority': 'low',
                    'effort_to_fix': 'low'
                }

            if 'issues' not in data:
                data['issues'] = []

            # Calculate duration conversions
            total_duration_nanos = data.get('total_duration_nanos')
            if total_duration_nanos:
                data['total_duration_s'] = total_duration_nanos / 1_000_000_000
                exec_count = data.get('execution_count', 1)
                data['avg_duration_ms'] = (total_duration_nanos / exec_count) / 1_000_000 if exec_count else None
            else:
                data['total_duration_s'] = None
                data['avg_duration_ms'] = None

            # Extract table metadata
            tables = []
            metadata = data.get('metadata', {})
            for db_name, db_data in metadata.items():
                if isinstance(db_data, dict) and 'tables' in db_data:
                    for table_info in db_data['tables']:
                        tables.append({
                            'table_name': table_info.get('tableName'),
                            'num_rows': table_info.get('numRows'),
                            'column_count': len(table_info.get('columns', [])),
                            'index_count': len(table_info.get('indexes', []))
                        })
            data['tables'] = tables

            # Calculate max severity
            issues = data.get('issues', [])
            if issues:
                severity_order = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
                max_issue = max(issues, key=lambda x: severity_order.get(x.get('severity', 'low'), 0))
                data['max_severity'] = max_issue.get('severity', 'low')
            else:
                data['max_severity'] = None

            # Create SQLRecord
            record = SQLRecord(
                agent_id=data.get('agent_id', agent),
                transaction_name=data.get('transaction_name', transaction),
                transaction_type=data.get('transaction_type'),
                query_number=data.get('query_number'),
                query_type=data.get('query_type'),
                total_duration_nanos=data.get('total_duration_nanos'),
                execution_count=data.get('execution_count'),
                total_rows=data.get('total_rows'),
                timestamp=data.get('timestamp'),
                sql_query=data.get('sql_query'),
                tables_used=data.get('tables_used', []),
                metadata=data.get('metadata', {}),
                summary=data['summary'],
                issues=data['issues'],
                total_duration_s=data.get('total_duration_s'),
                avg_duration_ms=data.get('avg_duration_ms'),
                tables=data['tables'],
                max_severity=data.get('max_severity')
            )

            result.append(record)

        return result


# ============================================================================
# Statistics Calculator
# ============================================================================

class StatisticsCalculator:
    """Calculate statistics from SQL records"""

    def __init__(self, records: List[SQLRecord]):
        self.records = records

    def calculate_overall_stats(self) -> Dict[str, Any]:
        """Calculate overall statistics"""
        stats = {
            'total_sqls': len(self.records),
            'total_issues': 0,
            'sqls_with_issues': 0,
            'by_severity': {'critical': 0, 'high': 0, 'medium': 0, 'low': 0},
            'by_category': {
                'performance': 0,
                'nplus1': 0,
                'hibernate': 0,
                'code_quality': 0,
                'index': 0
            },
            'avg_performance_score': 0,
            'total_agents': set(),
            'total_transactions': set(),
            'agents': set()
        }

        total_performance = 0

        for record in self.records:
            summary = record.summary
            issues = record.issues

            # Count SQLs with issues
            if len(issues) > 0:
                stats['sqls_with_issues'] += 1

            # Count issues by severity
            for issue in issues:
                severity = issue.get('severity', 'low')
                if severity in stats['by_severity']:
                    stats['by_severity'][severity] += 1
                stats['total_issues'] += 1

                # Count by category
                category = issue.get('category', 'code_quality')
                if category in stats['by_category']:
                    stats['by_category'][category] += 1

            # Performance score
            total_performance += summary.get('performance_score', 0)

            # Track agents and transactions
            stats['total_agents'].add(record.agent_id)
            stats['total_transactions'].add(record.transaction_name)
            stats['agents'].add(record.agent_id)

        # Calculate averages
        if len(self.records) > 0:
            stats['avg_performance_score'] = total_performance / len(self.records)

        # Convert sets to lists
        stats['total_agents'] = len(stats['total_agents'])
        stats['total_transactions'] = len(stats['total_transactions'])
        stats['agents'] = sorted(list(stats['agents']))

        return stats

    def group_by_agent(self) -> Dict[str, Dict[str, Any]]:
        """Group SQL records by agent"""
        from collections import defaultdict

        agents = defaultdict(lambda: {
            'total_sqls': 0,
            'total_issues': 0,
            'total_transactions': set(),
            'performance_scores': [],
            'max_severity': None,
            'severity_order': {}
        })

        for record in self.records:
            agent_id = record.agent_id
            agents[agent_id]['total_sqls'] += 1
            agents[agent_id]['total_transactions'].add(record.transaction_name)

            perf_score = record.summary.get('performance_score', 0)
            agents[agent_id]['performance_scores'].append(perf_score)

            issues = record.issues
            agents[agent_id]['total_issues'] += len(issues)

            # Track max severity
            if record.max_severity:
                severity_order = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
                current_severity = severity_order.get(record.max_severity, 0)
                max_sev = agents[agent_id]['severity_order'].get(agent_id, 0)
                if current_severity > max_sev:
                    agents[agent_id]['max_severity'] = record.max_severity
                    agents[agent_id]['severity_order'][agent_id] = current_severity

        # Calculate averages and convert sets
        result = {}
        for agent_id, data in agents.items():
            perf_scores = data['performance_scores']
            avg_perf = sum(perf_scores) / len(perf_scores) if perf_scores else 0

            result[agent_id] = {
                'total_sqls': data['total_sqls'],
                'total_issues': data['total_issues'],
                'total_transactions': len(data['total_transactions']),
                'avg_performance': round(avg_perf, 1),
                'max_severity': data['max_severity']
            }

        # Sort by total issues descending
        return dict(sorted(result.items(), key=lambda x: x[1]['total_issues'], reverse=True))

    def group_by_transaction(self) -> Dict[str, Dict[str, Any]]:
        """Group SQL records by transaction"""
        from collections import defaultdict

        transactions = defaultdict(lambda: {
            'total_queries': 0,
            'total_issues': 0,
            'total_agents': set(),
            'performance_scores': [],
            'max_severity': None,
            'severity_order': {}
        })

        for record in self.records:
            trans_name = record.transaction_name
            transactions[trans_name]['total_queries'] += 1
            transactions[trans_name]['total_agents'].add(record.agent_id)

            perf_score = record.summary.get('performance_score', 0)
            transactions[trans_name]['performance_scores'].append(perf_score)

            issues = record.issues
            transactions[trans_name]['total_issues'] += len(issues)

            # Track max severity
            if record.max_severity:
                severity_order = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
                current_severity = severity_order.get(record.max_severity, 0)
                max_sev = transactions[trans_name]['severity_order'].get(trans_name, 0)
                if current_severity > max_sev:
                    transactions[trans_name]['max_severity'] = record.max_severity
                    transactions[trans_name]['severity_order'][trans_name] = current_severity

        # Calculate averages and convert sets
        result = {}
        for trans_name, data in transactions.items():
            perf_scores = data['performance_scores']
            avg_perf = sum(perf_scores) / len(perf_scores) if perf_scores else 0

            result[trans_name] = {
                'total_queries': data['total_queries'],
                'total_issues': data['total_issues'],
                'total_agents': len(data['total_agents']),
                'avg_performance': round(avg_perf, 1),
                'max_severity': data['max_severity']
            }

        # Sort by total issues descending
        return dict(sorted(result.items(), key=lambda x: x[1]['total_issues'], reverse=True))

    def calculate_performance_distribution(self) -> Dict[str, int]:
        """Calculate distribution of performance scores"""
        distribution = {
            'very_poor': 0,  # 1-2
            'poor': 0,       # 3-4
            'fair': 0,       # 5-6
            'good': 0,       # 7-8
            'excellent': 0   # 9-10
        }

        for record in self.records:
            score = record.summary.get('performance_score', 0)

            if score <= 2:
                distribution['very_poor'] += 1
            elif score <= 4:
                distribution['poor'] += 1
            elif score <= 6:
                distribution['fair'] += 1
            elif score <= 8:
                distribution['good'] += 1
            else:
                distribution['excellent'] += 1

        return distribution

    def get_top_agents(self, limit: int = 10) -> tuple[List[str], List[int]]:
        """Get top agents with most issues"""
        from collections import defaultdict

        agent_issues = defaultdict(int)

        for record in self.records:
            agent_id = record.agent_id
            issues = record.issues
            agent_issues[agent_id] += len(issues)

        # Sort by issue count
        sorted_agents = sorted(agent_issues.items(), key=lambda x: x[1], reverse=True)[:limit]

        labels = [agent[0] for agent in sorted_agents]
        data = [agent[1] for agent in sorted_agents]

        return labels, data


# ============================================================================
# Report Generator
# ============================================================================

class ReportGenerator:
    """Generate HTML report using Jinja2"""

    def __init__(self, template_dir: Path):
        self.template_dir = Path(template_dir)
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def generate(self,
                 records: List[SQLRecord],
                 output_file: Path,
                 template_name: str = TEMPLATE_NAME) -> str:
        """Generate HTML report from records"""
        print(f"📊 Calculating statistics...")

        calc = StatisticsCalculator(records)
        overall_stats = calc.calculate_overall_stats()
        agents = calc.group_by_agent()
        transactions = calc.group_by_transaction()
        performance_dist = calc.calculate_performance_distribution()
        top_agents_labels, top_agents_data = calc.get_top_agents()

        # Separate SQLs with and without issues
        sqls_with_issues_list = [r for r in records if r.issues]

        # Convert SQLRecord objects to dictionaries for JSON serialization
        # Only include essential fields for the report to keep file size small
        from dataclasses import asdict
        records_dict = []
        for record in records:
            # Only include essential fields for the report
            record_dict = {
                'agent_id': record.agent_id,
                'transaction_name': record.transaction_name,
                'query_number': record.query_number,
                'query_type': record.query_type,
                'execution_count': record.execution_count,
                'total_rows': record.total_rows,
                'sql_query': record.sql_query,
                'summary': record.summary,
                'issues': record.issues,
                'avg_duration_ms': record.avg_duration_ms,
            }
            records_dict.append(record_dict)

        sqls_with_issues_dict = []
        for r in sqls_with_issues_list:
            sqls_with_issues_dict.append({
                'agent_id': r.agent_id,
                'transaction_name': r.transaction_name,
                'query_number': r.query_number,
                'sql_query': r.sql_query,
                'summary': r.summary,
                'issues': r.issues,
            })

        print(f"✓ Total SQLs: {overall_stats['total_sqls']}")
        print(f"✓ SQLs with issues: {overall_stats['sqls_with_issues']}")
        print(f"✓ Total issues: {overall_stats['total_issues']}")

        # Load template
        print(f"🎨 Rendering template: {template_name}")
        template = self.env.get_template(template_name)

        # Prepare template data
        template_data = {
            'generated_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'overall_stats': overall_stats,
            'all_sqls': records_dict,
            'sqls_with_issues_list': sqls_with_issues_dict,
            'agents': agents,
            'transactions': transactions,
            'performance_distribution': performance_dist,
            'top_agents_labels': top_agents_labels,
            'top_agents_data': top_agents_data
        }

        # Render
        html_content = template.render(**template_data)

        # Write output
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        file_size = output_file.stat().st_size / (1024 * 1024)  # MB

        print(f"\n{'='*60}")
        print(f"✅ Report generated successfully!")
        print(f"{'='*60}")
        print(f"📄 Output: {output_file.absolute()}")
        print(f"📊 File size: {file_size:.1f} MB")
        print(f"🌐 Open: file://{output_file.absolute()}")
        print(f"{'='*60}\n")

        return str(output_file.absolute())


# ============================================================================
# Main
# ============================================================================

def generate_report(outputs_dir: str,
                   output_file: str = None,
                   template_name: str = TEMPLATE_NAME) -> str:
    """
    Main entry point to generate SQL review report

    Args:
        outputs_dir: Path to outputs directory
        output_file: Output HTML file path (optional)
        template_name: Name of Jinja2 template file (optional)

    Returns:
        Path to generated report file
    """
    outputs_path = Path(outputs_dir)

    if not outputs_path.exists():
        print(f"❌ Error: Directory not found: {outputs_path}")
        return ""

    if output_file is None:
        output_file = outputs_path / DEFAULT_OUTPUT
    else:
        output_file = Path(output_file)

    # Get template directory (scripts/templates relative to this script)
    template_dir = Path(__file__).parent.parent / "templates"

    # Load data
    loader = DataLoader(outputs_path)
    records = loader.load_all()

    if not records:
        print("❌ Error: No SQL records found")
        return ""

    # Generate report
    generator = ReportGenerator(template_dir)
    return generator.generate(records, output_file, template_name)


if __name__ == "__main__":
    import sys

    # Default outputs directory
    outputs_dir = Path(__file__).parent.parent / "outputs"

    if len(sys.argv) > 1:
        outputs_dir = sys.argv[1]

    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]

    template_name = TEMPLATE_NAME
    if len(sys.argv) > 3:
        template_name = sys.argv[3]

    generate_report(outputs_dir, output_file, template_name)
