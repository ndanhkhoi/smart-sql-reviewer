"""
Z.ai API SQL Reviewer - Reviews SQL queries using Z.ai API.
"""

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..utils import (
    setup_logger,
    ensure_dir,
    print_section,
    format_number,
    create_log_file_path
)


class ZAiSQLReviewer:
    """
    Reviews SQL queries using Z.ai API.

    Attributes:
        config: Configuration dictionary
        api_url: Z.ai API URL
        api_key: Z.ai API key
        model: Model name for API requests
        system_prompt: System prompt from review_prompt.txt
        sql_dir: Directory containing SQL files
        sql_info_dir: Directory containing SQL info files
        metadata_dir: Directory containing metadata files
        review_dir: Directory to save review results
        max_workers: Number of parallel API calls
        max_retries: Maximum number of retry attempts
        initial_retry_delay: Initial retry delay in seconds
        max_retry_delay: Maximum retry delay in seconds
        logger: Logger instance
        stats: Statistics about the review operation
        stats_lock: Thread lock for parallel processing
    """

    # CJK Unicode ranges
    CJK_RANGES = [
        (0x4E00, 0x9FFF),   # CJK Unified Ideographs
        (0x3040, 0x309F),   # Hiragana
        (0x30A0, 0x30FF),   # Katakana
        (0xAC00, 0xD7AF),   # Hangul Syllables
        (0x1100, 0x11FF),   # Hangul Jamo
    ]

    def __init__(self, config: Dict):
        """
        Initialize the reviewer with configuration.

        Args:
            config: Configuration dictionary containing:
                - api_url: Z.ai API URL (or use ZAI_API_URL env var)
                - api_key: Z.ai API key (or use ZAI_API_KEY env var)
                - model: Model name (default: glm-4.5)
                - system_prompt_file: Path to system prompt file
                - output: Output configuration
                - logging: Logging configuration
                - review: Review-specific configuration
        """
        self.config = config

        # API Configuration
        review_config = config.get("review", {})
        self.api_url = os.getenv(
            "ZAI_API_URL",
            review_config.get("api_url", "https://api.z.ai/api/coding/paas/v4/chat/completions")
        )
        self.api_key = os.getenv("ZAI_API_KEY", review_config.get("api_key", ""))
        self.model = review_config.get("model", "glm-4.6")

        if not self.api_key:
            raise ValueError(
                "ZAI_API_KEY environment variable or review.api_key config is required"
            )

        # Setup project root (needed for multiple paths)
        project_root = Path(__file__).parent.parent.parent

        # Setup output directories from centralized config
        output_config = config.get("output", {})
        base_output = Path(output_config.get("base_dir", "outputs"))
        self.sql_dir = base_output / output_config.get("sql_dir", "fetchers/sql")
        self.sql_info_dir = base_output / output_config.get("sql_info_dir", "fetchers/sql_info")
        self.metadata_dir = base_output / output_config.get("metadata_dir", "metadata")
        self.review_dir = base_output / review_config.get("review_dir", "review")

        # Retry configuration
        self.max_retries = review_config.get("max_retries", 15)
        self.initial_retry_delay = review_config.get("initial_retry_delay", 2)
        self.max_retry_delay = review_config.get("max_retry_delay", 10)

        # Parallel processing config
        self.max_workers = review_config.get("max_workers", 3)

        # Setup logging with dynamic file naming
        logs_dir_name = output_config.get("logs_dir", "logs")
        self.log_file = create_log_file_path(project_root, "review", logs_dir_name)
        self.logger = setup_logger(
            "ZAiSQLReviewer",
            log_file=self.log_file,
            level=config["logging"].get("level", "INFO"),
            console_output=config["logging"].get("console_output", True),
            file_output=config["logging"].get("file_output", True)
        )

        self.logger.info(f"Log file: {self.log_file}")

        # System prompt file (load after logger is set up)
        system_prompt_path = review_config.get(
            "system_prompt_file",
            project_root / "resources" / "review_prompt.txt"
        )
        self.system_prompt = self._load_system_prompt(system_prompt_path)

        # Statistics with thread lock for parallel processing
        self.stats = {
            "total_files": 0,
            "successful_reviews": 0,
            "failed_reviews": 0,
            "skipped_files": 0,
            "total_retries": 0,
            "total_tokens_used": 0,
            "total_review_time": 0.0,
            # Token usage details
            "prompt_tokens": 0,           # Input tokens
            "completion_tokens": 0,       # Output tokens
            "reasoning_tokens": 0,        # Reasoning tokens
            "cached_tokens": 0,           # Cached tokens
        }
        self.stats_lock = threading.Lock()

    def _load_system_prompt(self, prompt_file: Path) -> str:
        """
        Load system prompt from file.

        Args:
            prompt_file: Path to system prompt file

        Returns:
            System prompt content

        Raises:
            FileNotFoundError: If prompt file not found
        """
        prompt_file = Path(prompt_file)
        if not prompt_file.exists():
            raise FileNotFoundError(f"System prompt file not found: {prompt_file}")

        with open(prompt_file, "r", encoding="utf-8") as f:
            content = f.read()
        self.logger.info(f"Loaded system prompt from {prompt_file}")
        return content

    def _is_cjk(self, char: str) -> bool:
        """
        Check if a character is in the CJK Unicode ranges.

        Args:
            char: Single character to check

        Returns:
            True if character is CJK, False otherwise
        """
        codepoint = ord(char)
        for start, end in self.CJK_RANGES:
            if start <= codepoint <= end:
                return True
        return False

    def _clean_cjk_in_json(self, data: Any) -> Any:
        """
        Recursively clean CJK characters from JSON-compatible data.

        Args:
            data: JSON-compatible data (dict, list, str, or primitive)

        Returns:
            Cleaned data with CJK characters removed from string values
        """
        if isinstance(data, dict):
            return {k: self._clean_cjk_in_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._clean_cjk_in_json(item) for item in data]
        elif isinstance(data, str):
            return "".join(char for char in data if not self._is_cjk(char))
        else:
            return data

    def _read_sql_file(self, sql_path: Path) -> str:
        """
        Read SQL file content.

        Args:
            sql_path: Path to SQL file

        Returns:
            SQL content as string

        Raises:
            FileNotFoundError: If file not found
            IOError: If file cannot be read
        """
        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read().strip()

    def _read_sql_info_file(self, info_path: Path) -> Dict[str, Any]:
        """
        Read SQL info JSON file.

        Args:
            info_path: Path to SQL info file

        Returns:
            SQL info dictionary

        Raises:
            FileNotFoundError: If file not found
            json.JSONDecodeError: If file is not valid JSON
        """
        with open(info_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _read_metadata_file(self, metadata_path: Path) -> Dict[str, Any]:
        """
        Read metadata JSON file.

        Args:
            metadata_path: Path to metadata file

        Returns:
            Metadata dictionary, or empty dict if file not found
        """
        if not metadata_path.exists():
            self.logger.warning(f"Metadata file not found: {metadata_path}")
            return {}

        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"Failed to read metadata file {metadata_path}: {e}")
            return {}

    def _call_zai_api(self, payload: Dict[str, Any], filename: str) -> Optional[Dict[str, Any]]:
        """
        Call Z.ai API with retry logic.

        Args:
            payload: Request payload to send to API
            filename: SQL filename for logging

        Returns:
            API response content, or None if all retries failed

        Raises:
            requests.exceptions.RequestException: If API request fails
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        retry_count = 0
        delay = self.initial_retry_delay

        while retry_count <= self.max_retries:
            try:
                self.logger.debug(f"Calling API for {filename} (attempt {retry_count + 1}/{self.max_retries + 1})")

                response = requests.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                    timeout=120
                )

                response.raise_for_status()
                result = response.json()

                # Extract usage info
                usage = result.get("usage", {})
                prompt_tokens = usage.get("prompt_tokens", 0)
                completion_tokens = usage.get("completion_tokens", 0)
                total_tokens = usage.get("total_tokens", 0)

                # Extract detailed token usage
                prompt_details = usage.get("prompt_tokens_details", {})
                cached_tokens = prompt_details.get("cached_tokens", 0)

                completion_details = usage.get("completion_tokens_details", {})
                reasoning_tokens = completion_details.get("reasoning_tokens", 0)

                with self.stats_lock:
                    self.stats["total_tokens_used"] += total_tokens
                    self.stats["prompt_tokens"] += prompt_tokens
                    self.stats["completion_tokens"] += completion_tokens
                    self.stats["reasoning_tokens"] += reasoning_tokens
                    self.stats["cached_tokens"] += cached_tokens

                self.logger.debug(
                    f"API call succeeded for {filename}: "
                    f"{prompt_tokens} prompt + {completion_tokens} completion = {total_tokens} total tokens "
                    f"(reasoning: {reasoning_tokens}, cached: {cached_tokens})"
                )

                return result

            except requests.exceptions.Timeout as e:
                retry_count += 1
                with self.stats_lock:
                    self.stats["total_retries"] += 1

                if retry_count > self.max_retries:
                    self.logger.error(f"API timeout for {filename} after {retry_count} attempts: {e}")
                    return None

                self.logger.warning(f"API timeout for {filename} (attempt {retry_count}): {e}")

            except requests.exceptions.HTTPError as e:
                retry_count += 1
                with self.stats_lock:
                    self.stats["total_retries"] += 1

                if retry_count > self.max_retries:
                    self.logger.error(f"API HTTP error for {filename} after {retry_count} attempts: {e}")
                    return None

                self.logger.warning(f"API HTTP error for {filename} (attempt {retry_count}): {e}")

            except requests.exceptions.RequestException as e:
                retry_count += 1
                with self.stats_lock:
                    self.stats["total_retries"] += 1

                if retry_count > self.max_retries:
                    self.logger.error(f"API request failed for {filename} after {retry_count} attempts: {e}")
                    return None

                self.logger.warning(f"API request error for {filename} (attempt {retry_count}): {e}")

            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse API response for {filename}: {e}")
                return None

            # Exponential backoff with max delay
            sleep_time = min(delay, self.max_retry_delay)
            self.logger.debug(f"Retrying in {sleep_time}s...")
            time.sleep(sleep_time)
            delay *= 2

        return None

    def _review_single_sql(self, sql_path: Path, idx: int, total: int) -> Tuple[str, Dict[str, Any]]:
        """
        Review a single SQL file using Z.ai API (thread-safe).

        Args:
            sql_path: Path to SQL file
            idx: File index for logging
            total: Total number of files for logging

        Returns:
            Tuple of (filename, result_dict)
        """
        start_time = time.perf_counter()
        try:
            self.logger.info(f"[{idx}/{total}] Reviewing: {sql_path.name}")

            # Read SQL content first to check for ALTER SESSION
            sql = self._read_sql_file(sql_path)

            # Skip ALTER SESSION commands - create fake review result
            sql_upper = sql.strip().upper()
            if sql_upper.startswith('ALTER SESSION') or sql_upper.startswith('ALTER\tSESSION'):
                # Create fake review result for ALTER SESSION command (matching z.ai API schema)
                fake_review = {
                    "summary": {
                        "performance_score": 10,
                        "complexity_score": 0,
                        "total_issues": 0,
                        "by_severity": {
                            "critical": 0,
                            "high": 0,
                            "medium": 0,
                            "low": 0
                        },
                        "by_category": {
                            "performance": 0,
                            "nplus1": 0,
                            "hibernate": 0,
                            "code_quality": 0,
                            "index": 0
                        },
                        "overall_assessment": "Đây là lệnh ALTER SESSION dùng để cấu hình tham số tại mức session (ví dụ: SET CURRENT_SCHEMA). Không phải là lệnh truy vấn dữ liệu (SELECT/INSERT/UPDATE/DELETE) nên không cần đánh giá hiệu suất. Không có vấn đề về performance, không cần thêm index, không cần tối ưu.",
                        "priority": "low",
                        "effort_to_fix": "low"
                    },
                    "issues": []
                }

                # Save fake review result
                review_path = self.review_dir / sql_path.with_suffix(".json").name
                with open(review_path, "w", encoding="utf-8") as f:
                    json.dump(fake_review, f, indent=2, ensure_ascii=False)

                result = {
                    "status": "success",
                    "issues": 0,
                    "performance_score": 10,
                    "skipped": True
                }

                elapsed_time = time.perf_counter() - start_time
                self.logger.info(f"[{idx}/{total}] ✓ Skipped {sql_path.name}: ALTER SESSION command (not a query) - Reviewed in {elapsed_time:.2f}s")
                return (sql_path.name, result)

            # Get corresponding info and metadata files
            info_path = self.sql_info_dir / sql_path.with_suffix(".json").name
            metadata_path = self.metadata_dir / sql_path.with_suffix(".json").name

            # Check if info file exists
            if not info_path.exists():
                result = {
                    "status": "skipped",
                    "reason": "SQL info file not found"
                }
                elapsed_time = time.perf_counter() - start_time
                self.logger.warning(f"[{idx}/{total}] ⚠ Skipped {sql_path.name}: info file not found - Reviewed in {elapsed_time:.2f}s")
                return (sql_path.name, result)

            # Read SQL info
            sql_info = self._read_sql_info_file(info_path)

            # Read metadata
            metadata = self._read_metadata_file(metadata_path)

            # Build API request payload
            user_content = json.dumps({
                "sql": sql,
                "sql_info": sql_info,
                "metadata": metadata
            }, ensure_ascii=False)

            payload = {
                "model": self.model,
                "messages": [
                    {
                        "role": "system",
                        "content": self.system_prompt
                    },
                    {
                        "role": "user",
                        "content": user_content
                    }
                ],
                "max_tokens": 4096,
                "temperature": 0.1,
                "top_p": 1.0,
                "frequency_penalty": 0.0,
                "presence_penalty": 0.0,
                "response_format": {
                    "type": "json_object"
                },
                "thinking": {
                    "type": "enabled"
                }
            }

            # Retry loop for API call + response processing
            processing_retry_count = 0
            processing_delay = self.initial_retry_delay
            last_error = None

            while processing_retry_count <= self.max_retries:
                try:
                    # Call API
                    api_response = self._call_zai_api(payload, sql_path.name)

                    if api_response is None:
                        # API call failed after all its internal retries
                        last_error = "API call failed after all retries"
                        if processing_retry_count < self.max_retries:
                            # Retry the entire process
                            processing_retry_count += 1
                            with self.stats_lock:
                                self.stats["total_retries"] += 1
                            sleep_time = min(processing_delay, self.max_retry_delay)
                            self.logger.warning(
                                f"[{idx}/{total}] Processing retry {processing_retry_count}/{self.max_retries} "
                                f"for {sql_path.name}: API call failed, retrying in {sleep_time}s..."
                            )
                            time.sleep(sleep_time)
                            processing_delay *= 2
                            continue
                        else:
                            # All retries exhausted
                            result = {
                                "status": "failed",
                                "reason": last_error
                            }
                            elapsed_time = time.perf_counter() - start_time
                            self.logger.error(f"[{idx}/{total}] ✗ Failed to review {sql_path.name} after {processing_retry_count} processing retries - Reviewed in {elapsed_time:.2f}s")
                            return (sql_path.name, result)

                    # Extract review content from response
                    choices = api_response.get("choices", [])
                    if not choices:
                        last_error = "No choices in API response"
                        if processing_retry_count < self.max_retries:
                            processing_retry_count += 1
                            with self.stats_lock:
                                self.stats["total_retries"] += 1
                            sleep_time = min(processing_delay, self.max_retry_delay)
                            self.logger.warning(
                                f"[{idx}/{total}] Processing retry {processing_retry_count}/{self.max_retries} "
                                f"for {sql_path.name}: {last_error}, retrying in {sleep_time}s..."
                            )
                            time.sleep(sleep_time)
                            processing_delay *= 2
                            continue
                        else:
                            result = {
                                "status": "failed",
                                "reason": last_error
                            }
                            elapsed_time = time.perf_counter() - start_time
                            self.logger.error(f"[{idx}/{total}] ✗ No choices in API response for {sql_path.name} after {processing_retry_count} processing retries - Reviewed in {elapsed_time:.2f}s")
                            return (sql_path.name, result)

                    review_content = choices[0].get("message", {}).get("content", "")

                    # Parse review JSON
                    review_data = json.loads(review_content)

                    # Clean CJK characters from review data
                    cleaned_review = self._clean_cjk_in_json(review_data)

                    # Save review result
                    review_path = self.review_dir / sql_path.with_suffix(".json").name
                    with open(review_path, "w", encoding="utf-8") as f:
                        json.dump(cleaned_review, f, indent=2, ensure_ascii=False)

                    # Count issues
                    summary = cleaned_review.get("summary", {})
                    total_issues = summary.get("total_issues", 0)
                    performance_score = summary.get("performance_score", "N/A")

                    result = {
                        "status": "success",
                        "issues": total_issues,
                        "performance_score": performance_score
                    }

                    elapsed_time = time.perf_counter() - start_time
                    self.logger.info(
                        f"[{idx}/{total}] ✓ Reviewed {sql_path.name}: "
                        f"{total_issues} issues, score={performance_score} - Reviewed in {elapsed_time:.2f}s"
                    )

                    return (sql_path.name, result)

                except (json.JSONDecodeError, AttributeError, KeyError, TypeError) as e:
                    # Errors during response processing
                    last_error = f"Response processing error: {type(e).__name__}: {e}"
                    if processing_retry_count < self.max_retries:
                        processing_retry_count += 1
                        with self.stats_lock:
                            self.stats["total_retries"] += 1
                        sleep_time = min(processing_delay, self.max_retry_delay)
                        self.logger.warning(
                            f"[{idx}/{total}] Processing retry {processing_retry_count}/{self.max_retries} "
                            f"for {sql_path.name}: {last_error}, retrying in {sleep_time}s..."
                        )
                        time.sleep(sleep_time)
                        processing_delay *= 2
                    else:
                        result = {
                            "status": "failed",
                            "reason": last_error
                        }
                        elapsed_time = time.perf_counter() - start_time
                        self.logger.error(
                            f"[{idx}/{total}] ✗ Failed to process response for {sql_path.name} "
                            f"after {processing_retry_count} processing retries: {e} - Reviewed in {elapsed_time:.2f}s"
                        )
                        return (sql_path.name, result)

                except Exception as e:
                    # Unexpected errors during processing
                    last_error = f"Unexpected error during processing: {type(e).__name__}: {e}"
                    if processing_retry_count < self.max_retries:
                        processing_retry_count += 1
                        with self.stats_lock:
                            self.stats["total_retries"] += 1
                        sleep_time = min(processing_delay, self.max_retry_delay)
                        self.logger.warning(
                            f"[{idx}/{total}] Processing retry {processing_retry_count}/{self.max_retries} "
                            f"for {sql_path.name}: {last_error}, retrying in {sleep_time}s..."
                        )
                        time.sleep(sleep_time)
                        processing_delay *= 2
                    else:
                        result = {
                            "status": "failed",
                            "reason": last_error
                        }
                        elapsed_time = time.perf_counter() - start_time
                        self.logger.error(
                            f"[{idx}/{total}] ✗ Unexpected error processing {sql_path.name} "
                            f"after {processing_retry_count} processing retries: {e} - Reviewed in {elapsed_time:.2f}s"
                        )
                        return (sql_path.name, result)

        except Exception as e:
            result = {
                "status": "failed",
                "reason": str(e)
            }
            elapsed_time = time.perf_counter() - start_time
            self.logger.error(f"[{idx}/{total}] ✗ Error reviewing {sql_path.name}: {e} - Reviewed in {elapsed_time:.2f}s")
            return (sql_path.name, result)

    def run(self, clean_output: bool = False, limit: Optional[int] = None, files_to_review: Optional[List[str]] = None):
        """
        Main execution method - reviews all SQL files.

        Args:
            clean_output: Clean review directory before reviewing (default: False)
            limit: Only review the first N files (default: None = all files)
            files_to_review: List of specific filenames to review (default: None = all files).
                           Only files matching these names/patterns will be processed.
        """
        run_start_time = time.perf_counter()
        print_section("SQL Reviewer - Z.ai API Reviewer")

        # Create review directory
        ensure_dir(self.review_dir)

        # Clean output if requested
        if clean_output and self.review_dir.exists():
            self.logger.info("Cleaning review directory...")
            for file in self.review_dir.glob("*.json"):
                file.unlink()
            self.logger.info(f"Cleaned {len(list(self.review_dir.glob('*.json')))} review files")

        self.logger.info(f"Review directory: {self.review_dir.absolute()}")
        self.logger.info(f"API URL: {self.api_url}")
        self.logger.info(f"Model: {self.model}")
        self.logger.info(f"Max retries: {self.max_retries}")
        self.logger.info(f"Max parallel workers: {self.max_workers}")
        print("-" * 70)

        # Get all SQL files
        sql_files = sorted(list(self.sql_dir.glob("*.sql")))

        # Apply file filtering if specific files are requested
        if files_to_review is not None and files_to_review:
            original_count = len(sql_files)
            filtered_files = []

            for pattern in files_to_review:
                pattern_lower = pattern.lower()
                # Check if pattern matches any filename (case-insensitive)
                matches = [f for f in sql_files if pattern_lower in f.name.lower() or f.name.lower() == pattern_lower]
                filtered_files.extend(matches)

            # Remove duplicates while preserving order
            seen = set()
            sql_files = []
            for f in filtered_files:
                if f not in seen:
                    seen.add(f)
                    sql_files.append(f)

            if not sql_files:
                self.logger.error(f"No SQL files match the specified patterns: {files_to_review}")
                return

            self.logger.info(f"File filtering: Processing {len(sql_files)} of {original_count} files matching patterns: {', '.join(files_to_review)}")

        # Apply limit if specified
        if limit is not None and limit > 0:
            original_count = len(sql_files)
            sql_files = sql_files[:limit]
            self.logger.info(f"Limit mode: Processing first {len(sql_files)} of {original_count} files")

        total_files = len(sql_files)

        if total_files == 0:
            self.logger.warning(f"No SQL files found in {self.sql_dir}")
            return

        self.logger.info(f"Found {format_number(total_files)} SQL files to review")

        with self.stats_lock:
            self.stats["total_files"] = total_files

        # Review SQL files in parallel
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="zai_review") as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self._review_single_sql, sql_path, idx, total_files): sql_path
                for idx, sql_path in enumerate(sql_files, start=1)
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                sql_path = future_to_file[future]
                try:
                    filename, result = future.result()
                    results[filename] = result

                    # Update statistics
                    with self.stats_lock:
                        if result["status"] == "success":
                            self.stats["successful_reviews"] += 1
                        elif result["status"] == "skipped":
                            self.stats["skipped_files"] += 1
                        else:
                            self.stats["failed_reviews"] += 1

                except Exception as e:
                    results[sql_path.name] = {
                        "status": "failed",
                        "reason": str(e)
                    }
                    self.logger.error(f"Error processing {sql_path.name}: {e}")
                    with self.stats_lock:
                        self.stats["failed_reviews"] += 1

        # Calculate total review time
        total_run_time = time.perf_counter() - run_start_time
        with self.stats_lock:
            self.stats["total_review_time"] = total_run_time

        # Print summary
        self._print_summary()

    def _print_summary(self):
        """Print execution summary with statistics."""
        print_section("SUMMARY")

        self.logger.info(f"Total files processed: {format_number(self.stats['total_files'])}")
        self.logger.info(f"Successful reviews: {format_number(self.stats['successful_reviews'])}")
        self.logger.info(f"Failed reviews: {format_number(self.stats['failed_reviews'])}")
        self.logger.info(f"Skipped files: {format_number(self.stats['skipped_files'])}")
        self.logger.info(f"Total retries: {format_number(self.stats['total_retries'])}")
        self.logger.info(f"Total tokens used: {format_number(self.stats['total_tokens_used'])}")

        # Token usage details
        self.logger.info("")
        self.logger.info("Token Usage Details:")
        self.logger.info(f"  Input tokens (prompt):    {format_number(self.stats['prompt_tokens'])}")
        self.logger.info(f"  Output tokens (completion): {format_number(self.stats['completion_tokens'])}")
        self.logger.info(f"  Reasoning tokens:         {format_number(self.stats['reasoning_tokens'])}")
        self.logger.info(f"  Cached tokens:            {format_number(self.stats['cached_tokens'])}")

        # Calculate effective tokens (actual new tokens processed)
        effective_tokens = (
            self.stats['prompt_tokens'] - self.stats['cached_tokens'] +
            self.stats['completion_tokens']
        )
        self.logger.info(f"  Effective (new) tokens:   {format_number(effective_tokens)}")

        # Cache hit rate
        if self.stats['prompt_tokens'] > 0:
            cache_hit_rate = (self.stats['cached_tokens'] / self.stats['prompt_tokens']) * 100
            self.logger.info(f"  Cache hit rate:           {cache_hit_rate:.1f}%")

        # Format and display total review time
        total_time = self.stats['total_review_time']
        self.logger.info("")
        if total_time >= 60:
            minutes = int(total_time // 60)
            seconds = total_time % 60
            self.logger.info(f"Total review time: {minutes}m {seconds:.2f}s")
        else:
            self.logger.info(f"Total review time: {total_time:.2f}s")

        if self.stats['total_files'] > 0:
            success_rate = (self.stats['successful_reviews'] / self.stats['total_files']) * 100
            self.logger.info(f"Success rate: {success_rate:.1f}%")

        self.logger.info(f"Review directory: {self.review_dir.absolute()}")
        print("=" * 70)
