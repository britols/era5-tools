#!/usr/bin/env python3
"""
ERA5 downloader with support for multiple requests per API key.

This is a simplified version of weather-dl optimized for local downloads. It uses ThreadPoolExecutor instead of Apache Beam's DirectRunner for parallel downloads with

Usage:
    python era5_local_downloader.py <config_file> [options]

Options:
    --requests-per-key N    Number of concurrent requests per API key (default: 5)
    --dry-run               Don't actually download, just show what would be done
"""

'''
#for longer downloads, use nohup:
nohup .venv/bin/python era5_downloader.py examples/download_hourly_data_by_day/era5_hourly.cfg API_KEY  > era5_download_$(date +%Y%m%d_%H%M%S).log 2>&1 &
'''
import argparse
import configparser
import itertools
import json
import logging
import os
import sys
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from threading import Lock

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s %(asctime)s %(name)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class APIKey:
    '''
        Represents an API key configuration.
        APIKey(
        name='' #name is 'a' if API is in [parameters.a], 
        api_url='', 
        api_key='', 
        api_email='')
    '''
    name: str
    api_url: str
    api_key: str
    api_email: str


@dataclass
class DownloadConfig:
    """Main configuration for downloads."""
    client: str
    dataset: str
    target_path: str
    partition_keys: List[str]
    selection: Dict[str, Any]
    api_keys: List[APIKey]


@dataclass
class Partition:
    """Represents a single download partition."""
    target_path: str
    selection: Dict[str, Any]
    api_key: APIKey
    partition_id: str

def get_api_keys(api_key_file: str) -> List[str]:

    with open(api_key_file, 'r') as f:
        parser = configparser.ConfigParser()
        parser.read_file(f)
        keys = {s: dict(parser.items(s)) for s in parser.sections()}

    # Extract API keys from subsections
    api_keys = []
    for section_name, section_data in keys.items():
        if section_name.startswith('key.'):
            key_name = section_name.split('.', 1)[1]
            api_keys.append(APIKey(
                name=key_name,
                api_url=section_data.get('api_url', ''),
                api_key=section_data.get('api_key', ''),
                api_email=section_data.get('api_email', '')
            ))

    if not api_keys: #If is empty
        # If no subsections, create a default one from environment
        api_keys.append(APIKey(
            name='default',
            api_url=os.environ.get('CDSAPI_URL', params.get('api_url', '')),
            api_key=os.environ.get('CDSAPI_KEY', params.get('api_key', '')),
            api_email=params.get('api_email', '')
        ))

    return api_keys

def parse_config_file(config_path: str) -> DownloadConfig:
    """Parse configuration file (*.cfg or *.json)."""
    with open(config_path, 'r') as f:
        if config_path.endswith('.json'):
            config = json.load(f)
        else:
            parser = configparser.ConfigParser()
            parser.read_file(f)
            config = {s: dict(parser.items(s)) for s in parser.sections()}

    # Parse parameters section
    params = config.get('parameters', {})
    client = params.get('client', 'cds')
    dataset = params.get('dataset', '')
    target_path = params.get('target_path', '')
    partition_keys_raw = params.get('partition_keys', '')

    # Parse partition keys
    if isinstance(partition_keys_raw, str):
        partition_keys = [k.strip() for k in partition_keys_raw.strip().splitlines() if k.strip()]
    else:
        partition_keys = partition_keys_raw

    # Parse selection section
    selection = {}
    if 'selection' in config:
        for key, value in config['selection'].items():
            if isinstance(value, str):
                if '\n' in value:
                    # Multi-line list
                    selection[key] = [v.strip() for v in value.strip().splitlines() if v.strip()]
                elif '/' in value:
                    # MARS range syntax like "2024-01-01/to/2024-03-20" or "00/to/23"
                    # Parse for ALL keys, not just partition keys
                    parsed = parse_mars_range(value)
                    # If it's a partition key, keep as list; otherwise join back
                    if key in partition_keys:
                        selection[key] = parsed
                    else:
                        # For non-partition keys like 'time', keep as list for API
                        selection[key] = parsed
                else:
                    selection[key] = value
            else:
                selection[key] = value

    return DownloadConfig(
        client=client,
        dataset=dataset,
        target_path=target_path,
        partition_keys=partition_keys,
        selection=selection,
        api_keys=[]
    )


def parse_mars_range(range_str: str) -> List[str]:
    """Parse MARS range syntax like '2024-01-01/to/2024-03-20' or '00/to/23'."""
    from datetime import datetime, timedelta

    parts = range_str.split('/')
    if 'to' not in parts:
        return [range_str]

    to_idx = parts.index('to')
    start_str = parts[to_idx - 1]
    end_str = parts[to_idx + 1]

    # Check for 'by' increment
    increment = 1
    if 'by' in parts:
        by_idx = parts.index('by')
        increment = int(parts[by_idx + 1])

    # Try to parse as dates first
    try:
        start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_str, '%Y-%m-%d').date()

        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=increment)
        return dates
    except ValueError:
        pass

    # Try to parse as integers (e.g., hours 00/to/23, months 01/to/12)
    try:
        start_int = int(start_str)
        end_int = int(end_str)

        # Preserve leading zeros if present
        width = len(start_str)
        result = []
        current = start_int
        while current <= end_int:
            result.append(str(current).zfill(width))
            current += increment
        return result
    except ValueError:
        pass

    # If all parsing fails, return as-is
    return [range_str]


def format_target_path(template: str, partition_dict: Dict[str, Any]) -> str:
    """Format target path handling datetime and numeric format specifiers."""
    import re
    from datetime import datetime

    # Replace %% with % first (configparser doubles them)
    template = template.replace('%%', '%')

    # Find all format placeholders like {date:%Y%m%d} or {month:02d}
    pattern = r'\{(\w+)(?::([^}]+))?\}'

    def replacer(match):
        key = match.group(1)
        format_spec = match.group(2)
        value = partition_dict.get(key, '')

        if format_spec:
            if key in ['date', 'time']:
                # Parse the date/time value and format it
                try:
                    if key == 'date':
                        # Try to parse date in various formats
                        for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y-%j']:
                            try:
                                dt = datetime.strptime(str(value), fmt)
                                return dt.strftime(format_spec)
                            except ValueError:
                                continue
                    # If parsing fails, just return the value
                    return str(value)
                except Exception:
                    return str(value)
            else:
                # For numeric values like {month:02d}, use Python's format
                try:
                    # Try to format as integer first
                    int_value = int(value)
                    return f'{int_value:{format_spec}}'
                except (ValueError, TypeError):
                    # Fall back to string formatting
                    return f'{value:{format_spec}}'
        else:
            return str(value)

    return re.sub(pattern, replacer, template)


def optimize_selection_for_monthly(selection: Dict[str, Any], partition_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Optimize selection when using year/month partitioning or year-only partitioning with day=all.

    Cases:
    1. Year+Month partitioning: year=2019, month=01, day=all -> date=2019-01-01/2019-01-31
    2. Year-only partitioning: year=2019, month=01/to/12, day=all -> date=2019-01-01/2019-12-31

    Note: CDS API expects "start_date/end_date" format (no '/to/')
    """
    import calendar
    from datetime import datetime

    result = selection.copy()

    # Check if we're using day=all
    if 'day' in result and result.get('day') == 'all':
        # Case 1: Year+Month partitioning (e.g., monthly downloads)
        if 'year' in partition_dict and 'month' in partition_dict:
            year = int(partition_dict['year'])
            month = int(partition_dict['month'])

            # Get number of days in this month
            _, n_days = calendar.monthrange(year, month)

            # Create date range for the full month
            # CDS API format: "start_date/end_date" (no '/to/')
            result['date'] = f'{year:04d}-{month:02d}-01/{year:04d}-{month:02d}-{n_days:02d}'

            # Remove year, month, day from the request
            result.pop('year', None)
            result.pop('month', None)
            result.pop('day', None)

        # Case 2: Year-only partitioning (e.g., yearly downloads)
        elif 'year' in partition_dict and 'month' in result:
            year = int(partition_dict['year'])

            # Create date range for the full year
            # CDS API format: "start_date/end_date" (no '/to/')
            result['date'] = f'{year:04d}-01-01/{year:04d}-12-31'

            # Remove year, month, day from the request
            result.pop('year', None)
            result.pop('month', None)
            result.pop('day', None)

    return result


def create_partitions(config: DownloadConfig) -> List[Partition]:
    """Create all download partitions from configuration."""
    # Build cartesian product of partition keys
    partition_values = []
    for key in config.partition_keys:
        values = config.selection.get(key, [])
        if not isinstance(values, list):
            values = [values]
        partition_values.append([(key, v) for v in values])

    # Create all combinations
    partitions = []
    api_key_cycle = itertools.cycle(config.api_keys)

    for combo in itertools.product(*partition_values):
        # Build selection for this partition
        partition_selection = config.selection.copy()
        partition_dict = {}
        for key, value in combo:
            partition_selection[key] = value
            partition_dict[key] = value

        # Optimize selection for monthly downloads (handles day=all)
        partition_selection = optimize_selection_for_monthly(partition_selection, partition_dict)

        # Format target path with proper date formatting
        target_path = format_target_path(config.target_path, partition_dict)

        # Assign to next API key in round-robin
        api_key = next(api_key_cycle)

        partition_id = '_'.join(str(v) for k, v in combo)
        partitions.append(Partition(
            target_path=target_path,
            selection=partition_selection,
            api_key=api_key,
            partition_id=partition_id
        ))

    return partitions


def download_partition(partition: Partition, config: DownloadConfig, dry_run: bool = False, force_download: bool = False) -> Tuple[bool, str]:
    """Download a single partition using CDS API."""
    worker_name = f"{partition.api_key.name}"

    try:
        # Check if file already exists (unless force_download is enabled)
        if not force_download and os.path.exists(partition.target_path) and os.path.getsize(partition.target_path) > 0:
            logger.debug(f"[{worker_name}] Skipping existing file: {partition.target_path}")
            return (True, 'skipped')

        # Create target directory if needed
        os.makedirs(os.path.dirname(partition.target_path), exist_ok=True)

        if dry_run:
            logger.info(f"[{worker_name}] DRY RUN: Would download {partition.target_path}")
            time.sleep(0.1)  # Simulate some work
            return (True, 'completed')

        # Track timing for this request
        request_start = time.time()

        # Import CDS API
        try:
            import cdsapi
        except ImportError:
            logger.error("cdsapi not installed. Install with: pip install cdsapi")
            return (False, 'failed')

        # Create CDS client
        c = cdsapi.Client(
            url=partition.api_key.api_url,
            key=partition.api_key.api_key,
            verify=True
        )

        logger.info(f"[{worker_name}] Downloading {partition.target_path}")

        # Perform download
        with tempfile.NamedTemporaryFile(delete=False, suffix='.nc') as tmp:
            tmp_path = tmp.name

        try:
            c.retrieve(
                config.dataset,
                partition.selection,
                tmp_path
            )

            # Move to final location
            os.rename(tmp_path, partition.target_path)
            logger.info(f"[{worker_name}] Completed {partition.target_path}")
            return (True, 'completed')

        finally:
            # Clean up temp file if it still exists
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except Exception as e:
        logger.error(f"[{worker_name}] Failed to download {partition.target_path}: {e}")
        return (False, 'failed')


def format_time_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if secs > 0 or not parts:
        parts.append(f"{secs} second{'s' if secs != 1 else ''}")

    return " ".join(parts)

class DownloadStats:
    """Thread-safe download statistics."""
    def __init__(self):
        self.lock = Lock()
        self.total = 0
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = None
        self.by_key = defaultdict(lambda: {'completed': 0, 'failed': 0})

    def start(self, total: int):
        with self.lock:
            self.total = total
            self.start_time = time.time()

    def increment(self, status: str, key_name: str = None):
        with self.lock:
            if status == 'completed':
                self.completed += 1
                if key_name:
                    self.by_key[key_name]['completed'] += 1
            elif status == 'failed':
                self.failed += 1
                if key_name:
                    self.by_key[key_name]['failed'] += 1
            elif status == 'skipped':
                self.skipped += 1

    def get_progress(self) -> str:
        with self.lock:
            done = self.completed + self.failed + self.skipped
            pct = (done / self.total * 100) if self.total > 0 else 0
            elapsed = time.time() - self.start_time if self.start_time else 0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (self.total - done) / rate if rate > 0 else 0

            return (f"Progress: {done}/{self.total} ({pct:.1f}%) | "
                   f"Completed: {self.completed} | Failed: {self.failed} | Skipped: {self.skipped} | "
                   f"Rate: {rate:.2f}/s | ETA: {eta:.0f}s")


def run_downloads(config: DownloadConfig, partitions: List[Partition],
                 requests_per_key: int, dry_run: bool = False, force_download: bool = False):
    """Run downloads with proper parallelization.

    Note: requests_per_key limits how many requests we keep "in-flight"
    (submitted to ECMWF) per API key. ECMWF's scheduler processes these
    requests in parallel on their side. Start with 5 and increase gradually
    to avoid rate limits.
    """
    # Track total execution time
    start_time = time.time()

    # Group partitions by API key
    partitions_by_key = defaultdict(list)
    for partition in partitions:
        partitions_by_key[partition.api_key.name].append(partition)

    stats = DownloadStats()
    stats.start(len(partitions))

    logger.info(f"Starting downloads:")
    logger.info(f"  Total partitions: {len(partitions)}")
    logger.info(f"  API keys: {len(config.api_keys)}")
    logger.info(f"  Max concurrent requests per key: {requests_per_key}")
    logger.info(f"  Max total concurrent requests: {len(config.api_keys) * requests_per_key}")

    for key_name, key_partitions in partitions_by_key.items():
        logger.info(f"  [{key_name}]: {len(key_partitions)} partitions")

    # Create a thread pool - each thread submits a request to ECMWF and waits for it
    # This limits how many requests we keep "in-flight" at once
    max_workers = len(config.api_keys) * requests_per_key

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all downloads
        future_to_partition = {}
        for partition in partitions:
            future = executor.submit(download_partition, partition, config, dry_run, force_download)
            future_to_partition[future] = partition

        # Process completed downloads
        last_update = time.time()
        for future in as_completed(future_to_partition):
            partition = future_to_partition[future]
            success, status = future.result()
            stats.increment(status, partition.api_key.name)

            # Print progress every 5 seconds
            if time.time() - last_update > 5:
                logger.info(stats.get_progress())
                last_update = time.time()

    # Calculate total time
    total_time = time.time() - start_time
    time_str = format_time_duration(total_time)

    # Final stats
    logger.info("=" * 80)
    logger.info("Download complete!")
    logger.info(stats.get_progress())
    logger.info(f"Total execution time: {time_str}")
    logger.info("-" * 80)
    logger.info("Stats by API key:")
    for key_name, key_stats in stats.by_key.items():
        logger.info(f"  [{key_name}]: {key_stats['completed']} completed, {key_stats['failed']} failed")


def main():
    parser = argparse.ArgumentParser(
        description='Local ERA5 downloader with full multi-key and multi-request support'
    )
    parser.add_argument('config', help='Path to configuration file (*.cfg or *.json)')
    parser.add_argument('api', help='Path to api keys file')
    parser.add_argument('--requests-per-key', '-n', type=int, default=5,
                       help='Number of concurrent requests per API key (default: 5, max: 20)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be downloaded without actually downloading')
    parser.add_argument('--force-download', '-f', action='store_true',
                       help='Force re-download of files even if they already exist')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    # Parse configuration
    logger.info(f"Loading configuration from {args.config}")
    config = parse_config_file(args.config)
    api_keys = get_api_keys(args.api)
    config.api_keys  = api_keys
    logger.info(f"Configuration loaded:")
    logger.info(f"  Client: {config.client}")
    logger.info(f"  Dataset: {config.dataset}")
    logger.info(f"  API keys: {len(config.api_keys)}")
    logger.info(f"  Partition keys: {config.partition_keys}")

    # Create partitions
    logger.info("Creating partitions...")
    partitions = create_partitions(config)
    logger.info(f"Created {len(partitions)} partitions")

    # Show force-download warning if enabled
    if args.force_download:
        logger.warning("Force download enabled - existing files will be re-downloaded")

    # Run downloads
    run_downloads(config, partitions, args.requests_per_key, args.dry_run, args.force_download)


if __name__ == '__main__':
    main()
