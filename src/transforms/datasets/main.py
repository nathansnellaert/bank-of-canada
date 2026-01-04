"""
Transform Bank of Canada raw series data into granular datasets.

Uses the mapping configuration in src/mappings/datasets.json to:
1. Load raw series data from data/raw/series/*.json
2. Pivot from skinny format to wide format (date as index, series as columns)
3. Upload each dataset as a separate Delta table
"""

import json
import re
import pyarrow as pa
from pathlib import Path
from collections import defaultdict

from subsets_utils import upload_data, publish, load_raw_json
from subsets_utils.environment import get_data_dir
from subsets_utils.r2 import is_cloud_mode

MAPPINGS_DIR = Path(__file__).parent.parent.parent / "mappings"


def normalize_date(date: str, frequency: str) -> str:
    """
    Normalize dates to ISO 8601 format based on frequency.

    Conversions:
    - daily: YYYY-MM-DD (keep as-is)
    - monthly: YYYY-MM-DD -> YYYY-MM
    - quarterly: 2004Q1 -> 2004-Q1, YYYY-MM-DD -> YYYY-QN
    - annual: YYYY-MM-DD -> YYYY
    """
    if not date:
        return date

    # Handle quarterly format: 2004Q1 -> 2004-Q1
    quarterly_match = re.match(r'^(\d{4})Q([1-4])$', date)
    if quarterly_match:
        return f"{quarterly_match.group(1)}-Q{quarterly_match.group(2)}"

    # Handle YYYY-MM-DD format
    daily_match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', date)
    if daily_match:
        year, month, day = daily_match.groups()
        if frequency == "daily":
            return date  # Keep as-is
        elif frequency == "monthly":
            return f"{year}-{month}"
        elif frequency == "quarterly":
            quarter = (int(month) - 1) // 3 + 1
            return f"{year}-Q{quarter}"
        elif frequency in ("annual", "biennial", "triennial"):
            return year

    # Already in correct format or unknown - return as-is
    return date


def load_mapping() -> dict:
    """Load the dataset mapping configuration."""
    mapping_path = MAPPINGS_DIR / "datasets.json"
    with open(mapping_path) as f:
        return json.load(f)


def load_raw_series(series_code: str) -> list[dict]:
    """Load raw data for a single series.

    In cloud mode: loads from R2 via load_raw_json
    In local mode: loads from data/raw/series/{code}.json
    """
    if is_cloud_mode():
        try:
            return load_raw_json(f"series/{series_code}")
        except FileNotFoundError:
            return []
    else:
        raw_dir = Path(get_data_dir()) / "raw" / "series"
        series_path = raw_dir / f"{series_code}.json"

        if not series_path.exists():
            return []

        with open(series_path) as f:
            return json.load(f)


def transform_dataset(dataset_id: str, config: dict) -> pa.Table | None:
    """
    Transform a single dataset from skinny to wide format.

    Args:
        dataset_id: The output dataset identifier
        config: Dataset configuration with title, description, frequency, series mapping

    Returns:
        PyArrow table in wide format, or None if no data
    """
    series_mapping = config["series"]

    # Collect all data points indexed by date
    # Structure: {date: {column_name: value, ...}}
    date_rows = defaultdict(dict)

    series_found = 0
    series_missing = []

    for series_code, series_config in series_mapping.items():
        column_name = series_config["column"]
        raw_data = load_raw_series(series_code)

        if not raw_data:
            series_missing.append(series_code)
            continue

        series_found += 1

        for obs in raw_data:
            date = obs.get("date")
            value_str = obs.get("value")

            if not date or value_str is None:
                continue

            # Normalize date format based on frequency
            date = normalize_date(date, config.get("frequency", ""))

            # Try to convert to float, keep as string if not numeric
            try:
                value = float(value_str)
            except (ValueError, TypeError):
                # Some series have string values (like dates), skip these
                continue

            date_rows[date][column_name] = value

    if not date_rows:
        print(f"  {dataset_id}: No data found")
        return None

    if series_missing:
        print(f"  {dataset_id}: Missing {len(series_missing)} series: {series_missing[:5]}{'...' if len(series_missing) > 5 else ''}")

    # Build rows with date + all columns
    all_columns = [series_config["column"] for series_config in series_mapping.values()]
    rows = []

    for date in sorted(date_rows.keys()):
        row = {"date": date}
        for col in all_columns:
            row[col] = date_rows[date].get(col)  # None if missing
        rows.append(row)

    # Build schema: date as string, all other columns as float64 (nullable)
    schema_fields = [pa.field("date", pa.string(), nullable=False)]
    for col in all_columns:
        schema_fields.append(pa.field(col, pa.float64(), nullable=True))

    schema = pa.schema(schema_fields)
    table = pa.Table.from_pylist(rows, schema=schema)

    print(f"  {dataset_id}: {len(table)} rows, {series_found}/{len(series_mapping)} series")

    return table


def make_metadata(dataset_id: str, config: dict) -> dict:
    """Generate metadata for a dataset."""
    column_descriptions = {"date": "Observation date"}

    for series_code, series_config in config["series"].items():
        column_descriptions[series_config["column"]] = series_config["description"]

    return {
        "id": dataset_id,
        "title": config["title"],
        "description": config["description"],
        "column_descriptions": column_descriptions,
    }


def run(dataset_filter: str | None = None):
    """
    Transform all datasets defined in the mapping.

    Args:
        dataset_filter: If provided, only transform datasets matching this prefix
    """
    mapping = load_mapping()
    datasets = mapping["datasets"]

    print(f"Processing {len(datasets)} datasets from mapping...")

    success_count = 0
    skip_count = 0

    for dataset_id, config in datasets.items():
        # Apply filter if provided
        if dataset_filter and not dataset_id.startswith(dataset_filter):
            continue

        table = transform_dataset(dataset_id, config)

        if table is None or len(table) == 0:
            skip_count += 1
            continue

        # Upload and publish (merge by date to handle incremental updates)
        upload_data(table, dataset_id, mode="merge", merge_key="date")
        publish(dataset_id, make_metadata(dataset_id, config))
        success_count += 1

    print(f"\nComplete: {success_count} datasets uploaded, {skip_count} skipped")


if __name__ == "__main__":
    import sys

    # Allow filtering by dataset prefix: python -m transforms.datasets.main bos_
    dataset_filter = sys.argv[1] if len(sys.argv) > 1 else None
    run(dataset_filter)
