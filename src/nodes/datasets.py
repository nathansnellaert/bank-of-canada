"""Transform Bank of Canada series data into wide-format datasets.

This node:
1. Loads the mapping configuration from mappings/datasets.json
2. For each dataset in the mapping, loads the relevant raw series data
3. Pivots from skinny format to wide format (date as index, series as columns)
4. Uploads each dataset as a separate Delta table

Uses state diff to only process series that have been updated since last transform.
"""
import json
import re
import pyarrow as pa
from pathlib import Path
from collections import defaultdict
from subsets_utils import load_raw_json, load_state, save_state, upload_data, validate

MAPPINGS_DIR = Path(__file__).parent.parent / "mappings"

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
    """Load raw data for a single series."""
    try:
        return load_raw_json(f"series/{series_code}")
    except FileNotFoundError:
        return []

def test_wide_table(table: pa.Table, dataset_id: str, config: dict) -> None:
    """Validate a wide-format dataset."""
    # Build expected columns
    expected_columns = {"date": "string"}
    for series_config in config["series"].values():
        expected_columns[series_config["column"]] = "double"

    # Validate schema and basic constraints
    validate(table, {
        "columns": expected_columns,
        "not_null": ["date"],
        "min_rows": 1,
    })

    # Validate date format based on frequency
    frequency = config.get("frequency", "")
    dates = table.column("date").to_pylist()

    if frequency == "daily":
        # Expect YYYY-MM-DD format
        for d in dates[:10]:  # Sample check
            assert len(d) == 10, f"Daily date should be YYYY-MM-DD: {d}"
            assert d[4] == "-" and d[7] == "-", f"Invalid daily date format: {d}"

    elif frequency == "monthly":
        # Expect YYYY-MM format
        for d in dates[:10]:
            assert len(d) == 7, f"Monthly date should be YYYY-MM: {d}"
            assert d[4] == "-", f"Invalid monthly date format: {d}"

    elif frequency == "quarterly":
        # Expect YYYY-QN format
        for d in dates[:10]:
            assert "Q" in d, f"Quarterly date should contain Q: {d}"

    elif frequency == "annual":
        # Expect YYYY format
        for d in dates[:10]:
            assert len(d) == 4, f"Annual date should be YYYY: {d}"
            assert d.isdigit(), f"Annual date should be numeric: {d}"

    # Check that we have data in at least some columns (not all null)
    non_date_cols = [c for c in table.column_names if c != "date"]
    has_data = False
    for col in non_date_cols:
        non_null_count = len(table) - table.column(col).null_count
        if non_null_count > 0:
            has_data = True
            break

    assert has_data, f"Dataset {dataset_id} has no data in any column"

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
        "title": config["title"],
        "description": config["description"],
        "column_descriptions": column_descriptions,
    }

def run(dataset_filter: str | None = None):
    """
    Transform all datasets defined in the mapping.

    Uses state diff to only process datasets whose underlying series have changed.

    Args:
        dataset_filter: If provided, only transform datasets matching this prefix
    """
    print("Transforming datasets...")

    # Load states
    ingest_state = load_state("series_data")
    transform_state = load_state("datasets")

    fetched_series = set(ingest_state.get("fetched_series", []))
    transformed_series = set(transform_state.get("transformed_series", []))
    new_series = fetched_series - transformed_series

    if not new_series and not dataset_filter:
        # Check if any series states have changed (new data for existing series)
        series_states = ingest_state.get("series_states", {})
        last_transform_states = transform_state.get("last_series_states", {})

        # Find series with updated last_date
        changed_series = set()
        for code, state in series_states.items():
            prev_state = last_transform_states.get(code, {})
            if state.get("last_date") != prev_state.get("last_date"):
                changed_series.add(code)

        if not changed_series:
            print("  No new or updated series to transform")
            return

        print(f"  {len(changed_series)} series have new data")
    else:
        print(f"  {len(new_series)} new series fetched")

    # Load mapping
    mapping = load_mapping()
    datasets = mapping["datasets"]

    print(f"  Processing {len(datasets)} datasets from mapping...")

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

        # Validate before upload
        try:
            test_wide_table(table, dataset_id, config)
        except AssertionError as e:
            print(f"    Validation failed for {dataset_id}: {e}")
            skip_count += 1
            continue

        # Upload (merge by date to handle incremental updates)
        upload_data(table, dataset_id, mode="merge", merge_key="date")
        success_count += 1

    # Update transform state
    save_state("datasets", {
        "transformed_series": sorted(fetched_series),  # All fetched series are now transformed
        "last_series_states": ingest_state.get("series_states", {}),
    })

    print(f"  Complete: {success_count} datasets uploaded, {skip_count} skipped")

from nodes.series_data import run as series_data_run

NODES = {
    run: [series_data_run],
}

if __name__ == "__main__":
    run()
