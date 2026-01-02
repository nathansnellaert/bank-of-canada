"""
Validation tests for Bank of Canada datasets.
"""

import pyarrow as pa
from subsets_utils import validate


def test_wide_table(table: pa.Table, dataset_id: str, config: dict) -> None:
    """
    Validate a wide-format dataset.

    Args:
        table: The transformed PyArrow table
        dataset_id: Dataset identifier for error messages
        config: Dataset config with series mapping
    """
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
        # Expect YYYYQN format
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

    print(f"    Validation passed: {len(table)} rows, {len(non_date_cols)} data columns")
