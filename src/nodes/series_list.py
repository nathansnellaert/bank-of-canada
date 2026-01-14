"""Ingest and transform Bank of Canada series list.

This node:
1. Fetches the series list CSV from Bank of Canada API
2. Parses and transforms to PyArrow table
3. Uploads to Delta table
"""
import csv
import io
import pyarrow as pa
from subsets_utils import get, save_raw_file, load_raw_file, upload_data, validate


def parse_series_csv(csv_text: str) -> list[dict]:
    """Parse Bank of Canada series CSV format.

    The CSV has a header section followed by 'SERIES' marker and then the actual data.
    """
    lines = csv_text.split('\n')
    data_start = -1
    for i, line in enumerate(lines):
        if line.strip() == 'SERIES':
            data_start = i + 1
            break

    if data_start == -1:
        raise ValueError("Could not find SERIES section in response")

    csv_content = '\n'.join(lines[data_start:])
    reader = csv.DictReader(io.StringIO(csv_content))
    return list(reader)


def test(table: pa.Table) -> None:
    """Validate series list output."""
    validate(table, {
        "columns": {
            "name": "string",
            "label": "string",
            "description": "string",
            "link": "string",
        },
        "not_null": ["name"],
        "min_rows": 1000,  # Bank of Canada has thousands of series
    })


def run():
    """Fetch, transform, and upload series list."""
    print("Processing series list...")

    # Ingest
    print("  Fetching series list from API...")
    response = get("https://www.bankofcanada.ca/valet/lists/series/csv", timeout=30.0)
    response.raise_for_status()
    save_raw_file(response.text, "series_list", extension="csv")

    # Transform
    print("  Transforming series list...")
    csv_text = load_raw_file("series_list", extension="csv")
    series_data = parse_series_csv(csv_text)

    schema = pa.schema([
        pa.field("name", pa.string()),
        pa.field("label", pa.string()),
        pa.field("description", pa.string()),
        pa.field("link", pa.string())
    ])

    table = pa.Table.from_pylist(series_data, schema=schema)
    print(f"  {len(series_data)} series")

    test(table)
    upload_data(table, "series_list", mode="overwrite")
    print("  Done!")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
