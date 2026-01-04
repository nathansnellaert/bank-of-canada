"""Transform Bank of Canada series list to dataset."""

import pyarrow as pa
from subsets_utils import load_raw_file, upload_data
from utils.csv_parser import parse_series_csv


def run():
    """Transform raw series list to PyArrow table and upload."""
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
    print(f"  Processed {len(series_data)} series")
    upload_data(table, "series_list")
