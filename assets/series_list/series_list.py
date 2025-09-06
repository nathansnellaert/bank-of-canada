import pyarrow as pa
import csv
import io
from datetime import datetime
from utils.http_client import get
from utils.io import load_state, save_state

def fetch_all_series() -> list:
    """Fetch all available series from Bank of Canada API"""
    response = get("https://www.bankofcanada.ca/valet/lists/series/csv", timeout=30.0)
    response.raise_for_status()
    
    # Parse CSV content - skip preamble
    lines = response.text.split('\n')
    
    # Find where actual CSV data starts (after SERIES header)
    data_start = -1
    for i, line in enumerate(lines):
        if line.strip() == 'SERIES':
            # The header row is the next line after 'SERIES'
            data_start = i + 1
            break
    
    if data_start == -1:
        raise ValueError("Could not find SERIES section in response")
    
    # Join the relevant lines and parse as CSV
    csv_content = '\n'.join(lines[data_start:])
    reader = csv.DictReader(io.StringIO(csv_content))
    
    return list(reader)


def process_series_list() -> pa.Table:
    """Process series list and return as PyArrow table"""
    # Define schema with source column names
    schema = pa.schema([
        pa.field("name", pa.string()),
        pa.field("label", pa.string()),
        pa.field("description", pa.string()),
        pa.field("link", pa.string())
    ])
    
    series_data = fetch_all_series()
    
    # Convert to PyArrow table with explicit schema
    table = pa.Table.from_pylist(series_data, schema=schema)
    
    # Save state
    save_state("series_list", {"last_fetch": datetime.now().isoformat()})
    return table