import pyarrow as pa
import csv
import io
from datetime import datetime
from utils.http_client import get
from utils.io import load_state, save_state

def fetch_series_data(series_code: str, start_date: str) -> list:
    """Fetch data for a specific series"""
    url = f"https://www.bankofcanada.ca/valet/observations/{series_code}/csv"
    params = {"start_date": start_date}
    
    response = get(url, params=params, timeout=30.0)
    response.raise_for_status()
    
    # Parse CSV content
    content = response.text
    lines = content.split('\n')
    
    # Find the OBSERVATIONS section
    obs_start = -1
    for i, line in enumerate(lines):
        if '"OBSERVATIONS"' in line:
            obs_start = i + 1
            break
    
    if obs_start == -1:
        return []
    
    # Parse observations
    obs_lines = '\n'.join(lines[obs_start:])
    reader = csv.DictReader(io.StringIO(obs_lines))
    
    observations = []
    for row in reader:
        if 'date' in row and series_code in row:
            observations.append(row)
    
    return observations


def fetch_all_series_data(series_list: pa.Table) -> list:
    """Fetch data for all series in the series list"""
    # Get state
    state = load_state("series_data")
    
    # Get series metadata using source column names
    series_codes = series_list["name"].to_pylist()
    series_labels = series_list["label"].to_pylist()
    series_descriptions = series_list["description"].to_pylist()
    
    results = []
    new_state = state.copy()
    
    # Process series one by one
    for series_code, label, description in zip(series_codes, series_labels, series_descriptions):
        # Determine start date for this series
        last_date = state.get(series_code, {}).get("last_date")
        if last_date:
            # Fetch only new data since last date
            start_date = last_date
        else:
            # Fetch all available historical data
            start_date = "1900-01-01"
        
        # Fetch all data for this series
        observations = fetch_series_data(series_code, start_date)
        
        for obs in observations:
            date = obs.get("date")
            value = obs.get(series_code)
            
            if date and value:
                try:
                    # Try to convert value to float, skip if not numeric
                    numeric_value = float(value)
                    results.append({
                        "date": date,
                        "series_code": series_code,
                        "series_label": label,
                        "series_description": description,
                        "value": numeric_value,
                        "last_updated": datetime.now().isoformat()
                    })
                except (ValueError, TypeError):
                    # Skip non-numeric values
                    continue
                
                # Update state with latest date
                if series_code not in new_state or date > new_state.get(series_code, {}).get("last_date", ""):
                    new_state[series_code] = {"last_date": date}
    
    # Save updated state
    save_state("series_data", new_state)
    
    return results

def process_series_data(series_list_table: pa.Table) -> pa.Table:
    """Process all series data using the series list"""
    # Define schema - keep date as string to handle various formats
    schema = pa.schema([
        pa.field("date", pa.string(), nullable=False),
        pa.field("series_code", pa.string(), nullable=False),
        pa.field("series_label", pa.string(), nullable=False),
        pa.field("series_description", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("last_updated", pa.string(), nullable=False)
    ])
    
    # Fetch data for all series
    data = fetch_all_series_data(series_list_table)
    
    if not data:
        # Return empty table with correct schema
        return pa.Table.from_pylist([], schema=schema)
    
    # Keep dates as raw strings - no parsing needed
    # The date field already contains the raw date string from the API
    
    # Convert to PyArrow table
    table = pa.Table.from_pylist(data, schema=schema)
    
    return table