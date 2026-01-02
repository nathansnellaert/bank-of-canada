
import pyarrow as pa
from datetime import datetime
from subsets_utils import load_raw_json, upload_data, save_state


def run():
    raw_observations = load_raw_json("series_data")
    print(f"Loaded {len(raw_observations)} observations from raw cache")
    
    schema = pa.schema([
        pa.field("date", pa.string(), nullable=False),
        pa.field("series_code", pa.string(), nullable=False),
        pa.field("series_label", pa.string(), nullable=False),
        pa.field("series_description", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("last_updated", pa.string(), nullable=False)
    ])
    
    rows = []
    new_state = {}
    
    for obs in raw_observations:
        date = obs.get("date")
        series_code = obs.get("series_code")
        value_str = obs.get("value")
        
        if not date or not series_code or not value_str:
            continue
        
        try:
            numeric_value = float(value_str)
        except (ValueError, TypeError):
            continue
        
        rows.append({
            "date": date,
            "series_code": series_code,
            "series_label": obs.get("series_label", ""),
            "series_description": obs.get("series_description", ""),
            "value": numeric_value,
            "last_updated": datetime.now().isoformat()
        })
        
        # Track latest date per series for state
        if series_code not in new_state or date > new_state.get(series_code, {}).get("last_date", ""):
            new_state[series_code] = {"last_date": date}
    
    print(f"Processed {len(rows)} valid observations")
    
    if rows:
        table = pa.Table.from_pylist(rows, schema=schema)
        upload_data(table, "series_data")
    else:
        print("No data to upload")
    
    save_state("series_data", new_state)
