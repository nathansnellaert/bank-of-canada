
import csv
import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
from subsets_utils import get, load_raw_file, load_raw_json, save_raw_json, load_state, save_state
from subsets_utils.environment import get_data_dir
from subsets_utils.r2 import is_cloud_mode
from utils.csv_parser import parse_series_csv


def convert_quarterly_to_iso(date_str: str) -> str:
    if 'Q' in date_str:
        year, quarter = date_str.split('Q')
        quarter_to_month = {'1': '01', '2': '04', '3': '07', '4': '10'}
        month = quarter_to_month.get(quarter, '01')
        return f"{year}-{month}-01"
    return date_str


def fetch_series_observations(series_code: str, start_date: str) -> list | None:
    """Fetch observations for a series. Returns None if the series is inaccessible."""
    url = f"https://www.bankofcanada.ca/valet/observations/{series_code}/csv"
    api_start_date = convert_quarterly_to_iso(start_date)
    params = {"start_date": api_start_date}

    response = get(url, params=params, timeout=30.0)

    # Handle API errors gracefully - some series may be restricted or unavailable
    if response.status_code == 403:
        return None  # Series is forbidden/restricted
    if response.status_code == 404:
        return None  # Series doesn't exist
    response.raise_for_status()

    lines = response.text.split('\n')

    obs_start = -1
    for i, line in enumerate(lines):
        if '"OBSERVATIONS"' in line:
            obs_start = i + 1
            break

    if obs_start == -1:
        return []

    obs_lines = '\n'.join(lines[obs_start:])
    reader = csv.DictReader(io.StringIO(obs_lines))

    observations = []
    for row in reader:
        if 'date' in row and series_code in row:
            observations.append({
                "date": row['date'],
                "series_code": series_code,
                "value": row[series_code]
            })

    return observations


def load_series_data(series_code: str) -> list:
    """Load existing observations for a series.

    In cloud mode: loads from R2 via save_raw_json/load_raw_json
    In local mode: loads from data/raw/series/{code}.json
    """
    if is_cloud_mode():
        try:
            return load_raw_json(f"series/{series_code}")
        except FileNotFoundError:
            return []
    else:
        series_dir = Path(get_data_dir()) / "raw" / "series"
        series_path = series_dir / f"{series_code}.json"
        if series_path.exists():
            with open(series_path) as f:
                return json.load(f)
        return []


def save_series_data(series_code: str, data: list) -> None:
    """Save observations for a series.

    In cloud mode: saves to R2 via save_raw_json
    In local mode: saves to data/raw/series/{code}.json
    """
    if is_cloud_mode():
        save_raw_json(data, f"series/{series_code}")
    else:
        series_dir = Path(get_data_dir()) / "raw" / "series"
        series_dir.mkdir(parents=True, exist_ok=True)
        series_path = series_dir / f"{series_code}.json"
        with open(series_path, 'w') as f:
            json.dump(data, f)


def run():
    csv_text = load_raw_file("series_list", extension="csv")
    series_list = parse_series_csv(csv_text)
    print(f"Loaded {len(series_list)} series from raw cache")

    state = load_state("series_data")

    updated_count = 0
    skipped_count = 0
    inaccessible_count = 0

    for series in tqdm(series_list, desc="Fetching series data"):
        series_code = series['name']

        # Load existing observations (from R2 in cloud mode)
        existing_obs = load_series_data(series_code)

        # Get last_date from state, or derive from existing data
        last_date = state.get(series_code, {}).get("last_date")
        if not last_date and existing_obs:
            # Filter out invalid dates (headers, "date", "REVISIONS", etc.)
            valid_dates = [obs["date"] for obs in existing_obs if obs["date"] and '-' in obs["date"]]
            if valid_dates:
                last_date = max(valid_dates)

        # Fetch from day after last_date to avoid duplicates
        if last_date:
            # Handle quarterly format (2025Q3) vs daily format (2025-01-01)
            if 'Q' in last_date:
                # For quarterly, just use the same date - API handles dedup
                start_date = last_date
            else:
                start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = "1900-01-01"

        new_obs = fetch_series_observations(series_code, start_date)

        if new_obs is None:
            inaccessible_count += 1
            continue

        if not new_obs:
            skipped_count += 1
            continue

        # Add metadata to new observations
        for obs in new_obs:
            obs['series_label'] = series.get('label', '')
            obs['series_description'] = series.get('description', '')

        # Merge new observations, deduplicating by date (only valid dates)
        existing_dates = {obs["date"] for obs in existing_obs if obs["date"] and '-' in obs["date"]}
        new_unique = [obs for obs in new_obs if obs["date"] and '-' in obs["date"] and obs["date"] not in existing_dates]
        all_obs = existing_obs + new_unique

        if not new_unique:
            skipped_count += 1
            continue

        # Save observations (to R2 in cloud mode)
        save_series_data(series_code, all_obs)

        # Update state with new last_date (filter out invalid dates)
        valid_new_dates = [obs["date"] for obs in new_obs if obs["date"] and '-' in obs["date"]]
        if not valid_new_dates:
            continue
        new_last_date = max(valid_new_dates)
        state[series_code] = {"last_date": new_last_date}
        save_state("series_data", state)

        updated_count += 1

    print(f"Updated {updated_count} series, {skipped_count} up to date, {inaccessible_count} inaccessible")
