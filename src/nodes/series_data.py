"""Ingest Bank of Canada series observations.

This node:
1. Reads the series list to know what series to fetch
2. Incrementally fetches observations for each series
3. Saves per-series JSON files to raw/
4. Tracks fetched series in state for the datasets transform to diff against

State tracks:
- series_states: {series_code: {"last_date": "YYYY-MM-DD"}} for incremental updates
- fetched_series: [series_code, ...] list of all series that have been fetched
"""
import csv
import io
import time
from datetime import datetime, timedelta
from tqdm import tqdm
from subsets_utils import get, load_raw_file, load_raw_json, save_raw_json, load_state, save_state


GH_ACTIONS_MAX_RUN_SECONDS = 5.5 * 60 * 60


def parse_series_csv(csv_text: str) -> list[dict]:
    """Parse Bank of Canada series CSV format."""
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


def convert_quarterly_to_iso(date_str: str) -> str:
    """Convert quarterly date format for API call."""
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
    """Load existing observations for a series."""
    try:
        return load_raw_json(f"series/{series_code}")
    except FileNotFoundError:
        return []


def save_series_data(series_code: str, data: list) -> None:
    """Save observations for a series."""
    save_raw_json(data, f"series/{series_code}")


def run() -> bool:
    """Fetch series observations incrementally. Returns True if more work to do."""
    print("Ingesting series data...")
    start_time = time.time()

    # Load series list
    csv_text = load_raw_file("series_list", extension="csv")
    series_list = parse_series_csv(csv_text)
    print(f"  {len(series_list)} series in catalog")

    # Load state
    state = load_state("series_data")
    series_states = state.get("series_states", {})
    fetched_series = set(state.get("fetched_series", []))

    updated_count = 0
    skipped_count = 0
    inaccessible_count = 0

    for series in tqdm(series_list, desc="Fetching series data"):
        # Time budget check before each series
        if time.time() - start_time >= GH_ACTIONS_MAX_RUN_SECONDS:
            print(f"  Time budget exhausted")
            return True

        series_code = series['name']

        # Load existing observations (from R2 in cloud mode)
        existing_obs = load_series_data(series_code)

        # Get last_date from state, or derive from existing data
        last_date = series_states.get(series_code, {}).get("last_date")
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
            # Still mark as fetched even if no new data
            fetched_series.add(series_code)
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
            fetched_series.add(series_code)
            continue

        # Save observations (to R2 in cloud mode)
        save_series_data(series_code, all_obs)

        # Update state with new last_date
        valid_new_dates = [obs["date"] for obs in new_obs if obs["date"] and '-' in obs["date"]]
        if valid_new_dates:
            new_last_date = max(valid_new_dates)
            series_states[series_code] = {"last_date": new_last_date}

        # Track that this series has been fetched
        fetched_series.add(series_code)

        # Save state after each series for resumability
        save_state("series_data", {
            "series_states": series_states,
            "fetched_series": sorted(fetched_series)
        })

        updated_count += 1

    print(f"  Updated {updated_count} series, {skipped_count} up to date, {inaccessible_count} inaccessible")
    return False
