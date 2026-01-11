"""Ingest and transform Bank of Canada groups.

This node:
1. Fetches all group names from the API
2. Fetches details for each group (with async parallelism)
3. Transforms to a flat group-series mapping table
4. Uploads to Delta table
"""
import csv
import io
import asyncio
import pyarrow as pa
from subsets_utils import get, save_raw_json, load_raw_json, upload_data, validate


def fetch_all_groups() -> list:
    """Fetch all available groups from Bank of Canada API."""
    response = get("https://www.bankofcanada.ca/valet/lists/groups/csv", timeout=30.0)
    response.raise_for_status()

    lines = response.text.split('\n')

    data_start = -1
    for i, line in enumerate(lines):
        if line.strip() == 'GROUPS':
            data_start = i + 1
            break

    if data_start == -1:
        raise ValueError("Could not find GROUPS section in response")

    csv_content = '\n'.join(lines[data_start:])
    reader = csv.DictReader(io.StringIO(csv_content))

    return list(reader)


def get_group_details_sync(group_name: str) -> dict:
    """Fetch details for a single group."""
    url = f"https://www.bankofcanada.ca/valet/groups/{group_name}/json"
    response = get(url, timeout=30.0)
    response.raise_for_status()
    return response.json()


async def get_group_details_async(group_name: str, semaphore) -> dict:
    """Async wrapper for group details fetch."""
    async with semaphore:
        return await asyncio.to_thread(get_group_details_sync, group_name)


def test(table: pa.Table) -> None:
    """Validate groups output."""
    validate(table, {
        "columns": {
            "group_id": "string",
            "group_label": "string",
            "group_description": "string",
            "series_id": "string",
            "series_label": "string",
            "series_link": "string",
        },
        "not_null": ["group_id", "series_id"],
        "min_rows": 1000,
    })


def run():
    """Fetch, transform, and upload groups data."""
    print("Processing groups...")

    # Ingest - fetch group list
    print("  Fetching group list...")
    groups_list = fetch_all_groups()
    print(f"  Found {len(groups_list)} groups")

    # Ingest - fetch details for each group in parallel
    print("  Fetching group details...")

    async def fetch_all_details():
        semaphore = asyncio.Semaphore(10)
        tasks = [get_group_details_async(g['name'], semaphore) for g in groups_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_data = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"    Error fetching group {groups_list[i]['name']}: {result}")
            else:
                all_data.append(result)
        return all_data

    details = asyncio.run(fetch_all_details())
    print(f"  Fetched details for {len(details)} groups")
    save_raw_json(details, "groups")

    # Transform - flatten to group-series mapping
    print("  Transforming to flat table...")
    raw_data = load_raw_json("groups")

    records = []
    for group_details in raw_data:
        group_info = group_details.get('groupDetails', {})
        group_name = group_info.get('name', '')
        label = group_info.get('label', '')
        description = group_info.get('description', '')

        for series_id, series_info in group_info.get('groupSeries', {}).items():
            records.append({
                "group_id": group_name,
                "group_label": label,
                "group_description": description,
                "series_id": series_id,
                "series_label": series_info.get('label', ''),
                "series_link": series_info.get('link', '')
            })

    schema = pa.schema([
        pa.field("group_id", pa.string()),
        pa.field("group_label", pa.string()),
        pa.field("group_description", pa.string()),
        pa.field("series_id", pa.string()),
        pa.field("series_label", pa.string()),
        pa.field("series_link", pa.string())
    ])

    table = pa.Table.from_pylist(records, schema=schema)
    print(f"  {len(records)} group-series mappings")

    test(table)
    upload_data(table, "groups", mode="overwrite")
    print("  Done!")
