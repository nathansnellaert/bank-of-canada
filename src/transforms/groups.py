
"""Transform Bank of Canada groups to dataset."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data


def run():
    """Transform raw groups data to PyArrow table and upload."""
    print("  Transforming groups...")

    raw_data = load_raw_json("groups")

    records = []
    for details in raw_data:
        group_info = details.get('groupDetails', {})
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
    print(f"  Processed {len(records)} group-series mappings")
    upload_data(table, "groups")
