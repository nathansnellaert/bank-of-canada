"""Sync Bank of Canada catalog metadata to status.json."""
import csv
import io
from pathlib import Path

from subsets_utils import get
from subsets_utils.catalog import sync_catalog

CATALOG_URL = "https://www.bankofcanada.ca/valet/lists/series/csv"
STATUS_FILE = Path(__file__).parent / "status.json"


def fetch_catalog() -> dict:
    response = get(CATALOG_URL, timeout=30.0)
    response.raise_for_status()
    csv_text = response.text

    lines = csv_text.split("\n")
    data_start = -1
    for i, line in enumerate(lines):
        if line.strip() == "SERIES":
            data_start = i + 1
            break

    if data_start == -1:
        raise ValueError("Could not find SERIES section in response")

    csv_content = "\n".join(lines[data_start:])
    reader = csv.DictReader(io.StringIO(csv_content))

    series = {}
    for row in reader:
        name = row.get("name", "").strip()
        if name:
            series[name] = {
                "title": row.get("label", "").strip() or name,
                "metadata": {
                    "label": row.get("label", "").strip(),
                    "description": row.get("description", "").strip(),
                    "link": row.get("link", "").strip(),
                },
            }
    return series


def sync():
    sync_catalog(fetch_catalog(), CATALOG_URL, STATUS_FILE)


if __name__ == "__main__":
    sync()
