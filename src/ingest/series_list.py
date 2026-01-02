"""Ingest Bank of Canada series list."""

from subsets_utils import get, save_raw_file


def run():
    """Fetch series list and save raw CSV."""
    print("  Fetching series list...")
    response = get("https://www.bankofcanada.ca/valet/lists/series/csv", timeout=30.0)
    response.raise_for_status()
    save_raw_file(response.text, "series_list", extension="csv")
    print("  Saved raw series list")
