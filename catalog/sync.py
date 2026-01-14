#!/usr/bin/env python3
"""Sync Bank of Canada series catalog and detect drift.

Usage:
    python catalog/sync.py           # Report new/removed series (dry run)
    python catalog/sync.py --integrate  # Add new series with status=disabled

Status values:
    active   - Being ingested
    archived - Was active, now disabled
    disabled - Not being ingested
"""
import argparse
import csv
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

CATALOG_URL = "https://www.bankofcanada.ca/valet/lists/series/csv"
CATALOG_DIR = Path(__file__).parent
STATUS_FILE = CATALOG_DIR / "status.json"


def fetch_series_csv() -> str:
    """Fetch series list CSV from Bank of Canada API."""
    sys.path.insert(0, str(CATALOG_DIR.parent / "src"))
    from subsets_utils import get

    print(f"Fetching {CATALOG_URL}...")
    response = get(CATALOG_URL, timeout=30.0)
    response.raise_for_status()
    return response.text


def parse_series_csv(csv_text: str) -> dict:
    """Parse Bank of Canada series CSV format.

    The CSV has a header section followed by 'SERIES' marker and then the actual data.
    Returns dict mapping series name -> {label, description, link}.
    """
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
                "label": row.get("label", "").strip(),
                "description": row.get("description", "").strip(),
                "link": row.get("link", "").strip(),
            }
    return series


def load_status() -> dict:
    """Load status.json."""
    if STATUS_FILE.exists():
        with open(STATUS_FILE) as f:
            return json.load(f)
    return {"_meta": {}, "series": {}}


def save_status(status: dict):
    """Save status.json."""
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)


def sync(integrate: bool = False):
    """Sync catalog and detect drift.

    Args:
        integrate: If True, add new series with status=disabled. If False, just report.
    """
    csv_text = fetch_series_csv()
    catalog_series = parse_series_csv(csv_text)
    print(f"Found {len(catalog_series)} series in catalog")

    status = load_status()
    known_series = set(status.get("series", {}).keys())
    catalog_names = set(catalog_series.keys())

    # Detect changes
    new_series = catalog_names - known_series
    removed_series = known_series - catalog_names

    # Report NEW series
    if new_series:
        print()
        print(f"NEW ({len(new_series)} series):")
        for name in sorted(new_series)[:15]:
            info = catalog_series[name]
            label = info["label"][:60] if info["label"] else name
            print(f"  + {name}: {label}")
        if len(new_series) > 15:
            print(f"  ... and {len(new_series) - 15} more")

    # Report REMOVED series
    if removed_series:
        print()
        print(f"REMOVED ({len(removed_series)} series):")
        for name in sorted(removed_series)[:15]:
            print(f"  - {name}")
        if len(removed_series) > 15:
            print(f"  ... and {len(removed_series) - 15} more")

    # Integrate new series if requested
    if integrate and new_series:
        print()
        print(f"Integrating {len(new_series)} new series with status=disabled...")
        for name in new_series:
            info = catalog_series[name]
            status.setdefault("series", {})[name] = {
                "title": info["label"] or name,
                "metadata": {
                    "label": info["label"],
                    "description": info["description"],
                    "link": info["link"],
                },
                "status": "disabled",
                "reason": "awaiting curation",
                "node": None,
            }

        # Update metadata
        status["_meta"] = {
            "last_synced": datetime.now(timezone.utc).isoformat(),
            "catalog_source": CATALOG_URL,
        }
        save_status(status)
        print("Done. status.json updated.")

    elif not integrate and (new_series or removed_series):
        print()
        print("Run with --integrate to add new series to status.json")

    # Summary
    if not new_series and not removed_series:
        print()
        print("No changes detected.")

    # Count by status
    counts = {"active": 0, "archived": 0, "disabled": 0}
    for info in status.get("series", {}).values():
        s = info.get("status", "disabled")
        counts[s] = counts.get(s, 0) + 1

    print()
    print("=" * 60)
    print(f"Status summary: {counts['active']} active, {counts['archived']} archived, {counts['disabled']} disabled")
    print("=" * 60)

    return 0


def main():
    parser = argparse.ArgumentParser(description="Sync Bank of Canada series catalog")
    parser.add_argument(
        "--integrate",
        action="store_true",
        help="Add new series to status.json with status=disabled",
    )
    args = parser.parse_args()
    return sync(integrate=args.integrate)


if __name__ == "__main__":
    sys.exit(main())
