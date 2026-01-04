"""CSV parsing utilities for Bank of Canada data."""

import csv
import io


def parse_series_csv(csv_text: str) -> list[dict]:
    """Parse Bank of Canada series CSV format.

    The CSV has a header section followed by 'SERIES' marker and then the actual data.
    """
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
