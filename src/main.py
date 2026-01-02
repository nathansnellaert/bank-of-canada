import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import series_list as ingest_series_list
from ingest import groups as ingest_groups
from ingest import series_data as ingest_series_data
from transforms.datasets import main as transform_datasets


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    parser.add_argument("--dataset", type=str, help="Only transform datasets matching this prefix")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("\n--- Ingesting series list ---")
        ingest_series_list.run()
        print("\n--- Ingesting groups ---")
        ingest_groups.run()
        print("\n--- Ingesting series data ---")
        ingest_series_data.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("\n--- Transforming datasets ---")
        transform_datasets.run(dataset_filter=args.dataset)


if __name__ == "__main__":
    main()
