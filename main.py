import os
os.environ['CONNECTOR_NAME'] = 'bank-of-canada'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')
from utils import validate_environment, upload_data
from assets.series_list.series_list import process_series_list
from assets.groups.groups import process_groups
from assets.series_data.series_data import process_series_data

def main():
    validate_environment()
    
    # Process and upload metadata assets first
    series_list_data = process_series_list()
    upload_data(series_list_data, "series_list")
    
    groups_data = process_groups()
    upload_data(groups_data, "groups")
    
    # Process and upload data assets that depend on metadata
    series_data = process_series_data(series_list_data)
    upload_data(series_data, "series_data")

if __name__ == "__main__":
    main()