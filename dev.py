import os

# Set all required environment variables
os.environ['CONNECTOR_NAME'] = 'bank-of-canada'
os.environ['RUN_ID'] = 'local-test'
os.environ['STORAGE_BACKEND'] = 'local'
os.environ['DATA_DIR'] = 'data'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CACHE_REQUESTS'] = 'false'
os.environ['WRITE_SNAPSHOT'] = 'false'

# Run the main connector
from main import main

try:
    main()
    print("\n✓ Bank of Canada connector ran successfully!")
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()