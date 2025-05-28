import pandas as pd
from sodapy import Socrata
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    r"D:\Data_warehouse_project\diesel-amulet-450313-c6-c5b8bf20618a.json"
)

data_url = "data.cityofnewyork.us"  # The Host Name for the API endpoint (the https:// part will be added automatically)
data_set = "erm2-nwe9"  # The data set at the API endpoint (311 data in this case)
app_token = "*****"  # The app token created in the prior steps
client = Socrata(data_url, app_token)  # Create the client to point to the API endpoint
# Set the timeout to 60 seconds
client.timeout = 5800
# Retrieve the first 2000 results returned as JSON object from the API
# The SoDaPy library converts this JSON object to a Python list of dictionaries


record_count = client.get(data_set, select="COUNT(*)")

start = 0  # Start at 0
chunk_size = 100000  # Fetch 2000 rows at a time
results = []  # Empty out our result list
while True:
    results.extend(client.get(data_set, offset=start, limit=chunk_size))
    # Move up the starting record
    start = start + chunk_size
    # If we have fetched all of the records, bail out
    if start > int(record_count[0]["COUNT"]):
        break

# Convert the list of dictionaries to a Pandas data frame
df = pd.DataFrame.from_records(results)

storage_client = storage.Client()

download_bucket = storage_client.get_bucket("group-project-data")

download_bucket.blob("raw_311_data.csv").upload_from_string(df.to_csv(), "csv")
