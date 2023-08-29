import os
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from common_helpers.get_dates import get_dates

def get_blobs_data(container, sas_token, IRType, start, stop=-1):
    accountURI = os.environ.get('URI')
    start_date, end_date = get_dates(start=start, stop=stop)
    path = f'V2/Data/{IRType}'
    print(start_date, end_date)

    blob_service_client = BlobServiceClient(account_url=accountURI, credential=sas_token)
    container_client = blob_service_client.get_container_client(container)
    blobs = []
    for blob in container_client.list_blobs(name_starts_with=path):
        if start_date <= blob.last_modified.date() <= end_date:
            blobs.append(blob.name)

    dfs_list = []
    for blob in blobs:
        blob_client = blob_service_client.get_blob_client(container, blob, snapshot=None)
        data = pa.BufferReader(blob_client.download_blob().readall())
        parquet_table = pq.read_table(data)
        df = parquet_table.to_pandas()
        dfs_list.append(df)

    all_data_df = pd.concat(dfs_list)



    print(len(all_data_df))
    return all_data_df