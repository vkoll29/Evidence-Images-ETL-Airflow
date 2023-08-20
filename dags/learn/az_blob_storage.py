from datetime import datetime, timedelta
from airflow.decorators import  dag, task
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from airflow.providers.microsoft.azure.triggers.wasb import WasbBlobSensorTrigger

default_args = {
    'owner':'vkoll29',
    'retries':1,
    'retry_delay': timedelta(seconds=30)
}

@dag(
    dag_id = 'ext_img_az',
    start_date=datetime(2023, 8, 17),
    schedule_interval = '0 0 * * thu',
    default_args=default_args
)

def connect_to_az():
    containerName = "ccba-kenya"
    accountUri = "https://irclientdataexport.blob.core.windows.net"
    sasToken = "sv=2020-08-04&si=AP_IRClient&sr=c&sig=NMOcjr6mq1u7aS%2FOEOwphqIjS3Wavu1YdVONBztLI5Q%3D"

    @task()
    def create_connection():
        try:
            blob_service_client = BlobServiceClient(account_url=accountUri, credential=sasToken)
            container_client = blob_service_client.get_container_client(containerName)
            path = 'V2/Data/IRMQ/2023/08/16'
            blobs = container_client.list_blobs(name_starts_with=path)
            print("Here are the blobs:")
            for blob in blobs:

                print("\t" + blob.name)
        except Exception as ex:
            print(f'Exception occured: {ex}')

    chk_blob = WasbBlobSensorTrigger(
        poke_interval=60,
        wasb_conn_id='ccba_kenya',
        blob_name='IR_MQ_2023-08-16-00-00-00.parquet',
        container_name=containerName,
    )

    create_connection()
    """
    @task()
    def download_blob():
        try:
            blob_service_client = BlobServiceClient(account_url=accountUri, credential=sasToken)
            container_client = blob_service_client.get_container_client(containerName)
            path = 'V2/Data/IRMQ/2023/08/16'
            # using list_blobs even though date is specified which means only one blob will be there
            blobs = container_client.list_blobs(name_starts_with=path)
            # for blob in blobs:
            #     blob_client = blob_service_client.get_blob_client(containerName, blob=blob)

                # here you get the file name without the directories
                # formatted_blob_name = blob_client.blob_name.split('/')[-1].replace(':', '')
                # blob_path = os.path.join(local_folder, formatted_blob_name)
        except Exception as ex:
            print(f'Exception occured: {ex}')
    """
az_dag = connect_to_az()