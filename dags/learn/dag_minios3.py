from datetime import datetime, timedelta
from airflow.decorators import  dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
from airflow.models.connection import Connection
default_args = {
    'owner':'vkoll29',
    'retries':1,
    'retry_delay': timedelta(seconds=30)
}

@dag(
    dag_id = 'minio_dag_v1',
    start_date=datetime(2023, 8, 17),
    schedule_interval = '0 0 * * *',
    default_args=default_args
)
def s3_check():
    # @task()
    # def test_connection():
    sss = S3KeySensor(
        task_id='s3_sensor',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn',
        timeout=60*60,
        poke_interval=15
    )
    #     print(sss)
    # test_conn = test_connection()

chk = s3_check()
