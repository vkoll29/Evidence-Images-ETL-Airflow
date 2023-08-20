from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import  DAG

default_args = {
    'owner':'vkoll29',
    'retries':1,
    'retry_delay': timedelta(seconds=30)
}

with DAG (
    dag_id = 'minio_dag_v3',
    start_date=datetime(2023, 8, 17),
    schedule_interval = '0 0 * * *',
    default_args=default_args
) as dag:
    task1 = S3KeySensor(
        task_id='test_connection',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn'
    )
