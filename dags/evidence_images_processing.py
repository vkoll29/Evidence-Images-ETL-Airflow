from datetime import datetime, timedelta
import logging
import os
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv


load_dotenv()


def get_dates(start=0, stop=-1):
    """
    :param start: How far ago do you want the pipeline to run. 0 means only today
    :param stop: when do you want the ETL to stop. Default is -1 meaning stop at the most recent file. I.e, when it's -1, last_modified will be tomorrow so it won't cut off any files
    :return: a date tuple with the start and stop dates
    """

    begin = datetime.today().date() - timedelta(days=start)
    end = datetime.today().date() - timedelta(days=stop)
    return begin, end


@dag(dag_id='1_process_evidence_images',
    start_date=datetime(2023,8,20),
    schedule_interval='@daily',
    default_args={
         'owner': 'vkoll29',
         'retries': 1,
         'retry_delay': timedelta(seconds=30)
    },
    catchup=False)
def process_evidence_images():
    sas = os.environ.get('KENYA_SAS')

    @task
    def create_irmq_tb():
        sql = """
            CREATE TABLE IF NOT EXISTS evidence_images(
                Sessionuid UUID,
                Sceneuid UUID,
                SceneType varchar(50),
                SubSceneType varchar(50),
                EvidenceImageURL text,
                EvidenceImageName varchar(255),
                FormattedEvidenceImageURL text[],
                FormattedEvidenceImageName varchar(255)[],
                CreatedOnTime timestamp,
                ReExportStatus Int,
                ReExportTime timestamp,
                ReProcessedStatus Int,
                ReProcessedTime	timestamp
                )                
        """

        hook = PostgresHook(postgres_conn_id='postgres_dk_lh', schema='ired')
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
            logging.info("Table Created")
        except Exception as e:
            logging.error("An exception occurred: %s", str(e), exc_info=True)
        cursor.close()
        conn.close()


    @task
    def get_blobs_data(container: str, sas_token: str, accountURI: str=os.environ.get('URI'), IRType: str='IRMQ'):
        """
        This needs to be improved so that the previous run and next dag run(no_dash) are used to generate start and end dates
          :param container: the azure storage container to connect to
          :param sas_token:  the container's SAS  token
          :param accountURI: fully qualified account URI
          :param IRType: the IR file to download
          :return: dataframe created from the blobs
          """

        start_date, end_date = get_dates(start=1)
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
        print(type(all_data_df))
        return all_data_df




    @task
    def filter_columns(df):

        """
           this function takes a dataframe and a list of columns to keep and returns a df only with those specified columns
           :param df: larger dataframe that includes unneeded columns
           :return: minimized df with only the specified columns
           """

        columns_to_keep = [
                'SessionUID',
                'SceneUID',
                'SceneType',
                'SubSceneType',
                'EvidenceImageURL',
                'EvidenceImageName',
                'CreatedOnTime',
                'ReExportStatus',
                'ReExportTime',
                'ReProcessedStatus',
                'ReProcessedTime'
        ]
        print(type(df))
        for col in df.columns:
            if col.lower() not in [col_keep.lower() for col_keep in columns_to_keep]:
                del df[col]
        # print(list(df.columns))
        print(type(df))
        return df

    @task
    def transform_column_dtypes(df):

        """
        convert a boolean-like columns to bit as in its corresponding column.
        Will transform the values in the provided column to 0 or 1 to match the column setup in db
        :param df: dataframe
        :return: dataframe with the columns transformed
        """

        for col in list(df.columns):
            df[col].replace({'True': 1, 'False': 0}, inplace=True)

        """
        Changes the columns whose data types  are loaded as object in the dataframe to string.
        This should be extended to accept a dictionary (or something like that) whose key is the current dtype and value is the desired dtype
        """
        for col in df.columns:
            if df.dtypes[col] == 'object':
                df[col] = df[col].astype("string")
        print(df.dtypes)
        return df


    @task
    def transform_date_columns(df):
        """
        Undefined dates are represented as NaT in pandas. change these to Null
        :param df:
        :return:
        """
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df = df.applymap(lambda x: None if pd.isna(x) else x)

        return df



    @task
    def filter_rows(df):
        return df[df['EvidenceImageURL'] != '']

    @task
    def load_to_table(df):
        sql = """
            INSERT INTO evidence_images(
                Sessionuid,
                Sceneuid,
                SceneType,
                SubSceneType,
                EvidenceImageURL,
                EvidenceImageName,
                CreatedOnTime,
                ReExportStatus,
                ReExportTime,
                ReProcessedStatus,
                ReProcessedTime
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        parameters=[tuple(row) for row in df.values]
        hook = PostgresHook(postgres_conn_id='postgres_dk_lh', schema='ired')

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, parameters)
        logging.info("Data inserted successfully!")


    @task
    def separate_image_names():
        select = """
            SELECT sceneuid, evidenceimagename FROM evidence_images 
            --WHERE evidenceimagename  like '%,%' --commented out this part to get all the images
        """
        update = "UPDATE evidence_images SET formattedevidenceimagename =%s WHERE sceneuid =%s"
        test_acid = "SELECT sceneuid, evidenceimagename, formattedevidenceimagename FROM evidence_images WHERE evidenceimagename  like '%,%'"
        hook = PostgresHook(postgres_conn_id='postgres_dk_lh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()
        updates_params = []

        for row in rows:

            # after selecting everything - both single and multiple images, split them in the following line
            # if there is a comma in the imagename, then split it (return list by default) else convert the single image to list
            list_of_images = row[1].split(',') if ',' in row[1] else [row[1]]
            updates_params.append((list_of_images, row[0]))
            # cursor.execute(update, (list_of_images, row[0]))

        cursor.executemany(update, updates_params)
        # you have to commit the changes for them to reflect in the DB - ACID, otherwise the results of the commented query will show that the formmated names have been edited but a different session will not read this
        # cursor.execute(test_acid)
        # new = cursor.fetchall()
        # print(new[0])

        conn.commit()
        cursor.close()
        conn.close()


    @task
    def format_image_urls():
        """
        Loop over each image in formattedevidence image and concat it withevidenceimageurl to create fully qualified urls for each image.
        Add these to formattedevidenceimageurl
        :return:
        """
        select = "SELECT sceneuid, evidenceimageurl, formattedevidenceimagename FROM evidence_images"
        update = "UPDATE evidence_images SET formattedevidenceimageurl=%s WHERE sceneuid=%s"
        hook = PostgresHook(postgres_conn_id='postgres_dk_lh')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()

        updates_params = []
        for i, row in enumerate(rows):
            urls = []
            for image in row[2]:
                url = row[1]+image
                urls.append(url)

            updates_params.append((urls, row[0]))

        cursor.executemany(update, updates_params)
        conn.commit()
        cursor.close()
        conn.close()












    tb = create_irmq_tb()
    df = get_blobs_data(
        container='ccba-kenya',
        sas_token=sas,
        accountURI='https://irclientdataexport.blob.core.windows.net',
        IRType='IRMQ'
    )
    filtered_columns = filter_columns(df)
    tb.set_downstream(filtered_columns)
    transformed_column_types = transform_column_dtypes(filtered_columns)
    filtered_rows = filter_rows(transformed_column_types)
    transformed_dates = transform_date_columns(filtered_rows)
    rslt =load_to_table(transformed_dates)

    separated_images = separate_image_names()
    rslt.set_downstream(separated_images)

    formated_url = format_image_urls()
    separated_images.set_downstream(formated_url)

images = process_evidence_images()

if __name__ == "__main__":
    # Configure logging settings
    logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")
