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

from common_helpers.get_dates import get_dates
from common_helpers.concat_dfs import concat_dfs

load_dotenv()



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
        cursor.execute(sql)
        conn.commit()
        logging.info("Table Created")
        cursor.close()
        conn.close()

    @task
    def create_sessions_tb():
        sql = """
               CREATE TABLE IF NOT EXISTS sessions(
                   Sessionuid UUID,
                   session_start_date timestamp,
                   session_end_date timestamp,
                   session_length interval,
                   program_id int,
                   program_name varchar(255),
                   program_item_id int,
                   program_item_name varchar(255),
                   client_code varchar(50),
                   sub_client_code varchar(50),
                   outlet_code varchar(100),
                   outlet_name varchar(255),
                   country_code varchar(10),
                   user_id varchar(50),
                   user_profile varchar(100),
                   sessionstatus varchar(50),
                   latitude double precision,
                   longitude double precision,
                   cancel_call_note varchar(50),
                   cancel_call_reason varchar(255),
                   cancel_evidence_image_url text,
                   cancel_evidence_image_name varchar(100),
                   session_end_latitude double precision,
                   session_end_longitude double precision
                   )                
           """
        hook = PostgresHook(postgres_conn_id='postgres_dk_lh', schema='ired')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        logging.info("Table Created")
        cursor.close()
        conn.close()


    blob_params_list = [
        {'container': os.environ.get('KEN_CONTAINER'), 'sas_token': os.environ.get('KEN_SAS')},
        {'container': os.environ.get('BWA_CONTAINER'), 'sas_token': os.environ.get('BWA_SAS')},
        {'container': os.environ.get('ETH_CONTAINER'), 'sas_token': os.environ.get('ETH_SAS')},
        {'container': os.environ.get('TZA_CONTAINER'), 'sas_token': os.environ.get('TZA_SAS')},
        {'container': os.environ.get('MOZ_CONTAINER'), 'sas_token': os.environ.get('MOZ_SAS')},
        {'container': os.environ.get('UGA_CONTAINER'), 'sas_token': os.environ.get('UGA_SAS')},
        {'container': os.environ.get('ZAM_CONTAINER'), 'sas_token': os.environ.get('ZAM_SAS')},
        {'container': os.environ.get('NAM_CONTAINER'), 'sas_token': os.environ.get('NAM_SAS')},
        {'container': os.environ.get('GHA_CONTAINER'), 'sas_token': os.environ.get('GHA_SAS')},
        {'container': os.environ.get('CBL_CONTAINER'), 'sas_token': os.environ.get('CBL_SAS')}
    ]

    def create_task(blob_type, country_code, container, sas_token):
        @task(task_id=f"get_{country_code}_{blob_type}")
        def get_blobs_data(accountURI=os.environ.get('URI'), IRType='IRMQ'):
            print(country_code)
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
            print(len(all_data_df))
            return all_data_df

        return get_blobs_data

    @task
    def combine_dfs_task(task_suffix, dfs_list):
        task_id = f'my_concat_dfs_task_{task_suffix}'
        return concat_dfs(dfs_list)

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




    #### TASKS ARE RUN HERE
    tb = create_irmq_tb()
    sessions_tb = create_sessions_tb()

    irmq_dfs = []
    for params in blob_params_list:
        # in the line below, get the key and not value of  whatever value is currently contained in the container key
        # basically doing a reverse search.
        variable_name = [key for key, value in os.environ.items() if value == params['container']][0]
        country_code = variable_name[:3].lower()

        # this line only generates the tasks. it doesn't run them
        get_irmq_task = create_task(blob_type='irmq', country_code=country_code, container=params['container'], sas_token=params['sas_token'])
        # you have to store the output in a variable and call the task to run it
        df_task = get_irmq_task()
        tb.set_downstream(df_task)
        irmq_dfs.append(df_task)

    sessions_dfs = []
    for params in blob_params_list:
        variable_name = [key for key, value in os.environ.items() if value == params['container']][0]
        country_code = variable_name[:3].lower()
        get_sessions_task = create_task(blob_type='sessions', country_code=country_code, container=params['container'], sas_token=params['sas_token'])
        sessions_task = get_sessions_task()
        sessions_tb.set_downstream(sessions_task)
        sessions_dfs.append(sessions_task)


    combined_irmq_df = combine_dfs_task('irmq', irmq_dfs)
    # combined_irmq_df = concat_dfs(irmq_dfs)
    combined_sessions_df = combine_dfs_task('sessions', sessions_dfs) # find a way to rename this dag_id

    filtered_columns = filter_columns(combined_irmq_df)
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
