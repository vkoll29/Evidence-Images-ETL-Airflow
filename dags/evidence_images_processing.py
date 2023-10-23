from datetime import datetime, timedelta
import logging
import os
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.email_operator import EmailOperator
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import psycopg2
from psycopg2 import errorcodes


from common_helpers.concat_dfs import concat_dfs
from common_helpers.blob_ingestion import get_blobs_data
from common_helpers.filter_columns import filter_columns
from common_helpers.column_transformations import transform_column_dtypes, transform_date_columns

load_dotenv()


email_to = 'vkollspam1@gmail.com'
start = 6
stop = -1
@dag(dag_id='1_process_evidence_images',
     start_date=datetime(2023,9,28),
     schedule_interval='@daily',
     max_active_runs=1,
    default_args={
         'owner': 'vkoll29',
         # 'retries': 1,

         # 'retry_delay': timedelta(seconds=30)
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
                   client_code varchar(255),
                   sub_client_code varchar(255),
                   outlet_code varchar(255),
                   outlet_name varchar(255),
                   country_code varchar(10),
                   user_id varchar(255),
                   user_profile varchar(255),
                   sessionstatus varchar(255),
                   latitude double precision,
                   longitude double precision,
                   cancel_call_note varchar(255),
                   cancel_call_reason varchar(255),
                   cancel_evidence_image_url text,
                   cancel_evidence_image_name varchar(255),
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


    def create_task(blob_type, country_code, container, sas_token, folder, start, stop=-1):
        @task(task_id=f"get_{country_code}_{blob_type}")
        def get_blobs_data_task():

            return get_blobs_data(container=container, sas_token=sas_token, IRType=folder, start=start, stop=stop)

        return get_blobs_data_task

    @task
    def combine_dfs_task(task_suffix, dfs_list, **kwargs):
        task_id = f'my_concat_dfs_task_{task_suffix}'
        """
                for key, value in kwargs.items():
                    print(f"The parameter is: {key}")
                    print(f"The length of {key} is {len(key)}")
                """
        return concat_dfs(dfs_list)

    @task
    def filter_columns_task(df, cols):
        return filter_columns(df, cols)

    @task
    def transform_column_dtypes_task(df):
        return transform_column_dtypes(df)


    @task
    def transform_date_columns_task(df):

        return transform_date_columns(df)



    @task
    def filter_rows(df):
        print(df.keys)
        return df[df['EvidenceImageURL'] != '']

    @task
    def load_to_mq_table(df):
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
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(sessionuid, sceneuid)
            DO NOTHING 
            """
        parameters=[tuple(row) for row in df.values]
        hook = PostgresHook(postgres_conn_id='postgres_dk_lh', schema='ired')

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, parameters)
        logging.info("Data inserted successfully!")

    @task
    def load_to_sessions_table(df):
        #TASK: Calculate session_length
        sql = """    
                INSERT INTO sessions(
                   Sessionuid,
                   session_start_date,
                   session_end_date,

                   program_id,
                   program_name,
                   program_item_id,
                   program_item_name,
                   client_code,
                   sub_client_code,
                   outlet_code,
                   outlet_name,
                   country_code,
                   user_id,
                   user_profile,
                   sessionstatus,
                   latitude,
                   longitude,
                   cancel_call_note,
                   cancel_call_reason,
                   cancel_evidence_image_url,
                   cancel_evidence_image_name,
                   session_end_latitude,
                   session_end_longitude
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        parameters = [tuple(row) for row in df.values]
        hook = PostgresHook(postgres_conn_id='postgres_dk_lh', schema='ired')

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.executemany(sql, parameters)
                    logging.info("Data inserted successfully!")

                except psycopg2.Error as e:
                    # TODO: Review this part so you can identify actual column causing error. For now, edit all columns
                    if e.pgcode == errorcodes.STRING_DATA_RIGHT_TRUNCATION:
                        print(f"Error: {e}")
                        print(f"Column causing the error: {e.diag.column_name}")
                        logging.error(f"Error: {e}")
                        logging.error(f"Log  Column causing the error: {e.diag.column_name}")
                        raise AirflowException("DB Error: Could not load data to Sessions table")


    @task
    def separate_image_names():
        select = """
            SELECT sceneuid, evidenceimagename FROM evidence_images 
            --WHERE evidenceimagename  like '%,%' --commented out this part to get all the images
        """
        update = "UPDATE evidence_images SET formattedevidenceimagename =%s WHERE sceneuid =%s"
        #test_acid = "SELECT sceneuid, evidenceimagename, formattedevidenceimagename FROM evidence_images WHERE evidenceimagename  like '%,%'"
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
            updates_params.append((list_of_images, row[0])) # row[0] is the sceneuid - refer to the select statement
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

    # send_email = EmailOperator(
    #     task_id='send_email',
    #     to=email_to,
    #     subject='MQ Data Processing Complete',
    #     html_content='<p><b> The job processing IRMQ images completed!<b><p>'
    # )


    #### TASKS ARE RUN HERE
    mq_tb = create_irmq_tb()
    sessions_tb = create_sessions_tb()

    irmq_dfs = []
    sessions_dfs = []

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

    with TaskGroup('get_irmq_blobs_grp') as irmq_group:
        for params in blob_params_list:
            # in the line below, get the key and not value of  whatever value is currently contained in the container key
            # basically doing a reverse search.
            variable_name = [key for key, value in os.environ.items() if value == params['container']][0]
            country_code = variable_name[:3].lower()

            # this line only generates the tasks. it doesn't run them

            get_irmq_task = create_task(
                blob_type='irmq',
                country_code=country_code,
                container=params['container'],
                sas_token=params['sas_token'],
                folder='IRMQ',
                start=start,
                stop=stop
            )
            # you have to store the output in a variable and call the task to run it
            mq_task = get_irmq_task()
            irmq_dfs.append(mq_task)


    with TaskGroup('get_sessions_blobs_grp') as sessions_group:
        for params in blob_params_list:
            variable_name = [key for key, value in os.environ.items() if value == params['container']][0]
            country_code = variable_name[:3].lower()

            get_sessions_task = create_task(
                blob_type='sessions',
                country_code=country_code,
                container=params['container'],
                sas_token=params['sas_token'],
                folder='IRSession',
                start=start,
                stop=stop
            )
            sessions_task = get_sessions_task()
            sessions_dfs.append(sessions_task)

    mq_tb.set_downstream(mq_task)
    sessions_tb.set_downstream(sessions_task)




    # combine the generated dfs for all countries into one
    combined_irmq_df = combine_dfs_task('irmq', irmq_dfs, rslt = mq_task)
    combined_sessions_df = combine_dfs_task('sessions', sessions_dfs) # find a way to rename this dag_id

    # remove unwanted columns
    # filtered_columns = filter_columns(combined_irmq_df)
    mq_columns_to_keep = [
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
    sessions_columns_to_keep = [
        'Sessionuid',
        'sessionstartdatetime',
        'sessionenddatetime',
        'programid',
        'programname',
        'programitemid',
        'programitemname',
        'clientcode',
        'subclientcode',
        'outletcode',
        'outletname',
        'countrycode',
        'userid',
        'userprofile',
        'sessionstatus',
        'latitude',
        'longitude',
        'cancelcallnote',
        'cancelcallreason',
        'cancelevidenceimageurl',
        'cancelevidenceimagename',
        'sessionendlatitude',
        'sessionendlongitude'
    ]
    mq_filtered_columns = filter_columns_task(combined_irmq_df, mq_columns_to_keep)
    sessions_filtered_columns = filter_columns_task(combined_sessions_df, sessions_columns_to_keep)


    #transform column types
    mq_transformed_column_types = transform_column_dtypes_task(mq_filtered_columns)
    sessions_transformed_column_type = transform_column_dtypes_task(sessions_filtered_columns)

    # transform date columns
    mq_transformed_dates = transform_date_columns_task(mq_transformed_column_types)
    sessions_transformed_dates = transform_date_columns_task(sessions_transformed_column_type)


    # # filter evidence image rows to only keep the rows that have images
    mq_filtered_rows = filter_rows(mq_transformed_dates)


    load_mq =load_to_mq_table(mq_filtered_rows)
    load_sessions = load_to_sessions_table(sessions_transformed_dates)
    #
    separated_images = separate_image_names()
    load_mq.set_downstream(separated_images)
    #
    formated_url = format_image_urls()
    separated_images.set_downstream(formated_url)

images = process_evidence_images()

if __name__ == "__main__":
    # Configure logging settings
    logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")
