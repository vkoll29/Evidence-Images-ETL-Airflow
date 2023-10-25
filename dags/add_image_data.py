from datetime import datetime, timedelta
import pyodbc


from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException


# from dotenv import load_dotenv
# import psycopg2
# from psycopg2 import errorcodes

@dag(
    dag_id='2_add_url_to_images_mssql',
    start_date=datetime(2023,10,24),
    schedule_interval='@daily',
    max_active_runs=1,
    default_args={
         'owner': 'vkoll29',
    },
    catchup=False
)
def add_image_data():

    def create_images_view():

        #this view is not used anywhere in this code, it's only here fore reference. I don't want to create a task with the view
        # now that I think about it, I should actually create that task
        url_view = """
            CREATE VIEW image_urls AS
            SELECT 
                    DATE(s.session_start_date) session_date,
                    s.client_code,  
                    s.outlet_code,
                    s.outlet_name,
                    s.country_code,
                    s.user_id,
                    e.sessionuid,
                    e.sceneuid,
                    e.scenetype,
                    e.subscenetype,
                  	formattedevidenceimagename[1],
                    formattedevidenceimageurL[1]
            FROM evidence_images e
            INNER JOIN sessions s 
                ON e.sessionuid = s.sessionuid
            WHERE s.sessionstatus = 'Complete'
        
        """

        get_urls = """
            SELECT 
                evidence_image_name, 
                evidence_image_url,
                outlet_code,  
                subscenetype,
                session_date        
            FROM public.image_urls;

        
        """
        insert_urls_mq = """            
            UPDATE [dbo].[View_ManualQuestions]
            SET 
                [EvidenceImageName] = %s,
                [EvidenceImageURL] = %s
            WHERE ,[OutletCode] = {{}}  AND [SubSceneType] = {{}} AND [Date] = {{}}
        """

        pg_hook = PostgresHook(postgres_conn_id='postgres_dk_lh')
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(get_urls)
        rows = pg_cursor.fetchall()


        ms_hook = MsSqlHook(mssql_conn_id='ired_conn_vm', schema='ired')
        ms_conn = ms_hook.get_conn()
        ms_cursor = ms_conn.cursor()
        ms_cursor.executemany(insert_urls_mq, rows)
        ms_conn.commit()

        ms_cursor.close()
        ms_conn.close()
        pg_cursor.close()
        pg_conn.close()

    #update_image_info = create_images_view()

    @task
    def test_mssql_conn():
        try:
            conn_str = f"Driver=ODBC Driver 17 for SQL Server;Server=168.63.70.212,1599;Database=ired;UID=su;PWD=7&F.n]z2"
            conn = pyodbc.connect(conn_str)
            print("Connection successful!")
            conn.close()
        except Exception as e:
            print(f"Connection failed: {str(e)}")

    test = test_mssql_conn()



image_data = add_image_data()