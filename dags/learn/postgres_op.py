from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'vkoll29',
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}

with DAG (
    dag_id='pg_op_dag_v2',
    start_date=datetime(2023,8,17),
    schedule_interval='0 0 * * mon-wed',
    default_args=default_args
) as dag:
    task1 = PostgresOperator(
        task_id = 'create_pg_table',
        postgres_conn_id='postgres_dk_lh',
        sql="""
            CREATE TABLE IF NOT EXISTS dag_run(
                dt DATE,
                dag_id character varying,
                primary key (dt, dag_id) 
            
            )
        """
    )

    task2 = PostgresOperator(
        task_id='insert_temp_data',
        postgres_conn_id='postgres_dk_lh',
        sql="""
                
                CREATE TABLE IF NOT EXISTS temp_dag_run(
                    dt DATE, 
                    dag_id character varying
                );
                INSERT INTO temp_dag_run (dt, dag_id)VALUES ('{{ ds }}', '{{ dag.dag_id }}');
            """
    )

    task3 = PostgresOperator(
        task_id = 'save_data',
        postgres_conn_id='postgres_dk_lh',
        sql="""
            MERGE INTO dag_run as target
            USING temp_dag_run as source
                ON target.dt = source.dt AND target.dag_id=source.dag_id
            
            --you do not define the table alias since all actions refer to the target table
            --if you specify table e.g target.dt you'll get an error
            WHEN MATCHED THEN
                UPDATE SET dt = source.dt, dag_id = source.dag_id
            WHEN NOT MATCHED THEN
                INSERT (dt, dag_id) VALUES (source.dt, source.dag_id);
        
        """

    )

    task4 = PostgresOperator(
        task_id = 'clean_up',
        postgres_conn_id='postgres_dk_lh',
        sql="""
            DROP TABLE temp_dag_run
        """
    )

    task1 >> task2 >> task3 >> task4