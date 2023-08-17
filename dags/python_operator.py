from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'vkoll29',
    'retries': 0
    # 'retry_delay':timedelta(seconds=30)
}

def get_time(ti):
    ti.xcom_push(key='tm', value=datetime.now().strftime("%H:%M:%S"))
    ti.xcom_push(key='dt', value=date.today())

# ti is the task instance from which an xcom should be derived
# task_ids actually takes the name of the function that's run within the task whose xcoms is needed
def do_sumn(ti):
    now = ti.xcom_pull(task_ids='gett_time', key='tm')
    print(f'You are still up at {now}')

def print_dag_run(ti):
    tm = ti.xcom_pull(task_ids='gett_time', key='tm')
    dt = ti.xcom_pull(task_ids='gett_time', key='dt')
    print(f"This dag was run on {dt} at {tm}")

with DAG(
        dag_id="python_op_dag",
        default_args=default_args,
        description="dag with python operator",
        start_date=datetime(2023, 8, 14, 2),
        schedule_interval='@daily'
) as dag:

    task1 = PythonOperator(
        task_id = "print_dag_time_py",
        python_callable = print_dag_run
    )

    task2 = PythonOperator(
        task_id="give_warning",
        python_callable =do_sumn
    )

    task3 = PythonOperator(
        task_id="gett_time",
        python_callable=get_time
    )

    task3 >> task2 >> task1




