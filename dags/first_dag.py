from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id = "first_dag",
    default_args = default_args,
    description = "the very first dag",
    start_date = datetime(2023, 8, 14, 2),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = "first_task",
        bash_command = "echo Hello world"
    )

    task2 = BashOperator(
        task_id = "second_task",
        bash_command = "echo This is your second command"
    )

    task3 = BashOperator(
        task_id = "third_task",
        bash_command = "echo Alright captain, time to pack it up!"
    )

    task1 >> [task2, task3]