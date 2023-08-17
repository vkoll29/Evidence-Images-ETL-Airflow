from datetime import date, datetime, timedelta, timezone
from airflow.decorators import dag, task
from functools import reduce

default_args = {
    'owner': 'vkoll29',
    'retries': 1,
    'retry_delay': timedelta(seconds=45)

}


@dag(
    dag_id='dag_taskflow',
    start_date=datetime(2023,8,14),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=True
)
def try_reduce():

    @task()
    def get_courses_lst():
        return ['DP203', 'AZ900']

    @task(
        multiple_outputs=True
    )
    def get_courses_schedule():
        return{
            'DP': datetime(2023,9,27),
            'AZ': datetime(2023,11,30)
        }

    @task()
    def print_courses_schedule(courses, shed1, shed2):
        stmnt = f"""
            You have the following exams to sit for:
            * EXam 1 will be {courses[0]} which you will sit for on {shed1}
            * The second exam is {courses[1]} to be taken on {shed2}
            --------------------------
        """
        print(stmnt)
        print(reduce(lambda x, y: x+ ' and '+ y, courses))

    courses = get_courses_lst()
    schedule = get_courses_schedule()
    print_courses_schedule(courses, shed1=schedule['DP'], shed2=schedule['AZ'])

tf_dag = try_reduce()
