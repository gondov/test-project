from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG('my_dag', default_args=default_args, schedule_interval=None)

task = BashOperator(
    task_id='my_bash_task',
    bash_command='echo "Hello, World!"',
    dag=dag,
)
