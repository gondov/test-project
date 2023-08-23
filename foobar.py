# Define the default arguments for the DAG
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1)
}

# Create the DAG with the specified schedule interval
dag = DAG('dbt_dag', default_args=default_args)

# Define the dbt run command as a BashOperator
task1 = BashOperator(
    task_id='run_dbt_model',
    bash_command='k exec -it -n airflow dag-dbt -- dbt seed',
    dag=dag
)

# Define the dbt run command as a BashOperator
task2 = BashOperator(
    task_id='run_dbt_model',
    bash_command='k exec -it -n airflow dag-dbt -- dbt run',
    dag=dag
)

# Set task dependencies
task1 >> task2
