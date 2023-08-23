from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_dag',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,  # Set to True if you want to backfill
    tags=['example'],
)

image = 'acracdsatatest.azurecr.io/dag-dbt:1.4.0'  # Replace with your ACR image details

# Define the first task to run "dbt seed"
task_seed = KubernetesPodOperator(
    task_id='dbt_seed',
    name='dbt-seed-pod',
    namespace='airflow',
    image=image,
    cmds=['dbt', 'seed'],
    get_logs=True,
    dag=dag,
)

# Define the second task to run "dbt run"
task_run = KubernetesPodOperator(
    task_id='dbt_run',
    name='dbt-run-pod',
    namespace='airflow',
    image=image,
    cmds=['dbt', 'run'],
    get_logs=True,
    dag=dag,
)

# Set task dependencies
task_seed >> task_run
