from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import V1Pod, V1Container

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL.py',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,  # Set to True if you want to backfill
    tags=['example'],
)

# images
image1 = 'acracdsatatest.azurecr.io/dag-etl2:1.2.0'  # Replace with your ACR image details
image2 = 'acracdsatatest.azurecr.io/dag-dbt:1.6.0'  # Replace with your ACR image details

# Define the first task to run "dbt seed"
task1 = KubernetesPodOperator(
    task_id='dbt_seed1',
    name='dbt-seed-pod1',
    namespace='airflow',
    image=image1,
    cmds=['dbt', 'seed'],
    get_logs=True,
    pod_template_file = None,
    dag=dag,
   
)

# Define the second task to run "dbt run"
task2 = KubernetesPodOperator(
    task_id='dbt_run1',
    name='dbt-run-pod1',
    namespace='airflow',
    image=image1,
    cmds=['dbt', 'run'],
    get_logs=True,
    pod_template_file = None,
    dag=dag,
    
)

# Define the first task to run "dbt seed"
task3 = KubernetesPodOperator(
    task_id='dbt_seed2',
    name='dbt-seed-pod2',
    namespace='airflow',
    image=image2,
    cmds=['dbt', 'seed'],
    get_logs=True,
    pod_template_file = None,
    dag=dag,
    
)

# Define the second task to run "dbt run"
task4 = KubernetesPodOperator(
    task_id='dbt_run2',
    name='dbt-run-pod2',
    namespace='airflow',
    image=image2,
    cmds=['dbt', 'run'],
    get_logs=True,
    pod_template_file = None,
    dag=dag,
    
)

# Set task dependencies
task1 >> task2 >> task3 >> task4
