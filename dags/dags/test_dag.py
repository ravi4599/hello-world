from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 19, 3, 0, 0),  # Corrected the start_date format
    'email': ['ravi.voleti@inspirebrands.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='TestDAG',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    max_active_runs=1
)

def print_message():
    print("Job started")

def print_message1():
    print("Job running")

def print_message2():
    print("Job completed")

Starttask = PythonOperator(
    task_id='Starttask',
    python_callable=print_message,
    dag=dag
)

Intermediatetask = PythonOperator(
    task_id='Intermediatetask',
    python_callable=print_message1,
    dag=dag
)

Finaltask = PythonOperator(
    task_id='Finaltask',
    python_callable=print_message2,
    dag=dag
)

Starttask >> Intermediatetask >> Finaltask
