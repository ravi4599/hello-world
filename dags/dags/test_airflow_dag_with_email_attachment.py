from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Initialize the DAG
dag = DAG(
    'send_email_with_attachment',
    default_args=default_args,
    description='A simple DAG to send email with an attachment',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

name = 'ravi'

send_email_task = EmailOperator(
    task_id='send_email',
    to='ravi.voleti@inspirebrands.com',
    subject='Pipe Delimited File',
    html_content='<p>'+name+'</p>',
    dag=dag,
)

send_email_task