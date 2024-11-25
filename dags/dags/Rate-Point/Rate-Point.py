from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from azure.storage.blob import BlobServiceClient
from airflow.operators.bash_operator import BashOperator
import requests
import xml.etree.ElementTree as ET
from datetime import timedelta

# Define constants for your API and ADLS
API_URL = 'https://ratepoint.sd-apps.com/ratepoint/api/getAllLocations?request=<request><username>INSPUDP</username><password>DeL$int387</password><parentCompanyName>Inspire Brands</parentCompanyName><subCompanyName>Dunkin</subCompanyName></request>'
ADLS_CONTAINER_NAME = 'azure://ibue2dev01udpadls2.blob.core.windows.net'
ADLS_FILE_PATH = 'arbys/outbound/sear'
ADLS_CONNECTION_STRING = 'ibue2dev01udp-airflow2.vault.azure.net/secrets/airflow-connection-adls-storage-options'

def fetch_data_from_api():
    # Fetch XML data from API
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.content  # Return raw XML content

def upload_to_adls(xml_data):
    # Upload XML data to ADLS
    blob_service_client = BlobServiceClient.from_connection_string(ADLS_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=ADLS_CONTAINER_NAME, blob=ADLS_FILE_PATH)
    blob_client.upload_blob(xml_data, overwrite=True)

def run_etl():
    # Fetch and upload XML data
    xml_data = fetch_data_from_api()
    upload_to_adls(xml_data)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_to_adls',
    default_args=default_args,
    description='Fetch XML data from API and upload it to ADLS',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

start_task = BashOperator(task_id='Start', dag=dag, bash_command='exit 0')         
end_task = BashOperator(task_id='End', dag=dag, bash_command='exit 0')

task = PythonOperator(
    task_id='api_to_adls_task',
    python_callable=run_etl,
    dag=dag,
)
start_task >> task >> end_task