import sys, pendulum
import os
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from data_inbound_outbound_framework.custom.adf_custom.operators.adf_operators import RunADFPipelineoperator
from airflow.models import Variable

# Get configs and load necessary path
confs = load_confs()
email = confs.get('email')
sys.path.append(confs.get('utils_path'))
sys.path.append(confs.get('airflow_path'))
adf_config = confs.get('azure').get('adf')
adf_connection_id = adf_config.get('connection_id')
adf_pipeline_name = adf_config.get('pipeline_name')
adf_job_name = adf_config.get('job_name')
adf_factory_name = adf_config.get('factory_name')
adf_resource_group = adf_config.get('resource_group')

# Environment and Connection setup
env = confs.get('env')

# Azure KeyVault Credentials
ls_username = confs.get('LSUserName')
ls_secretname = confs.get('LSSecretName')

# Setting up variables
airflow_vars = Variable.get(key=confs.get('dag_id'), default_var=dict(), deserialize_json=True)


# Default arguments and email_on_failure set to false because it's true in master
default_args = {
    'owner': 'SALESRECON',
    'email': email,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': pendulum.yesterday().in_timezone('America/New_York'),
    'retries': 0
}
# DAG initialization


with DAG(
    dag_id = confs.get('dag_id'),
    description = confs.get('dag_description'),
    default_args = default_args,
    schedule_interval = None,
    tags = ['SDI', 'SALESRECON_SONIC', 'FTP_TO_ADLS'],
    dagrun_timeout=timedelta(minutes=600),
    catchup = False
) as dag:

 Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 
 End = BashOperator(task_id='End', bash_command='exit 0')
 E2E_SALES_RECON_SDI_ORACLEGL_FTP_TO_ADLS = RunADFPipelineoperator(
    task_id='E2E_SALES_RECON_SDI_ORACLEGL_FTP_TO_ADLS',
    adf_pipeline_name=adf_pipeline_name,
    adf_connection_id=adf_connection_id,
    adf_factory_name=adf_factory_name,
    adf_resource_group=adf_resource_group,
    parameters={
        "JobName": adf_job_name
    }
)
 
#Task Run Flow
Start>>E2E_SALES_RECON_SDI_ORACLEGL_FTP_TO_ADLS  
E2E_SALES_RECON_SDI_ORACLEGL_FTP_TO_ADLS>>End
#Task Run Flow End

if __name__ == "__main__":
    dag.cli()
 