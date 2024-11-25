import sys, pendulum
import os
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from data_inbound_outbound_framework.custom.adf_custom.operators.adf_operators import RunADFPipelineoperator

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
adf_pipeline_file_size = adf_config.get('pipeline_file_size')
adf_SourceFolderName = adf_config.get('sourceFolderName')

# --------------------AF_VARS------------------------------
default_load_variable = {
    "ld_start_dt": "",
    "ld_end_dt": ""
}
af_params = Variable.get('E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_SQL_TO_RDS', default_load_variable, deserialize_json=True)
ld_file_date = pendulum.now('America/New_York').add(days=-1).format('YYYYMMDD')

file_from_date = af_params['ld_start_dt'] if af_params['ld_start_dt'] != '' \
                                          else pendulum.now('America/New_York').add(days=-30).format('YYYYMMDD')
file_to_date = af_params['ld_end_dt'] if af_params['ld_end_dt'] != '' \
                                      else pendulum.now('America/New_York').add(days=-1).format('YYYYMMDD')

ld_start_dt = datetime.strptime(file_from_date, "%Y%m%d")
ld_end_dt = datetime.strptime(file_to_date, "%Y%m%d")

ld_start_date = datetime.strftime(ld_start_dt, "%Y-%m-%d")
ld_end_date = datetime.strftime(ld_end_dt, "%Y-%m-%d")

#Checking the given date is sunday or not
if ld_start_dt.weekday() == 6:
    print(ld_start_dt,":The date fall on the  sunday.")
    sunday=pendulum.now('America/New_York').add(days=-90)
    ld_start_date=sunday.strftime("%Y-%m-%d")
      
else:
    print(" other day.")



# Get configs and load necessary path
confs = load_confs()
email = confs.get('email')
sys.path.append(confs.get('utils_path'))
sys.path.append(confs.get('airflow_path'))

# Environment and Connection setup
snowflake_conn_id = confs.get('snowflake_conn_id')
env = confs.get('env')
warehouse = confs.get('warehouse_id')
schema = confs.get('schema_id')
file_format = confs.get('file_format')
pattern = confs.get('pattern')
schedule_interval = confs.get('schedule_interval')
if not schedule_interval:
    schedule_interval = None

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
    dag_id=confs.get('dag_id'),
    description=confs.get('dag_description'),
    default_args=default_args,
    schedule_interval=schedule_interval,
    template_searchpath=confs.get('sql_path'),
    tags=['BWW', 'SALESRECON_BWW', 'SQL_TO_RDS'],
    dagrun_timeout=timedelta(minutes=600),
    catchup=False
) as dag:
    Start = BashOperator(task_id='Start', bash_command='exit 0')
    End = BashOperator(task_id='End', bash_command='exit 0')

    E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_SQL_TO_ADLS = RunADFPipelineoperator(
       task_id='E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_SQL_TO_ADLS',
       adf_pipeline_name=adf_pipeline_name,
       adf_connection_id=adf_connection_id,
       adf_factory_name=adf_factory_name,
       adf_resource_group=adf_resource_group,
       parameters={
           "Start_date": ld_start_date,
           "End_date": ld_end_date,

       }
    )

    E2E_SR_GNDTENDER_CHECKFILE_SIZE = RunADFPipelineoperator(
       task_id='E2E_SR_GNDTENDER_CHECKFILE_SIZE',
       adf_pipeline_name=adf_pipeline_file_size,
       adf_connection_id=adf_connection_id,
       adf_factory_name=adf_factory_name,
       adf_resource_group=adf_resource_group,
       parameters={
            "run_date": ld_file_date,
            "folder_path": adf_SourceFolderName
       }
    )

    E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_ADLS_TO_RDS = SnowflakeOperator(
       task_id='E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_ADLS_TO_RDS',
       snowflake_conn_id=snowflake_conn_id,
       sql='rds/bww/ingest-bww-dpvhstgndtender-adls-to-rds.sql',
       params={
                "warehouse": warehouse,
                "schema": schema,
                "env": env,
                "file_format": file_format,
                "pattern": pattern,
                "fromdate": ld_start_date,
                "todate": ld_end_date,
                "run_date": ld_file_date
       }
     )

# Task Run Flow
Start >> E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_SQL_TO_ADLS
E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_SQL_TO_ADLS >> E2E_SR_GNDTENDER_CHECKFILE_SIZE
E2E_SR_GNDTENDER_CHECKFILE_SIZE >> E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_ADLS_TO_RDS
E2E_SALES_RECON_BWW_DPVHST_GNDTENDER_ADLS_TO_RDS >> End
# Task Run Flow End

if __name__ == "__main__":
    dag.cli()
