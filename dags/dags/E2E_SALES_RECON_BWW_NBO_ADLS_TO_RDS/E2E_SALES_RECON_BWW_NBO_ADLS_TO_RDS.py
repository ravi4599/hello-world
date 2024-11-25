import sys, pendulum
import os
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable

# --------------------AF_VARS------------------------------
default_load_variable = {
    "ld_start_dt": "",
    "ld_end_dt": ""
}
af_params = Variable.get('E2E_SALES_RECON_BWW_NBO_ADLS_TO_RDS', default_load_variable, deserialize_json=True)

from_date = af_params['ld_start_dt'] if af_params['ld_start_dt'] != '' else pendulum.now('America/New_York').add(days=1).format('YYYYMMDD')
to_date = af_params['ld_end_dt'] if af_params['ld_end_dt'] != '' else pendulum.now('America/New_York').add(days=1).format('YYYYMMDD')

# --------------------Confs------------------------------

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
pattern = confs.get('pattern')
file_format = confs.get('file_format')

# Setting up variables
airflow_vars = Variable.get(key=confs.get('dag_id'), default_var=dict(), deserialize_json=True)
run_date = pendulum.now("US/Eastern").add(days=0).strftime("%Y%m%d")

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
    tags = ['BWW', 'SALESRECON_BWW', 'ADLS_TO_RDS'],
    template_searchpath = confs.get('sql_path'),
    dagrun_timeout=timedelta(minutes=600),
    catchup = False
) as dag:

 Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 
 End = BashOperator(task_id='End', bash_command='exit 0')
 E2E_SALES_RECON_BWW_NBO_ADLS_TO_RDS = SnowflakeOperator(
    task_id='E2E_SALES_RECON_BWW_NBO_ADLS_TO_RDS',
    snowflake_conn_id=snowflake_conn_id,
    sql = 'rds/bww/ingest-bww-nbo-adls-to-rds.sql',
    params={
             "warehouse": warehouse
            , "schema": schema
            , "env": env
            , "file_format": file_format
            , "pattern": pattern
            , "load_start_dt": from_date
            , "load_end_dt": to_date
            , "run_date": run_date
    }
  )
#Task Run Flow
Start>>E2E_SALES_RECON_BWW_NBO_ADLS_TO_RDS  
E2E_SALES_RECON_BWW_NBO_ADLS_TO_RDS>>End
#Task Run Flow End

if __name__ == "__main__":
    dag.cli()
 