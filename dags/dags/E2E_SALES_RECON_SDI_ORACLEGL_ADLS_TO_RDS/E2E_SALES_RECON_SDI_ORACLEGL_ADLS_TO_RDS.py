import sys, pendulum
import os
from pytz import timezone
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --------------------AF_VARS------------------------------
default_load_variable = {
    "ld_start_dt": "",
    "ld_end_dt": ""
}
af_params = Variable.get('E2E_SALES_RECON_SDI_ORACLEGL_ADLS_TO_RDS', default_load_variable, deserialize_json=True)

from_date = af_params['ld_start_dt'] if af_params['ld_start_dt'] != '' else pendulum.now('America/New_York').add(days=1).format('YYYYMMDD')
to_date = af_params['ld_end_dt'] if af_params['ld_end_dt'] != '' else pendulum.now('America/New_York').add(days=1).format('YYYYMMDD')

current_date = datetime.now(timezone('America/New_York')).strftime('%Y%m%d')
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


# Default arguments and email_on_failure set to false because it's true in master
default_args = {
    'owner': 'SALESRECON_SONIC',
    'email': email,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': pendulum.yesterday().in_timezone('America/New_York'),
    'retries': 0
}

params_sdi_oraclegl_audit = {
    'snowflake_rds_db' : confs.get('rds_env'),
    'dest_schema' : confs.get('schema_id'),
    'snowflake_warehouse_name' : confs.get('warehouse_id'),
    'RDS_TABLE' : 'ORACLE_GL', 
    'SOURCE_SYSTEM_NAME' : 'OracleGL',
    'LOADID_C' : 'LOADID',
    'STAGE_NAME' : confs.get('stage_name'),
    'STAGE_FILE_FORMAT_NAME' : confs.get('file_format'),
    'Loaddate' : current_date,
    'filename_column' : 'FILENAME',
    'ARCHIVE_PATH_EXIST' : None,
    'POD' : 'SALES_RECON'
}

# DAG initialization

with DAG(
    dag_id = confs.get('dag_id'),
    description = confs.get('dag_description'),
    default_args = default_args,
    schedule_interval = None,
    tags = ['SDI', 'SALESRECON_SONIC', 'ADLS_TO_RDS'],
    template_searchpath = confs.get('sql_path'),
    dagrun_timeout=timedelta(minutes=600),
    catchup = False
) as dag:

 Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 
 End = BashOperator(task_id='End', bash_command='exit 0')
 
 E2E_SALES_RECON_SDI_ORACLEGL_ADLS_TO_RDS = SnowflakeOperator(
    task_id='E2E_SALES_RECON_SDI_ORACLEGL_ADLS_TO_RDS',
    snowflake_conn_id=snowflake_conn_id,
    sql = 'rds/sdi/ingest-sdi-oraclegl-adls-to-rds.sql',
    params={
             "warehouse": warehouse
            , "schema": schema
            , "env": env
            , "file_format": file_format
            , "pattern": pattern
            , "load_start_dt": from_date
            , "load_end_dt": to_date
    }
  )

trigger_audit_sdi_oraclegl = TriggerDagRunOperator(
    task_id='trigger_audit_sdi_oraclegl',
    trigger_dag_id='UDP_Audit_Validation',
    conf=params_sdi_oraclegl_audit,
    dag=dag
)


#Task Run Flow
Start>>E2E_SALES_RECON_SDI_ORACLEGL_ADLS_TO_RDS  
E2E_SALES_RECON_SDI_ORACLEGL_ADLS_TO_RDS>>trigger_audit_sdi_oraclegl
trigger_audit_sdi_oraclegl>>End
#Task Run Flow End

if __name__ == "__main__":
    dag.cli()
 