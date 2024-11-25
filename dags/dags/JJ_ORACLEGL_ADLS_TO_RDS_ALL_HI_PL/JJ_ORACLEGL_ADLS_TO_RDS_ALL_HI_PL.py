"""
This DAG loads JJ'S ORACLEGL data from Azure Data Lake Storage Gen2 to RDS in snowflake.
"""

import sys, pendulum
import os
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pytz import timezone
import data_inbound_outbound_framework.custom.default_variables as dv

# --------------------AF_VARS------------------------------
default_load_variable = {
    "ld_start_dt": "",
    "ld_end_dt": ""
}
af_params = Variable.get('JJ_ORACLEGL_ADLS_TO_RDS_ALL_HI_PL', default_load_variable, deserialize_json=True)

from_date = af_params['ld_start_dt'] if af_params['ld_start_dt'] != '' \
    else pendulum.now('US/Eastern').add(days=-1).format('YYYYMMDD')
to_date = af_params['ld_end_dt'] if af_params['ld_end_dt'] != '' \
    else pendulum.now('US/Eastern').add(days=1).format('YYYYMMDD')

current_date = datetime.now(timezone('US/Eastern')).strftime('%Y%m%d')

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
    'owner': 'SALESRECON',
    'email': email,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': pendulum.yesterday().in_timezone('US/Eastern'),
    'retries': 0
}

params_Oracle_GL_raw_table_audit = {
    'snowflake_rds_db' : confs.get('snowflake_rds_database_name'), 
    'dest_schema' : confs.get('schema_id'),
    'snowflake_warehouse_name' : confs.get('warehouse_id'),
    'RDS_TABLE' : 'ORACLE_GL',
    'SOURCE_SYSTEM_NAME' : 'OracleGL',
    'LOADID_C' : 'LOADID',
    'STAGE_NAME' :  confs.get('stage_name')+'/OracleGL',
    'STAGE_FILE_FORMAT_NAME' : confs.get('file_format'), 
    'Loaddate' : current_date,
    'filename_column' : 'FILENAME',
    'ARCHIVE_PATH_EXIST' : 'null',
    'POD' : 'SALES_RECON'
}

# DAG initialization
with DAG(
    dag_id=confs.get('dag_id'),
    description=confs.get('dag_description'),
    default_args=default_args,
    schedule_interval=None,
    tags=['GL', 'SALESRECON_JJ', 'ADLS_TO_RDS'],
    template_searchpath=confs.get('sql_path'),
    dagrun_timeout=timedelta(minutes=600),
    catchup=False
) as dag:

    Start = BashOperator(task_id='Start', bash_command='exit 0', dag=dag)
    End = BashOperator(task_id='End', bash_command='exit 0', dag=dag)

trigger_audit_Oracle_GL = TriggerDagRunOperator(
           task_id='trigger_audit_Oracle_GL',
           trigger_dag_id='UDP_Audit_Validation',
           conf=params_Oracle_GL_raw_table_audit,
           dag=dag 
           ) 

JJ_ORACLEGL_ADLS_TO_RDS_ALL_HI_PL = SnowflakeOperator(
       task_id='JJ_ORACLEGL_ADLS_TO_RDS_ALL_HI_PL',
       snowflake_conn_id=snowflake_conn_id,
       sql = 'rds/jj/ingest-jj-oraclegl-adls-to-rds.sql',
       params={
            "warehouse": warehouse,
            "schema": schema,
            "env": env,
            "file_format": file_format,
            "pattern": pattern,
            "load_start_dt": from_date,
            "load_end_dt": to_date
       },
       dag=dag
)

# Task Run Flow
Start>>JJ_ORACLEGL_ADLS_TO_RDS_ALL_HI_PL
JJ_ORACLEGL_ADLS_TO_RDS_ALL_HI_PL>>trigger_audit_Oracle_GL
trigger_audit_Oracle_GL>> End
# Task Run Flow End

if __name__ == "__main__":
    dag.cli()
