import sys, pendulum
import os
from datetime import datetime,timedelta
from pytz import timezone
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import data_inbound_outbound_framework.custom.default_variables as dv
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --------------------AF_VARS------------------------------
# ld_start_dt = 'yyyymmdd', ld_end_dt = 'yyyymmdd'
default_load_variable = {
    "ld_start_dt": "",
    "ld_end_dt": ""
}
af_params = Variable.get('E2E_SALES_RECON_BWW_ALTAMETRICS_ADLS_TO_RDS', default_load_variable, deserialize_json=True)

file_from_date = af_params['ld_start_dt'] if af_params['ld_start_dt'] != '' else pendulum.now('America/New_York').add(days=-7).format('YYYYMMDD')
file_to_date = af_params['ld_end_dt'] if af_params['ld_end_dt'] != '' else pendulum.now('America/New_York').add(days=0).format('YYYYMMDD')

ld_start_dt = datetime.strptime(file_from_date, "%Y%m%d")
ld_end_dt = datetime.strptime(file_to_date, "%Y%m%d")

ld_start_date =datetime.strftime(ld_start_dt, "%Y-%m-%d")
ld_end_date =datetime.strftime(ld_end_dt, "%Y-%m-%d")

start_date = datetime.fromisoformat(ld_start_date)
end_date = datetime.fromisoformat(ld_end_date)

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
file_format = confs.get('file_format')
pattern = confs.get('pattern')

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

params_bos_altametrics_sales_audit = {
    'snowflake_rds_db' : confs.get('snowflake_rds_database_name'),
    'dest_schema' : confs.get('schema_id'),
    'snowflake_warehouse_name' : confs.get('warehouse_id'),
    'RDS_TABLE' : 'BOS_ALTAMETRICS_SALES',
    'SOURCE_SYSTEM_NAME' : 'Sales',
    'LOADID_C' : 'loadid',
    'STAGE_NAME' : confs.get('stage_name'),
    'STAGE_FILE_FORMAT_NAME' : confs.get('file_format'),
    'Loaddate' : current_date,
    'filename_column' : 'filename',
    'ARCHIVE_PATH_EXIST' : None,
    'POD' : 'SALES_RECON'
}

# DAG initialization
dag =  DAG( dag_id = confs.get('dag_id'),
            description = confs.get('dag_description'),
            default_args = default_args,
            template_searchpath = confs.get('sql_path'),
            start_date=start_date, 
            schedule_interval=None,
            tags = ['BWW', 'SALESRECON_BWW', 'ADLS_TO_RDS'],
            dagrun_timeout=timedelta(minutes=600),
            catchup = False
          )

start_task = BashOperator(task_id='Start', dag=dag, bash_command='exit 0')         
end_task = BashOperator(task_id='End', dag=dag, bash_command='exit 0')
trigger_audit_bos_altametrics_sales = TriggerDagRunOperator(
    task_id='trigger_audit_bos_altametrics_sales',
    trigger_dag_id='UDP_Audit_Validation',
    conf=params_bos_altametrics_sales_audit,
    dag=dag 
)
start_task.set_downstream(end_task)   
prev_task = start_task
for n in range ((end_date - start_date).days + 1):
        date = start_date + timedelta(n)
        task_id = f'BWW_ALTAMETRICS_{date.strftime("%Y-%m-%d")}'
        date_id = f'{date.strftime("%Y%m%d")}'
        task = SnowflakeOperator(
                        task_id=task_id,
                        snowflake_conn_id=snowflake_conn_id,
                        sql = 'rds/bww/ingest-bww-altametrics-adls-to-rds.sql',
                        warehouse=warehouse,
                        schema=schema,
                        dag=dag,
                        params={
                                "warehouse": warehouse
                                , "schema": schema
                                , "env": env
                                , "file_format": file_format
                                , "pattern": pattern 
                                , "from_date": date_id           
                                }
        )
        prev_task.set_downstream(task)
        prev_task = task

        if date == end_date:
            task.set_downstream(trigger_audit_bos_altametrics_sales)
  #End = BashOperator(task_id='End', bash_command='exit 0')



trigger_audit_bos_altametrics_sales.set_downstream(end_task)  
#Task Run Flow


if __name__ == "__main__":
    dag.cli()
 