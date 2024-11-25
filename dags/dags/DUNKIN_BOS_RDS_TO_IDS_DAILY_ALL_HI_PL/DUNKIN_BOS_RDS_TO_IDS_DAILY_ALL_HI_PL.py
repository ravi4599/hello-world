"""
This DAG loads DUNKIN'S BOS data from RDS to IDS Daily table in snowflake.
"""

import sys, pendulum
import os
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow import DAG
from airflow.models import Variable
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# --------------------AF_VARS------------------------------
default_load_variable = {
    "ld_start_dt": "",
    "ld_end_dt": ""
}
af_params =  Variable.get('DUNKIN_BOS_RDS_TO_IDS_DAILY_ALL_HI_PL', default_load_variable, deserialize_json=True)

today = pendulum.now('US/Eastern').add(days=0).format('YYYYMMDD')
today_dt = datetime.strptime(today, "%Y%m%d")

from_date = af_params['ld_start_dt'] if af_params['ld_start_dt'] != '' \
    else pendulum.now('US/Eastern').add(days=-90).format('YYYYMMDD') if today_dt.weekday() == 6 \
    else pendulum.now('US/Eastern').add(days=-1).format('YYYYMMDD')
to_date = af_params['ld_end_dt'] if af_params['ld_end_dt'] != '' \
    else pendulum.now('US/Eastern').add(days=1).format('YYYYMMDD')


# --------------------Confs------------------------------

# Get configs and load necessary path
confs = load_confs()
email = confs.get('email')
sys.path.append(confs.get('utils_path'))
sys.path.append(confs.get('airflow_path'))


# Environment and Connection setup
snowflake_conn_id = confs.get('snowflake_conn_id')
source_env = confs.get('source_env')
target_env = confs.get('target_env')
warehouse = confs.get('warehouse_id')

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

# DAG initialization
with DAG(
    dag_id=confs.get('dag_id'),
    description=confs.get('dag_description'),
    default_args=default_args,
    template_searchpath=confs.get('sql_path'),
    schedule_interval=None,
    tags=['BOS', 'SALESRECON_DUNKIN', 'RDS_TO_IDS'],
    dagrun_timeout=timedelta(minutes=600),
    catchup=False
) as dag:

    Start = BashOperator(task_id='Start', bash_command='exit 0')
    End = BashOperator(task_id='End', bash_command='exit 0')
    DUNKIN_BOS_RDS_TO_IDS_DAILY = SnowflakeOperator(
       task_id='DUNKIN_BOS_RDS_TO_IDS_DAILY_ALL_HI_PL',
       snowflake_conn_id=snowflake_conn_id,
       sql='ids/dunkin/transform-dunkin-bos-crunchtime-daily-rds-to-ids.sql',
       params={
           "warehouse": warehouse,
           "source_env": source_env,
           "target_env": target_env,
           "load_start_dt": from_date,
           "load_end_dt": to_date
           }
     )

# Task Run Flow
Start >> DUNKIN_BOS_RDS_TO_IDS_DAILY >> End
# Task Run Flow End

if __name__ == "__main__":
    dag.cli()
 