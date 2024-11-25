import sys, pendulum
import os
from datetime import datetime
from datetime import timedelta
from pytz import timezone
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.utils.helpers import cross_downstream
import data_inbound_outbound_framework.custom.default_variables as dv

# --------------------AF_VARS------------------------------
default_load_variable = {
    "ld_start_dt": "",
    "ld_end_dt": ""
}
af_params = Variable.get('ARBYS_POS_REVENUE_CENTER_ADLS_TO_RDS', default_load_variable, deserialize_json=True)

file_from_date = af_params['ld_start_dt'] if af_params['ld_start_dt'] != '' else pendulum.now('US/Eastern').add(days=-2).format('YYYYMMDD')
file_to_date = af_params['ld_end_dt'] if af_params['ld_end_dt'] != '' else pendulum.now('US/Eastern').add(days=0).format('YYYYMMDD')

ld_start_dt = datetime.strptime(file_from_date, "%Y%m%d")
ld_end_dt = datetime.strptime(file_to_date, "%Y%m%d")

ld_start_date =datetime.strftime(ld_start_dt, "%Y-%m-%d")
ld_end_date =datetime.strftime(ld_end_dt, "%Y-%m-%d")

start_date = datetime.fromisoformat(ld_start_date)
end_date = datetime.fromisoformat(ld_end_date)

current_date = datetime.now(timezone('US/Eastern')).strftime('%Y%m%d')

# --------------------Confs------------------------------
# Get configs and load necessary path
confs = load_confs()
email = confs.get('emails')
delete_days = confs.get('delete_days')
sys.path.append(confs.get('utils_path'))
sys.path.append(confs.get('airflow_path'))

# Environment and Connection setup
snowflake_conn_id = confs.get('snowflake_conn_id')
env = confs.get('env')
warehouse = confs.get('warehouse_id')
schema = confs.get('schema_id')
file_format = confs.get('file_format')

# Setting up variables
airflow_vars = Variable.get(key=confs.get('dag_id'), default_var=dict(), deserialize_json=True)

# Default arguments
default_args = {
    'owner': 'SALESRECON',
    'email': email,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024,2,21).in_tz('US/Eastern'),
    'retries': 0
}


# DAG initialization

with DAG( dag_id = confs.get('dag_id'),
            description = confs.get('dag_description'),
            default_args = default_args,
            template_searchpath = confs.get('sql_path'),
            start_date=start_date,            
            schedule_interval = None,
            tags = ['ARBYS', 'SALESRECON_ARBYS', 'ADLS_TO_RDS'],
            dagrun_timeout=timedelta(minutes=600),
            catchup = False
          ) as dag:


    Start = BashOperator(task_id='Start', dag=dag, bash_command='exit 0')         
    End = BashOperator(task_id='End', dag=dag, bash_command='exit 0')
    

    prev_task = Start
    with TaskGroup(group_id="REVENUE_CENTER") as REVENUE_CENTER:
        for n in range ((end_date - start_date).days + 1):
            date = start_date + timedelta(n)
            task_id = f'REVENUE_CENTER_{date.strftime("%Y-%m-%d")}'
            date_id = f'{date.strftime("%Y%m%d")}'
            task = SnowflakeOperator(
                        task_id=task_id,
                        snowflake_conn_id=snowflake_conn_id,
                        sql = 'rds/arbys/ingest-arb-pos-revenue-center-adls-to-rds.sql',
                        warehouse=warehouse,
                        schema=schema,
                        dag=dag,
                        params={
                                "warehouse": warehouse
                                , "schema": schema
                                , "env": env
                                , "file_format": file_format
                                , "delete_days" : delete_days
                                , "run_date": date_id           
                                }
            )
            prev_task.set_downstream(task)
            prev_task = task

            if date == end_date:

                task.set_downstream(End)


Start >> REVENUE_CENTER >> End

# Task Run Flow End
if __name__ == "__main__":
    dag.cli()
