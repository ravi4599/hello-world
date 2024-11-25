import sys, pendulum
import os
from pytz import timezone
from datetime import datetime
from dateutil import parser
from datetime import timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.models import Variable
from data_inbound_outbound_framework.custom.adf_custom.operators.adf_operators import RunADFPipelineoperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# -------------------AF_VARS------------------------------
default_load_variable = {
    "ld_dt": ""
}
af_params = Variable.get('DNKN_POLLED_SALES_WEEKLY' ,default_load_variable, deserialize_json=True)


from_date = af_params['ld_dt'] if af_params['ld_dt'] != '' else pendulum.now('America/New_York').format('YYYY-MM-DD')
current_date = datetime.now(timezone('America/New_York')).strftime('%Y%m%d')


loaddt = parser.parse(from_date)
loaddt = loaddt.strftime("%Y%m%d")
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
file_name = 'DKN_UDP_Finance_PolledSales_' + loaddt + '_Weekly.dat'


# ADLS Gen2 variables
adls_gen2_config = confs.get('azure').get('adls_gen2')
adls_gen2_base_path = adls_gen2_config.get('base_path')
adls_gen2_container = adls_gen2_config.get('container_name')
adls_gen2_directory = adls_gen2_config.get('directory')
adls_gen2_file_name = adls_gen2_config.get('file_name')

#ADF variables
adf_config = confs.get('azure').get('adf')
adf_connection_id = adf_config.get('connection_id')
adf_pipeline_name_PolledSales_PL = adf_config.get('pipeline_name').get('PolledSales_PL')
adf_factory_name = adf_config.get('factory_name')
adf_resource_group = adf_config.get('resource_group')

#adf_FileSourceHost = adf_config.get('parameters').get('FileSourceHost')
#adf_FileSourceDirectory = adf_config.get('parameters').get('FileSourceDirectory')
#adf_FileSourceUserDomain = adf_config.get('parameters').get('FileSourceUserDomain')
#adf_FileSourceUserName = adf_config.get('parameters').get('FileSourceUserName')
adf_FileSourcePasswordSecretName = adf_config.get('parameters').get('FileSourcePasswordSecretName')
adf_Targetfilepath = adf_config.get('parameters').get('Targetfilepath')


# Setting up variables
airflow_vars = Variable.get(key=confs.get('dag_id'), default_var=dict(), deserialize_json=True)

# Default arguments and email_on_failure set to false because it's true in master
default_args = {
    'owner': 'SALESRECON_DUNKIN',
    'email': email,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    #'start_date': pendulum.now().in_timezone('America/New_York'),
    'start_date': pendulum.datetime(2024, 2, 8).in_tz('America/New_York'),
    'retries': 0
}

# DAG initialization 
  
with DAG(
    dag_id = confs.get('dag_id'),
    description = confs.get('dag_description'),
    default_args = default_args,
    #start_date = pendulum.yesterday().in_timezone('America/New_York'),
    schedule_interval = confs.get('schedule_interval'),
    tags = ['DNKN', 'SALESRECON_DUNKIN', 'IDH_TO_ADLS'],
    template_searchpath = confs.get('sql_path'),
    dagrun_timeout=timedelta(minutes=600),
    catchup = False
) as dag:

 Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 
 End = BashOperator(task_id='End', bash_command='exit 0')
 COPY_TO_ADLS = SnowflakeOperator(
    task_id='COPY_TO_ADLS',
    snowflake_conn_id=snowflake_conn_id,
    sql = 'polled_sales/dunkin/unload-dunkin-pos-rds-to-adls-weekly.sql',
    params={
             "warehouse": warehouse
            , "schema": schema
            , "env": env
            , "file_format": file_format
            , "load_dt": from_date
             , "file_name": file_name
    }
  )


#Move export from ADLS Gen 2 into Oracle server
 COPY_TO_ORACLE_PIPELINE = RunADFPipelineoperator(
        task_id='COPY_TO_ORACLE_PIPELINE',
       adf_connection_id=adf_connection_id,
        adf_pipeline_name=adf_pipeline_name_PolledSales_PL,
        adf_factory_name=adf_factory_name,
        adf_resource_group=adf_resource_group,
        parameters={
                "Container" : adls_gen2_container,
                "Directory" : adls_gen2_directory,
                "FileSourcePasswordSecretName" : adf_FileSourcePasswordSecretName,
                "Targetfilepath" : adf_Targetfilepath,
                "Sourcefilename" : file_name,
                "Targetfilepath" : adf_Targetfilepath 
        }
    )

#Task Run Flow
Start>>COPY_TO_ADLS>>COPY_TO_ORACLE_PIPELINE>>End
#Task Run Flow End

if __name__ == "__main__":
    dag.cli()
 