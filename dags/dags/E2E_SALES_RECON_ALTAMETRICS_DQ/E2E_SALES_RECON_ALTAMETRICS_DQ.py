from curses.ascii import EM
import sys, pendulum
from airflow import DAG
from data_inbound_outbound_framework.core.utils import load_confs
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

# Get configs and load necessary path
confs = load_confs()
emails = confs.get('emails')
sys.path.append(confs.get('utils_path'))
sys.path.append(confs.get('airflow_path'))

# Environment and Connection setup
snowflake_conn_id = confs.get('snowflake_conn_irb_objowner_id')
warehouse = confs.get('warehouse_name')
env = confs.get('env')

database = confs.get('database')
schema_name = confs.get('schema_name')
brand = confs.get('brand')
target_schema = confs.get('target_schema')
table_list = confs.get('table_list')

# DAG Local parameters
dag_id = confs.get('dag_id')
dag_description = confs.get('dag_description')
emails = confs.get('emails')
owner = confs.get('owner')
schedule = confs.get('schedule', None)

# Default arguments
default_args = {
    'owner': owner,
    'email': emails,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0
}

# Convert variables data types to avoid errors
def prep_var(var):
    return "" if var is None else var

env = prep_var(env)
brand = prep_var(brand)
target_schema = prep_var(target_schema)

# DAG initialization
with DAG(
    dag_id=dag_id,
    description=dag_description,
    default_args=default_args,
    start_date=pendulum.yesterday().in_timezone('US/Eastern'),
    schedule_interval=schedule,
    tags=['DQ', 'Email','SALESRECON_ARBYS', 'ARB', 'ALTAMETRICS'],
    catchup=False,
    render_template_as_native_obj=True
) as dag:
    # Define tasks
    Start = EmptyOperator(task_id="Start")
    End = EmptyOperator(task_id="End")
    
    with TaskGroup(group_id = 'DQ_Health_Checks', tooltip = 'DQ Health Checks') as DQ_Health_Checks:
        for table in table_list:
            run_health_checks = TriggerDagRunOperator(
                task_id=f"data_quality_check_{table}",
                trigger_dag_id="IRB-Snowflake-DQ-Run_Health_Checks_Generic",
                wait_for_completion=True,
                conf={
                    "warehouse": warehouse,
                    "env": env,
                    "schema": schema_name,
                    "brand": brand,
                    "table_name": table                
                    }
                )


    send_DQ_results_via_email = TriggerDagRunOperator(
            task_id='send_DQ_results_via_email',
            trigger_dag_id="IRB-Snowflake-DQ-Send_Email_Generic",
            wait_for_completion=True,
            conf={
                "snowflake_conn_id": snowflake_conn_id,
                "warehouse": warehouse,
                "env": env,
                "schema": schema_name,
                "brand": brand,
                "table_list": table_list,
                "emails": emails,
                "target_schema": target_schema
                }
            )

# DAG running order        
Start >> DQ_Health_Checks >> send_DQ_results_via_email >> End