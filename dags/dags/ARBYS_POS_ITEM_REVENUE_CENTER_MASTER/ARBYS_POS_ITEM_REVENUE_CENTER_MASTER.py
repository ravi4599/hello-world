from datetime import datetime, timedelta
from typing import Iterable
import sys, pendulum
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.helpers import cross_downstream
from airflow.models import DAG
from airflow.models import Variable
from data_inbound_outbound_framework.core.utils import load_confs

confs = load_confs()
email = confs.get('email')
dag_config = confs.get('dag_config')
airflow_vars = Variable.get(key=dag_config.get('dag_id'), default_var=dict(), deserialize_json=True)
sys.path.append(confs.get('utils_path'))
sys.path.append(confs.get('airflow_path'))

schedule_interval = dag_config.get('schedule_interval')
if not schedule_interval:
    schedule_interval = None
    
# Update dag config agnostic of environment
dag_config.update(start_date=pendulum.yesterday().in_timezone('US/Eastern'))
dag_config.get('default_args').update(retry_delay = timedelta(seconds=30))
dag_config.update(schedule_interval=schedule_interval)


with DAG(**dag_config) as dag:

    Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 

    ARBYS_POS_ITEM_ADLS_TO_RDS = TriggerDagRunOperator(
        task_id='ARBYS_POS_ITEM_ADLS_TO_RDS',
        trigger_dag_id=confs['child_dags']['ARBYS_POS_ITEM_ADLS_TO_RDS'],
        wait_for_completion=True,
        poke_interval=60
    )

    ARBYS_POS_REVENUE_CENTER_ADLS_TO_RDS = TriggerDagRunOperator(
        task_id='ARBYS_POS_REVENUE_CENTER_ADLS_TO_RDS',
        trigger_dag_id=confs['child_dags']['ARBYS_POS_REVENUE_CENTER_ADLS_TO_RDS'],
        wait_for_completion=True,
        poke_interval=60
    )

    ARBYS_POS_REVENUE_CENTER_RDS_TO_IDS_DAILY = TriggerDagRunOperator(
        task_id='ARBYS_POS_REVENUE_CENTER_RDS_TO_IDS_DAILY',
        trigger_dag_id=confs['child_dags']['ARBYS_POS_REVENUE_CENTER_RDS_TO_IDS_DAILY'],
        wait_for_completion=True,
        poke_interval=60
    )

    ARBYS_POS_ITEM_RDS_TO_IDS_DAILY = TriggerDagRunOperator(
        task_id='ARBYS_POS_ITEM_RDS_TO_IDS_DAILY',
        trigger_dag_id=confs['child_dags']['ARBYS_POS_ITEM_RDS_TO_IDS_DAILY'],
        wait_for_completion=True,
        poke_interval=60
    ) 

    End = BashOperator(task_id='End', bash_command='exit 0')
    
###### ORCHESTRATION Sequence #########    
chain(Start, [ARBYS_POS_ITEM_ADLS_TO_RDS])
cross_downstream([ARBYS_POS_ITEM_ADLS_TO_RDS],[ARBYS_POS_REVENUE_CENTER_ADLS_TO_RDS])
cross_downstream([ARBYS_POS_REVENUE_CENTER_ADLS_TO_RDS],[ARBYS_POS_REVENUE_CENTER_RDS_TO_IDS_DAILY])
cross_downstream([ARBYS_POS_REVENUE_CENTER_RDS_TO_IDS_DAILY],[ARBYS_POS_ITEM_RDS_TO_IDS_DAILY])
cross_downstream([ARBYS_POS_ITEM_RDS_TO_IDS_DAILY],[End])
