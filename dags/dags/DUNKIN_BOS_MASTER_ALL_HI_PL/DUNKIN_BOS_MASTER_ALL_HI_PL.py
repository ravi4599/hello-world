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

# Update dag config agnostic of environment
dag_config.update(start_date=pendulum.yesterday().in_timezone('US/Eastern'))
dag_config.get('default_args').update(retry_delay=timedelta(seconds=30))


schedule_interval = dag_config.get('schedule_interval')
if not schedule_interval:
    schedule_interval = None
   
dag_config.update(schedule_interval=schedule_interval)
   
with DAG(**dag_config) as dag:

    Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 

    DUNKIN_BOS_SFTP_TO_ADLS = TriggerDagRunOperator(
        task_id='DUNKIN_BOS_SFTP_TO_ADLS_ALL_HI_PL',
        trigger_dag_id=confs['child_dags']['DUNKIN_BOS_SFTP_TO_ADLS_ALL_HI_PL'],
        wait_for_completion=True,
        poke_interval=60
    )

    DUNKIN_BOS_ADLS_TO_RDS = TriggerDagRunOperator(
        task_id='DUNKIN_BOS_ADLS_TO_RDS_ALL_HI_PL',
        trigger_dag_id=confs['child_dags']['DUNKIN_BOS_ADLS_TO_RDS_ALL_HI_PL'],
        wait_for_completion=True,
        poke_interval=60
    )
    
    DUNKIN_BOS_RDS_TO_IDS_DAILY = TriggerDagRunOperator(
        task_id='DUNKIN_BOS_RDS_TO_IDS_DAILY_ALL_HI_PL',
        trigger_dag_id=confs['child_dags']['DUNKIN_BOS_RDS_TO_IDS_DAILY_ALL_HI_PL'],
        wait_for_completion=True,
        poke_interval=60
    )

    DUNKIN_BOS_DQ = TriggerDagRunOperator(
        task_id='DUNKIN_BOS_DQ',
        trigger_dag_id=confs['child_dags']['DUNKIN_BOS_DQ'],
        wait_for_completion=True,
        poke_interval=60
    )
     
    End = BashOperator(task_id='End', bash_command='exit 0')
    
  ##------ ORCHESTRATION Sequence ---------------
chain(Start, [DUNKIN_BOS_SFTP_TO_ADLS])
cross_downstream([DUNKIN_BOS_SFTP_TO_ADLS],[DUNKIN_BOS_ADLS_TO_RDS])
cross_downstream([DUNKIN_BOS_ADLS_TO_RDS],[DUNKIN_BOS_RDS_TO_IDS_DAILY])
cross_downstream([DUNKIN_BOS_RDS_TO_IDS_DAILY],[DUNKIN_BOS_DQ])
cross_downstream([DUNKIN_BOS_DQ],[End])