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
emails = confs.get('emails')
dag_config = confs.get('dag_config')
airflow_vars = Variable.get(key=dag_config.get('dag_id'), default_var=dict(), deserialize_json=True)
sys.path.append(confs.get('utils_path'))
sys.path.append(confs.get('airflow_path'))
# Update dag config agnostic of environment
dag_config.update(start_date=pendulum.yesterday().in_timezone('US/Eastern'))
dag_config.get('default_args').update(retry_delay = timedelta(seconds=30))

with DAG(**dag_config) as dag:

    Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 

    E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_DAILY = TriggerDagRunOperator(
        task_id='E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_DAILY',
        trigger_dag_id=confs['child_dags']['E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_DAILY'],
        wait_for_completion=True,
        poke_interval=60
    )

    E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_WEEKLY = TriggerDagRunOperator(
        task_id='E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_WEEKLY',
        trigger_dag_id=confs['child_dags']['E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_WEEKLY'],
        wait_for_completion=True,
        poke_interval=60
    )
    
    E2E_SALES_RECON_PARBRINKS_DQ = TriggerDagRunOperator(
        task_id='E2E_SALES_RECON_PARBRINKS_DQ',
        trigger_dag_id=confs['child_dags']['E2E_SALES_RECON_PARBRINKS_DQ'],
        wait_for_completion=True,
        poke_interval=60
    )    

    End = BashOperator(task_id='End', bash_command='exit 0')
    
###### ORCHESTRATION Sequence #########    
chain(Start, [E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_DAILY])
cross_downstream([E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_DAILY],[E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_WEEKLY])
cross_downstream([E2E_SALES_RECON_PARBRINKS_IDS_TO_IDS_WEEKLY],[E2E_SALES_RECON_PARBRINKS_DQ])
cross_downstream([E2E_SALES_RECON_PARBRINKS_DQ],[End])

