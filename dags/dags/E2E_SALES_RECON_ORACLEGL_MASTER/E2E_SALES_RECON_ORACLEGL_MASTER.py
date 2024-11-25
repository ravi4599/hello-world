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
dag_config.update(start_date=pendulum.yesterday().in_timezone('America/New_York'))
dag_config.get('default_args').update(retry_delay = timedelta(seconds=30))

with DAG(**dag_config) as dag:

    Start = BashOperator(task_id='Start', bash_command='exit 0')                                                                 

    E2E_SALES_RECON_ORACLEGL_FTP_TO_ADLS = TriggerDagRunOperator(
        task_id='E2E_SALES_RECON_ORACLEGL_FTP_TO_ADLS',
        trigger_dag_id=confs['child_dags']['E2E_SALES_RECON_ORACLEGL_FTP_TO_ADLS'],
        wait_for_completion=True,
        poke_interval=60
    )

    E2E_SALES_RECON_ORACLEGL_ADLS_TO_RDS = TriggerDagRunOperator(
        task_id='E2E_SALES_RECON_ORACLEGL_ADLS_TO_RDS',
        trigger_dag_id=confs['child_dags']['E2E_SALES_RECON_ORACLEGL_ADLS_TO_RDS'],
        wait_for_completion=True,
        poke_interval=60
    )

    E2E_SALES_RECON_ORACLEGL_RDS_TO_IDS = TriggerDagRunOperator(
        task_id='E2E_SALES_RECON_ORACLEGL_RDS_TO_IDS',
        trigger_dag_id=confs['child_dags']['E2E_SALES_RECON_ORACLEGL_RDS_TO_IDS'],
        wait_for_completion=True,
        poke_interval=60
    )

    E2E_SALES_RECON_ORACLEGL_DQ = TriggerDagRunOperator(
        task_id='E2E_SALES_RECON_ORACLEGL_DQ',
        trigger_dag_id=confs['child_dags']['E2E_SALES_RECON_ORACLEGL_DQ'],
        wait_for_completion=True,
        poke_interval=60
    )    
    
    End = BashOperator(task_id='End', bash_command='exit 0')
    
###### ORCHESTRATION Sequence #########    
chain(Start, [E2E_SALES_RECON_ORACLEGL_FTP_TO_ADLS])
cross_downstream([E2E_SALES_RECON_ORACLEGL_FTP_TO_ADLS],[E2E_SALES_RECON_ORACLEGL_ADLS_TO_RDS])
cross_downstream([E2E_SALES_RECON_ORACLEGL_ADLS_TO_RDS],[E2E_SALES_RECON_ORACLEGL_RDS_TO_IDS])
cross_downstream([E2E_SALES_RECON_ORACLEGL_RDS_TO_IDS],[E2E_SALES_RECON_ORACLEGL_DQ])
cross_downstream([E2E_SALES_RECON_ORACLEGL_DQ],[End])

