import json
import logging
import time

import pendulum
from airflow import AirflowException
from airflow.models import BaseOperator, TaskInstance, DagModel, DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.api.common.experimental.trigger_dag import trigger_dag
from sqlalchemy import func, desc


class WaitParentDagOperator(BaseOperator):
    """
    Custom Airflow Operator responsible for holding the execution until an external DAG marked as parent
    complete its execution in the time range defined during the instantiation.
    In real life:

    -(time limit) ---> child-triggering ---> parent-execution-end-time ---> +(time limit)

    -(time limit) ---> parent-execution-end-time ---> child-triggering ---> +(time limit)

    The DAG creating this sensor will fail in case no parent is triggered before the `timedelta` boundary
    """

    TI = TaskInstance
    DM = DagModel
    DR = DagRun

    @apply_defaults
    def __init__(self,
                 parent_dag_id,  # type: str
                 timedelta_seconds_interval,  # type: long
                 parent_task_ids=None,  # type: list
                 incremental_wait_seconds=10,  # type: long
                 *args,
                 **kwargs):
        super(WaitParentDagOperator, self).__init__(*args, **kwargs)
        self.parent_task_ids = parent_task_ids
        self.parent_dag_id = parent_dag_id
        self.timedelta_interval = timedelta_seconds_interval
        self.timeout = timedelta_seconds_interval * 1.1
        self.incremental_wait_seconds = incremental_wait_seconds

    @provide_session
    def pre_execute(self, context, session):

        # Checking if DAG exists
        dag_to_wait = session.query(WaitParentDagOperator.DM).filter(
            WaitParentDagOperator.DM.dag_id == self.parent_dag_id
        ).first()

        if not dag_to_wait:
            raise AirflowException('The external DAG {} does not exist.'.format(self.parent_dag_id))

    @provide_session
    def execute(self, context, session=None):

        child_trigger_time = context["execution_date"]
        diff_now_child_timeout = pendulum.now().diff(child_trigger_time).in_seconds()

        if diff_now_child_timeout > self.timeout:
            raise AirflowException(
                'Child marked as failed because no parent was triggered '
                'OR parent finished time was greater than timedelta_seconds_interval')

        all_executions = []
        if self.parent_task_ids:
            for parent_task in self.parent_task_ids:
                max_task_dag_date = self.__get_max_execution_date_subquery(session, parent_task)
                query = session.query(WaitParentDagOperator.TI) \
                    .filter(
                    WaitParentDagOperator.TI.dag_id == self.parent_dag_id,
                    WaitParentDagOperator.TI.task_id == parent_task,
                    WaitParentDagOperator.TI.end_date == max_task_dag_date,
                    WaitParentDagOperator.TI.state == State.SUCCESS
                ).order_by(desc(WaitParentDagOperator.TI.end_date))
                all_executions.extend(query.all())

        else:
            max_task_dag_date = self.__get_max_execution_date_subquery(session)
            query = session.query(WaitParentDagOperator.DR) \
                .filter(
                WaitParentDagOperator.DR.dag_id == self.parent_dag_id,
                WaitParentDagOperator.DR.end_date == max_task_dag_date,
                WaitParentDagOperator.DR.state == State.SUCCESS
            ).order_by(desc(WaitParentDagOperator.DR.end_date))
            all_executions.extend(query.all())

        logging.info(
            "Tasks {} in DAG {} has {} executions".format(self.parent_task_ids, self.parent_dag_id,
                                                          len(all_executions)))

        parent_task = list(filter(
            lambda row: abs(child_trigger_time.diff(row.end_date).in_seconds()) < self.timedelta_interval,
            all_executions))

        logging.info("All Tasks with max are {} and elegible are {}".format(len(all_executions), len(parent_task)))

        logging.info("No eligible instance of Tasks {} in DAG {} found. Retrying...".format(self.parent_task_ids,
                                                                                            self.parent_dag_id))
        if len(all_executions) == 0 \
                or (self.parent_task_ids and len(parent_task) < len(self.parent_task_ids)) \
                or len(parent_task) < 1: #last in case of DAG without task
            time.sleep(self.incremental_wait_seconds)
            self.incremental_wait_seconds *= 1.1
            self.execute(context, session)

    def __get_max_execution_date_subquery(self, session, parent_task=None):
        if parent_task:
            return session.query(
                func.max(WaitParentDagOperator.TI.end_date)) \
                .filter(WaitParentDagOperator.TI.dag_id == self.parent_dag_id,
                        WaitParentDagOperator.TI.task_id == parent_task) \
                .group_by(WaitParentDagOperator.TI.dag_id, WaitParentDagOperator.TI.task_id)
        else:
            return session.query(
                func.max(WaitParentDagOperator.DR.end_date)) \
                .filter(WaitParentDagOperator.DR.dag_id == self.parent_dag_id) \
                .group_by(WaitParentDagOperator.DR.dag_id)


class TriggerDagAndWaitOperator(TriggerDagRunOperator):
    DR = DagRun

    class DagRunOrder(object):
        def __init__(self, run_id=None, payload=None):
            self.run_id = run_id
            self.payload = payload

    def __init__(self, trigger_dag_id,
                 python_callable=None,
                 execution_date=None,
                 *args,
                 **kwargs):
        super(TriggerDagAndWaitOperator, self) \
            .__init__(trigger_dag_id=trigger_dag_id,
                      python_callable=python_callable,
                      execution_date=execution_date,
                      *args,
                      **kwargs)

    def fetch_dag_run_by_ids(self,
                             session,
                             dag_id,
                             run_id,
                             retry_delay_seconds=30):
        logging.info("Waiting dag_id {} to finish the run with id {}".format(dag_id, run_id))
        query = session.query(TriggerDagAndWaitOperator.DR) \
            .filter(TriggerDagAndWaitOperator.DR.dag_id == dag_id,
                    TriggerDagAndWaitOperator.DR.run_id == run_id,
                    TriggerDagAndWaitOperator.DR.state == State.SUCCESS)

        execution_opt = query.one_or_none()
        if not execution_opt:
            time.sleep(retry_delay_seconds)
            return self.fetch_dag_run_by_ids(session, dag_id, run_id, int(retry_delay_seconds * 1.5))
        else:
            return execution_opt

    @provide_session
    def execute(self, context, session=None):
        if self.execution_date is not None:
            run_id = 'trig__{}'.format(self.execution_date)
            self.execution_date = timezone.parse(self.execution_date)
        else:
            run_id = 'trig__' + timezone.utcnow().isoformat()
        dro = TriggerDagAndWaitOperator.DagRunOrder(run_id=run_id)
        if self.python_callable is not None:
            dro = self.python_callable(context, dro)
        if dro:
            trigger_dag(dag_id=self.trigger_dag_id,
                        run_id=dro.run_id,
                        conf=json.dumps(dro.payload),
                        execution_date=self.execution_date,
                        replace_microseconds=False)
        else:
            self.log.info("Criteria not met, moving on")
        return self.fetch_dag_run_by_ids(session, self.trigger_dag_id, run_id)
