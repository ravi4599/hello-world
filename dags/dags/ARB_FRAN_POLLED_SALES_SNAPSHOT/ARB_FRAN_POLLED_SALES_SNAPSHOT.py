from data_inbound_outbound_framework.core.dag_factory import DAGFactory

pipeline = DAGFactory.from_name(airflow_var='ARB_FRAN_POLLED_SALES_SNAPSHOT')