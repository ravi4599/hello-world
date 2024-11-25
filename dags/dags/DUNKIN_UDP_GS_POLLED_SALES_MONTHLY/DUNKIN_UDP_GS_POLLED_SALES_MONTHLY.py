from data_inbound_outbound_framework.core.dag_factory import DAGFactory

pipeline = DAGFactory.from_name(airflow_var='DUNKIN_UDP_GS_POLLED_SALES_MONTHLY')