drop table if exists hive_schema_stg.seaware_revenue_summary_fact;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_revenue_summary_fact(
  snapshot_time timestamp, 
  res_id int, 
  ship_id int, 
  agent_id int, 
  agency_id int, 
  sail_id int, 
  guest_id int,
  currency varchar(100), 
  currency_rate float,   
  amount float, 
  voyage_amount float, 
  taxes_and_fees float, 
  voyage_protect float, 
  addon_fare float, 
  shorex_fare float, 
  com_protected float, 
  invoice_total float, 
  invoice_paid float, 
  gross_due float, 
  net_due float, 
  com_total float, 
  com_paid float, 
  com_bas_total float, 
  com_bon_total float, 
  charge_total float, 
  grand_total float,
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
PARTITIONED BY ( 
  snapshot_date date)
STORED AS PARQUET  
LOCATION
  's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_revenue_summary_fact'
;

ALTER TABLE hive_schema_stg.seaware_revenue_summary_fact ADD IF NOT EXISTS  PARTITION (snapshot_date = '2020-06-03') location 's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_revenue_summary_fact/snapshot_date=2020-06-03';


