drop table if exists hive_schema_stg.seaware_ship_dim;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_ship_dim(
  ship_id int, 
  ship varchar(10), 
  ship_name varchar(50), 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-staging-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_ship_dim'
;