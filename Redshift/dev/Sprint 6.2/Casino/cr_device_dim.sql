CREATE EXTERNAL TABLE hive_schema_stg.device_dim(
  device_skey bigint, 
  voyage_id varchar(100), 
  device_id bigint, 
  device_type_id bigint, 
  device_type_name varchar(100), 
  device_sub_type_name varchar(100), 
  device_machine_number varchar(100), 
  load_dt timestamp, 
  upd_dt timestamp, 
  primaryhash varchar(500), 
  md5_hash varchar(500))
STORED AS PARQUET  
LOCATION
  's3://vv-dev-emr-cluster/data/mart/hvtb_mart_dim_device'
;