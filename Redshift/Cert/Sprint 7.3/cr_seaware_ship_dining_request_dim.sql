drop table if exists hive_schema_stg.seaware_ship_dining_request_dim;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_ship_dining_request_dim(
  ship_dining_req_id int, 
  src_request_id int, 
  effective_date timestamp, 
  src_guest_id int, 
  dining_kind varchar(30), 
  src_facility_id int, 
  allocation_timestamp timestamp, 
  dining_date timestamp, 
  dining_time_from timestamp, 
  dining_time_to timestamp, 
  party_size int,
  inventory_status varchar(15),	
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-qa-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_ship_dining_request_dim'
;