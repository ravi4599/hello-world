drop table if exists hive_schema_stg.seaware_ship_room_request_dim;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_ship_room_request_dim(
  ship_room_req_id int, 
  src_request_id int, 
  effective_date timestamp, 
  src_guest_id int, 
  inventory_request_type varchar(15), 
  ship_code varchar(10), 
  facility_type varchar(15), 
  usage_start timestamp, 
  usage_duration int, 
  party_size int, 
  quantity int, 
  allocation_id int, 
  ship_room_id int, 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_ship_room_request_dim'
;