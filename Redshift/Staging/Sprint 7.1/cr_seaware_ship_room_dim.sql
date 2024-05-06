drop table if exists hive_schema_stg.seaware_ship_room_dim;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_ship_room_dim(
  ship_room_id int, 
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  src_ship_room_id int, 
  facility_id int, 
  room_number int, 
  room_name varchar(255), 
  room_size int, 
  max_occupancy int, 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-staging-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_ship_room_dim'
;