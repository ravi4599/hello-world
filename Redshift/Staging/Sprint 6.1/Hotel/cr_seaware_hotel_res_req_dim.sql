drop table if exists hive_schema_stg.seaware_hotel_res_req_dim;
CREATE EXTERNAL TABLE hive_schema_stg.seaware_hotel_res_req_dim(
  hotel_res_req_id int, 
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  src_res_id int, 
  src_guest_id int, 
  request_type varchar(30), 
  start_date timestamp, 
  end_date timestamp, 
  city_code varchar(10), 
  hotel_id int, 
  hotel_category varchar(30), 
  room_seq_number int, 
  room_category varchar(30), 
  room_type varchar(30), 
  occupancy int, 
  hotel_space_type varchar(30), 
  allocation_id int, 
  inv_result varchar(30), 
  is_land_component char(1), 
  hotel_bed_type varchar(30), 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp, 
  md5_dim varchar(500))
STORED AS PARQUET  
LOCATION
  's3://vv-staging-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_hotel_res_req_dim'
;