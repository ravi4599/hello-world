CREATE EXTERNAL TABLE hive_schema_stg.seaware_hotel_group_req_dim(
  hotel_group_req_id int, 
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  src_group_id int, 
  effective_date timestamp, 
  start_date timestamp, 
  end_date timestamp, 
  request_type varchar(30), 
  occupancy int, 
  quantity int, 
  n_of_guests int, 
  city_code varchar(10), 
  hotel_id int, 
  hotel_category varchar(30), 
  room_category varchar(30), 
  room_type varchar(30), 
  hotel_space_type varchar(30), 
  is_land_component char(1), 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp, 
  md5_dim varchar(500))
STORED AS PARQUET  
LOCATION
  's3://vv-dev-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_hotel_group_req_dim'
;