CREATE EXTERNAL TABLE hive_schema_stg.seaware_reservation_dim(
  res_id int, 
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  src_res_id int, 
  res_init_date timestamp, 
  res_guest_count int, 
  res_mode varchar(15), 
  cancellation_date timestamp, 
  res_status char(2), 
  operator varchar(30), 
  res_type varchar(15), 
  is_internal char(1), 
  source_code varchar(15), 
  effective_date timestamp, 
  md5_dim varchar(500), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp, 
  vip_status varchar(60), 
  stage varchar(120), 
  booking_source varchar(120), 
  last_modifiedby_id varchar(100), 
  hotel_flag char(1), 
  sail_id int)
STORED AS PARQUET  
LOCATION
  's3://vv-dev-emr-cluster/data/staging/seaware/hvtb_nbx_staging_sw_reservation_dim'
;
