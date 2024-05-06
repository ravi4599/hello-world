CREATE EXTERNAL TABLE hive_schema_stg.seaware_hotel_lkp(
  hotel_id int, 
  hotel_code varchar(15), 
  hotel_name varchar(40), 
  hotel_type varchar(15), 
  is_active char(1), 
  hotel_rating int, 
  address_type varchar(15), 
  address_line1 varchar(100), 
  address_line2 varchar(100), 
  address_line3 varchar(100), 
  address_line4 varchar(100), 
  address_city varchar(30), 
  state_code varchar(3), 
  zip varchar(10), 
  country_code varchar(3), 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-dev-emr-cluster/data/core/spf/hvtb_nbx_core_spf_hotel_lkp'
;