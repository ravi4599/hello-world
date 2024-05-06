drop table if exists hive_schema_stg.seaware_wearable_dim;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_wearable_dim(
  wearable_id bigint, 
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  wearable_rfid varchar(100), 
  src_guest_id bigint, 
  charge_id varchar(100), 
  name_to_be_itched varchar(100), 
  wearable_color varchar(30), 
  trackingnumber varchar(50), 
  shipping_company varchar(100), 
  shipment_status varchar(30), 
  mailing_address_line1 varchar(200), 
  mailing_address_line2 varchar(200), 
  mailing_address_line3 varchar(200), 
  mailing_address_city varchar(50), 
  mailing_address_state varchar(20), 
  mailing_address_zip varchar(20), 
  mailing_address_countrycode varchar(10), 
  load_dt timestamp, 
  upd_dt timestamp, 
  primaryhash varchar(500), 
  md5_hash varchar(500))
STORED AS PARQUET  
LOCATION
  's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_wearable_dim' 
;
