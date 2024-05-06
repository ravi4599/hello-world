drop table if exists hive_schema_stg.seaware_ship_facility_lkp;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_ship_facility_lkp(
  facility_id int, 
  ship_code varchar(10), 
  facility_type varchar(15), 
  facility_code varchar(15), 
  facility_name varchar(80), 
  deck_number int, 
  facility_size int, 
  max_occupancy int, 
  assign_mode varchar(15), 
  facility_rank int, 
  facility_subtype varchar(15), 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-staging-emr-cluster/data/core/spf/hvtb_nbx_core_spf_ship_facility_lkp'
;