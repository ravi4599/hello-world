CREATE EXTERNAL TABLE hive_schema_stg.seaware_sail_cabin_fact(
  snapshot_time timestamp, 
  sail_id int, 
  cabin_category varchar(5), 
  cabin_category_rank int, 
  generic_category varchar(30), 
  max_capacity int, 
  num_of_cabins int, 
  ok_abs int, 
  ok_wgt float, 
  gty_abs int, 
  gty_wgt float, 
  wtl_abs int, 
  wtl_wgt float, 
  avail_cabins int, 
  avail_abs int, 
  avail_wgt float, 
  avail_nested int, 
  num_of_reserved int, 
  in_allotments int, 
  num_of_upgrade int, 
  num_of_downgrade int, 
  shared_abs int, 
  shared_allotment_abs int, 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp, 
  na_abs int, 
  ok_gtry_rstr_abs int, 
  taxes_and_fees float, 
  voyage_fare float, 
  price_total float)
PARTITIONED BY ( 
  snapshot_date date)
STORED AS PARQUET  
LOCATION
  's3://vv-qa-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_sail_cabin_fact'
;

ALTER TABLE hive_schema_stg.seaware_sail_cabin_fact ADD IF NOT EXISTS  PARTITION (snapshot_date = '2020-01-28') location 's3://vv-qa-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_sail_cabin_fact/snapshot_date=2020-01-28';