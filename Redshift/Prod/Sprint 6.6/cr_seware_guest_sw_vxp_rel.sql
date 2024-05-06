CREATE EXTERNAL TABLE hive_schema_stg.seware_guest_sw_vxp_rel(
  sw_vxp_guest_rel_id int, 
  src_guest_id int, 
  client_id int, 
  vxp_guest_id varchar(100), 
  sail_date_from timestamp, 
  sail_date_to timestamp, 
  src_res_id int, 
  vxp_res_id varchar(100), 
  vxp_reservation_guest_id varchar(100), 
  loadtimestamp timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-prod-emr-cluster/data/hivetables/vv_db/hvtb_nbx_seware_guest_sw_vxp_rel'
;