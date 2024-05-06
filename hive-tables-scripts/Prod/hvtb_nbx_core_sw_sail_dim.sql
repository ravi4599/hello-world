CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_core_sw_sail_dim`(
  `sail_id` int, 
  `rec_start_dttm` timestamp, 
  `rec_end_dttm` timestamp, 
  `src_sail_id` int, 
  `sail_days` int, 
  `sail_route_code` string, 
  `sail_port_from` string, 
  `sail_port_to` string, 
  `sail_geog_area_code` string, 
  `dep_ref_id` int, 
  `arr_ref_id` int, 
  `sail_date_from` timestamp, 
  `sail_date_to` timestamp, 
  `md5_dim` string, 
  `etl_ld_status` string, 
  `etl_ld_dt` timestamp, 
  `etl_upd_dt` timestamp, 
  `is_active` string, 
  `ship_id` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_sail_dim'
TBLPROPERTIES (
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"sail_id","type":"integer","nullable":true,"metadata":{}},{"name":"rec_start_dttm","type":"timestamp","nullable":true,"metadata":{}},{"name":"rec_end_dttm","type":"timestamp","nullable":true,"metadata":{}},{"name":"src_sail_id","type":"integer","nullable":true,"metadata":{}},{"name":"sail_days","type":"integer","nullable":true,"metadata":{}},{"name":"sail_route_code","type":"string","nullable":true,"metadata":{}},{"name":"sail_port_from","type":"string","nullable":true,"metadata":{}},{"name":"sail_port_to","type":"string","nullable":true,"metadata":{}},{"name":"sail_geog_area_code","type":"string","nullable":true,"metadata":{}},{"name":"dep_ref_id","type":"integer","nullable":true,"metadata":{}},{"name":"arr_ref_id","type":"integer","nullable":true,"metadata":{}},{"name":"sail_date_from","type":"timestamp","nullable":true,"metadata":{}},{"name":"sail_date_to","type":"timestamp","nullable":true,"metadata":{}},{"name":"md5_dim","type":"string","nullable":true,"metadata":{}},{"name":"etl_ld_status","type":"string","nullable":true,"metadata":{}},{"name":"etl_ld_dt","type":"timestamp","nullable":true,"metadata":{}},{"name":"etl_upd_dt","type":"timestamp","nullable":true,"metadata":{}},{"name":"is_active","type":"string","nullable":true,"metadata":{}},{"name":"ship_id","type":"integer","nullable":true,"metadata":{}}]}', 
  'transient_lastDdlTime'='1574304720');
