drop table if exists hive_schema_stg.seaware_cabin_master;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_cabin_master(
  cabin_id int, 
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  cabin_number varchar(10), 
  ship varchar(10), 
  deck_number int, 
  cabin_name varchar(30), 
  cabin_rank int, 
  cabin_category varchar(4), 
  cabin_category_rank int, 
  cabin_category_generic varchar(15), 
  cabin_category_generic_rank int, 
  cabin_capacity int, 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp, 
  md5 varchar(500))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://vv-qa-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_cabin_master'
;