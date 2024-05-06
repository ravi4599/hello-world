CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_landing_sw_rpl_ship_cabin`(
	`ship_code` string,
	`cabin_number` string,
	`cabin_id` int,
	`deck_number` int,
	`cabin_name` string,
	`comments` string,
	`image_id` int,
	`cabin_rank` int,
	`firezone_code` string,
	`ext_cabin_id` int,
	`record_added_manually` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='', 
  'line.delim'='\n', 
  'serialization.format'='') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_ship_cabin';
  
  
/*MSH-23760_BrettMorris_20200312*/