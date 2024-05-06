CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_landing_sw_rpl_cabin_category`(
	`cabin_category` string,
	`cabin_category_id` int,
	`comments` string,
	`image_id` int,
	`cabin_capacity` int,
	`cabin_category_rank` int,
	`ship_code` string,
	`is_without_cabins` string,
	`category_capacity` int,
	`is_active` string)
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
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_cabin_category';
  
  
  /*MSH-23760_BrettMorris_20200312*/