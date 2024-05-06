CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_landing_sw_rpl_cabin_category_generic`(
	`record_id` int,
	`cabin_category_generic` string,
	`comments` string,
	`rank` int)
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
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_cabin_category_generic;
  
  
  /*MSH-23760_BrettMorris_20200312*/