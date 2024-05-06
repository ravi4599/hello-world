CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_landing_sw_rpl_cabin_cat_gen_lnk_cabin_cat`(
	`RECORD_ID` int,
	`CABIN_CATEGORY_GENERIC` string,
	`SHIP_CODE` string,
	`CABIN_CATEGORY` string,
	`LINK_TYPE` string,
	`EFF_DATE_FROM` timestamp,
	`EFF_DATE_TO` timestamp)
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
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_cabin_cat_gen_lnk_cabin_cat';
  
  
  /*MSH-23760_BrettMorris_20200312*/