CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_landing_sw_rpl_cabin_ship_cabin_ctg_link`(
	`RECORD_ID` int,
	`SHIP_CODE` string,
	`CABIN_NUMBER` string,
	`CABIN_CATEGORY` string,
	`EFF_SAIL_FROM` timestamp,
	`EFF_SAIL_TO` timestamp,
	`ROLLAWAY_BEDS` int,
	`CABIN_CAPACITY` int,
	`IS_ACTIVE` string,
	`EXT_CABIN_CONTRACT_TYPE` string,
	`ACCOUNT_NUMBER` string,
	`CHILD_BEDS` int)
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
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_ship_cabin_ctg_link';
  
  /*MSH-23760_BrettMorris_20200312*/