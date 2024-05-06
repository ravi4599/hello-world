CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_landing_paidmedia_search(
  day date, 
  month varchar(20), 
  week date, 
  engine varchar(100), 
  account varchar(100), 
  campaign varchar(200), 
  geo varchar(30), 
  brand_nb varchar(20), 
  nb_re_launch char(1), 
  cost float, 
  clicks int, 
  impr float, 
  ctr float, 
  avg_cpc float, 
  avg_cpm float, 
  vv_bookings int, 
  search_impr_share int, 
  search_lost_is int, 
  vv_initiate_booking int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-dev-emr-cluster/data/landing/Marketing/WeeklySearchDash'
;
