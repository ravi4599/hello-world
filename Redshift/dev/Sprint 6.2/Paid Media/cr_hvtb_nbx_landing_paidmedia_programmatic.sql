CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_landing_paidmedia_programmatic(
  date date, 
  campaign varchar(200), 
  channel varchar(50), 
  platform varchar(100), 
  objective varchar(100), 
  tactic varchar(200), 
  adname varchar(500), 
  impressions float, 
  viewableimpressions int, 
  measurableimpressions int, 
  clicks int, 
  total_conversions int, 
  video_completions int, 
  bookings int, 
  homepage int, 
  all_pages int, 
  month varchar(20), 
  week date, 
  spend float, 
  format varchar(100), 
  audience varchar(100))
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
  's3://vv-dev-emr-cluster/data/landing/Marketing/WeeklyProgrammaticDash'
;
