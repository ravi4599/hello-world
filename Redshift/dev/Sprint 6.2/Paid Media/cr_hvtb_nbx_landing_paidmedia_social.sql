CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_landing_paidmedia_social(
  date date, 
  year int, 
  campaign varchar(200), 
  week date, 
  objective varchar(100), 
  channel varchar(50), 
  platform varchar(100), 
  audience_tactics varchar(200), 
  ad_name varchar(500), 
  ad_format varchar(100), 
  amount_spent_usd float, 
  impressions float, 
  link_clicks int, 
  leads int, 
  bookings int, 
  audience_tactic2 varchar(100), 
  updated_audience_tactics varchar(200), 
  post_comments int, 
  post_engagement int, 
  post_reactions int, 
  post_saves int, 
  post_shares int)
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
  's3://vv-dev-emr-cluster/data/landing/Marketing/WeeklySocialDash'
;