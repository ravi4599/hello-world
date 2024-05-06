CREATE EXTERNAL TABLE vv_db.hvtb_nbx_core_gnip_brandvaluetag(
 value string,
 tag string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'  
STORED AS TEXTFILE
location 's3://vv-qa-emr-cluster/data/core/social-media/twitter/gnip/brandvaluetag';