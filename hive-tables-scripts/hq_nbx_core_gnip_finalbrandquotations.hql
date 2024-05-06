CREATE EXTERNAL TABLE vv_db.hvtb_nbx_core_gnip_finalbrandquotations( 
 uuid string, 
 brandname string, 
 quotation int,
 jobcreateddate string,
 fromDate string,
 toDate string) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001' 
STORED AS TEXTFILE 
LOCATION 's3://vv-qa-emr-cluster/data/core/social-media/twitter/gnip/finalbrandquotations';