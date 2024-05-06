CREATE EXTERNAL TABLE vv_db.hvtb_nbx_core_gnip_brandoptimizedquotation(
 uuid string, brandname string, quotation integer, jobcreateddate string, 
 sample_per integer, sample_size double, fromdate string, todate string
 ) 
STORED AS PARQUET 
LOCATION 's3://vv-qa-emr-cluster/data/core/social-media/twitter/gnip/brandoptimization';