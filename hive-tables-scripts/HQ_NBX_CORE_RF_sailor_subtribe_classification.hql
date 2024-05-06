CREATE EXTERNAL TABLE IF NOT EXISTS vv_db.hvtb_nbx_core_subtribe_sailorclassification(
 id string, 
 subtribe string) 
STORED AS PARQUET 
LOCATION 's3a://vv-qa-emr-cluster/data/core/tribe_subtribe/random_forest_classifier'