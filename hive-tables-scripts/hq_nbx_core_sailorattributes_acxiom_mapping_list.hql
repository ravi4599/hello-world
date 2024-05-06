CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_core_sailorattributes_acxiom_mapping_list`(
  `Master_Affinities` string, 
  `Values_From_Data` string, 
  `alternate` string,
  `attribute` string,
  `IsBooleanColumn` string
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3a://vv-training-emr-cluster/data/core/sailor_attributes/acxiom_mapping_list';
