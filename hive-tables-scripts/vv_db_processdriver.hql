CREATE EXTERNAL TABLE `vv_db.processdriver`(
  `processname` string, 
  `variablename` string, 
  `value` string
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3a://vv-qa-emr-cluster/data/core/processdriver';