drop table if exists hive_schema_stg.seaware_change_history;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_change_history
(
   change_id          bigint         ,
   src_res_id         bigint         ,
   attrib_name        varchar(500)   ,
   paramtype          varchar(500)   ,
   old_value          varchar(500)   ,
   new_value          varchar(500)   ,
   changed_by         varchar(500)   ,
   changed_timestamp  timestamp      ,
   load_dt            timestamp      ,
   upd_dt             timestamp )     
PARTITIONED BY ( 
  part_dt date)       
STORED AS PARQUET   
LOCATION
  's3://vv-dev-emr-cluster/data/mart/hvtb_mart_fact_seaware_change_history'
;
