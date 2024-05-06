drop table if exists hive_schema_stg.seaware_res_package_rel;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_res_package_rel(
res_package_id	int,
  src_res_id int, 
  src_guest_id int, 
  src_package_id int, 
   effective_date timestamp,
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-dev-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_res_package_rel'
;

create or replace view seaware.seaware_res_package_rel as select * from hive_schema_stg.seaware_res_package_rel with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;