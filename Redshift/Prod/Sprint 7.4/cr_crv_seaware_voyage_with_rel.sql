drop table if exists hive_schema_stg.seaware_voyage_with_rel;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_voyage_with_rel(
  voyage_with_rel_id int, 
  vw_header_id int, 
  src_res_id int, 
  vw_type varchar(15), 
  header_is_matched char(1), 
  res_is_matched char(1), 
  is_main char(1), 
  etl_ld_status varchar(30), 
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_voyage_with_rel'
;

create or replace view seaware.seaware_voyage_with_rel as select * from hive_schema_stg.seaware_voyage_with_rel with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;