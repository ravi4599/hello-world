create or replace view crm.vts_performance_targets_lkp as select * from hive_schema_stg.vts_performance_targets_lkp with no schema binding;

GRANT USAGE ON SCHEMA crm TO  group Tableau_poweruser_group;

grant select on all tables in schema crm to group Tableau_poweruser_group;