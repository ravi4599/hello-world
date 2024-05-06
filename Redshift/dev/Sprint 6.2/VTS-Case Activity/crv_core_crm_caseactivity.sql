create or replace view crm.core_crm_caseactivity_vw as select * from hive_schema_stg.hvtb_nbx_core_crm_caseactivity with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;


grant select on all tables in schema seaware to group Tableau_poweruser_group ;



