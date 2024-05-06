create or replace view crm.vts_phone_summary_fact as select * from hive_schema_stg.vts_phone_summary_fact with no schema binding;

GRANT USAGE ON SCHEMA crm TO  group Tableau_poweruser_group;

grant select on all tables in schema crm to group Tableau_poweruser_group ;