create or replace view seaware.seaware_revenue_summary_fact as select * from hive_schema_stg.seaware_revenue_summary_fact with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;


grant select on all tables in schema seaware to group Tableau_poweruser_group ;



