create or replace view seaware.seaware_ship_dining_request_dim as select * from hive_schema_stg.seaware_ship_dining_request_dim with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;