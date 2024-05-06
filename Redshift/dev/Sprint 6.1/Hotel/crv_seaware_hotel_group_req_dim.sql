CREATE OR REPLACE VIEW seaware.seaware_hotel_group_req_dim     
AS SELECT * FROM hive_schema_stg.seaware_hotel_group_req_dim with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;
GRANT USAGE ON SCHEMA seaware TO  group integration_user_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;
grant select on all tables in schema seaware to group integration_user_group ;
