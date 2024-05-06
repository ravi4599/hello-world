CREATE OR REPLACE VIEW shipdw.device_dim      
AS SELECT * FROM hive_schema_stg.device_dim with no schema binding;

GRANT USAGE ON SCHEMA shipdw TO  group Tableau_poweruser_group;
GRANT USAGE ON SCHEMA shipdw TO  group integration_user_group;

grant select on all tables in schema shipdw to group Tableau_poweruser_group ;
grant select on all tables in schema shipdw to group integration_user_group ;
