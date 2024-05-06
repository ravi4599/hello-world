CREATE OR REPLACE VIEW seaware.seaware_hotel_lkp       
AS SELECT * FROM hive_schema_stg.seaware_hotel_lkp with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;


grant select on all tables in schema seaware to group Tableau_poweruser_group ;

