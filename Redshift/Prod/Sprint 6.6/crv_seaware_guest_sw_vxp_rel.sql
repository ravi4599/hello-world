CREATE OR REPLACE VIEW seaware.seware_guest_sw_vxp_rel       
AS SELECT * FROM hive_schema_stg.seware_guest_sw_vxp_rel with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;


grant select on all tables in schema seaware to group Tableau_poweruser_group ;

