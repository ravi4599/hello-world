create or replace view seaware.seaware_ship_dim
as 
select * from hive_schema_stg.seaware_ship_dim
UNION
SELECT
-1,
'NA',
'NA',
'NA',
'1900-01-01 00:00:00',
'1900-01-01 00:00:00' 
FROM  hive_schema_stg.seaware_ship_dim

with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;

GRANT SELECT ON seaware.seaware_ship_dim TO tableau_poweruser_group;



