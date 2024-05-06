CREATE OR REPLACE VIEW seaware.postvoyage_survey_questions_dim 
AS SELECT * FROM hive_schema_stg.postvoyage_survey_questions_dim with no schema binding;

-- Permissions

GRANT USAGE ON SCHEMA hive_schema_stg TO tableauuser ;
GRANT USAGE ON SCHEMA shipdw TO tableauuser ;
GRANT USAGE ON SCHEMA seaware TO tableauuser ;

GRANT USAGE ON SCHEMA hive_schema_stg TO GROUP Tableau_poweruser_group;
GRANT USAGE ON SCHEMA shipdw TO GROUP Tableau_poweruser_group;
GRANT USAGE ON SCHEMA seaware TO GROUP Tableau_poweruser_group;

GRANT SELECT ON ALL TABLES IN SCHEMA shipdw TO tableauuser ;
GRANT SELECT ON ALL TABLES IN SCHEMA seaware TO tableauuser ;
GRANT SELECT ON ALL TABLES IN SCHEMA hive_schema_stg TO tableauuser ;

GRANT SELECT ON ALL TABLES IN SCHEMA shipdw TO GROUP Tableau_poweruser_group ;
GRANT SELECT ON ALL TABLES IN SCHEMA seaware TO GROUP Tableau_poweruser_group ;
GRANT SELECT ON ALL TABLES IN SCHEMA hive_schema_stg TO GROUP Tableau_poweruser_group ;