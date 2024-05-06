CREATE OR REPLACE VIEW shipdw.shipboard_survey_questions_dim 
AS SELECT * FROM hive_schema_stg.shipboard_survey_questions_dim with no schema binding;

GRANT USAGE ON SCHEMA shipdw TO GROUP Tableau_poweruser_group;

GRANT SELECT ON ALL TABLES IN SCHEMA shipdw TO GROUP Tableau_poweruser_group ;
