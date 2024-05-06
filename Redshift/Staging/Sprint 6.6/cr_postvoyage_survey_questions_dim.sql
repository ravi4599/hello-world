DROP TABLE IF EXISTS hive_schema_stg.postvoyage_survey_questions_dim;

CREATE EXTERNAL TABLE hive_schema_stg.postvoyage_survey_questions_dim(
question_id bigint, 
src_question_id varchar(50),
variable_id varchar(50),
question_text varchar(500),
Type varchar(50),
category varchar(50),
answer_code_min int,
answer_code_max int,
answer_code_min_text varchar(500),
answer_code_max_text varchar(500),
load_dt timestamp,
upd_dt timestamp,
primaryhash varchar(500),
md5_hash varchar(500))
STORED AS PARQUET
LOCATION 's3://vv-staging-emr-cluster/data/core/seaware/hvtb_nbx_core_postvoyage_survey_questions_dim';



