DROP TABLE IF EXISTS hive_schema_stg.shipboard_survey_questions_dim;

CREATE EXTERNAL TABLE hive_schema_stg.shipboard_survey_questions_dim(
question_skey bigint, 
src_question_id varchar(50), 
question_text varchar(500), 
master_question varchar(500), 
category varchar(50), 
type varchar(50), 
master_location varchar(100), 
is_primaryquestion varchar(1), 
is_closingquestion varchar(1), 
load_dt timestamp, 
upd_dt timestamp, 
primaryhash varchar(500), 
md5_hash varchar(500))
STORED AS PARQUET   
LOCATION
  's3://vv-staging-emr-cluster/data/mart/hvtb_mart_shipboard_survey_questions_dim';
  