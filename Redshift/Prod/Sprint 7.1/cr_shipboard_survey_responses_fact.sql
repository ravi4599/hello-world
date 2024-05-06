DROP TABLE IF EXISTS hive_schema_stg.shipboard_survey_responses_fact;

CREATE EXTERNAL TABLE hive_schema_stg.shipboard_survey_responses_fact(
request_id varchar(100), 
person_skey bigint, 
ship_id bigint, 
voyage_skey bigint, 
guest_id bigint, 
request_time timestamp, 
response_time timestamp, 
visit_time timestamp, 
location varchar(100), 
question_skey bigint, 
answer_rating bigint, 
answer_text varchar(1000), 
load_dt timestamp, 
upd_dt timestamp)
PARTITIONED BY ( 
part_dt date)
STORED AS PARQUET   
LOCATION
's3://vv-prod-emr-cluster/data/mart/hvtb_mart_shipboard_survey_responses_fact';

ALTER TABLE hive_schema_stg.shipboard_survey_responses_fact ADD IF NOT EXISTS  PARTITION (part_dt = '2020-05-06') location 's3://vv-prod-emr-cluster/data/mart/hvtb_mart_shipboard_survey_responses_fact/part_dt=2020-05-06';