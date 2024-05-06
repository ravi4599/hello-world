DROP TABLE IF EXISTS hive_schema_stg.postvoyage_survey_responses_fact;

CREATE EXTERNAL TABLE hive_schema_stg.postvoyage_survey_responses_fact(
request_id varchar(100),
client_id bigint,
ship_id bigint,
sail_id bigint,
guest_id bigint,
request_time timestamp,
response_time timestamp,
itinerary_skey bigint,
activity_skey bigint,
question_id bigint,
answer_rating decimal(6,3),
answer_text varchar(1000),
load_dt timestamp,
upd_dt timestamp)
PARTITIONED BY ( 
part_dt date)
STORED AS PARQUET
LOCATION 's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_postvoyage_survey_responses_fact';

ALTER TABLE hive_schema_stg.postvoyage_survey_responses_fact ADD IF NOT EXISTS  PARTITION (part_dt = '2020-04-22') location 's3://vv-prod-emr-cluster/data/core/seaware/hvtb_nbx_core_postvoyage_survey_responses_fact/part_dt=2020-04-22';

