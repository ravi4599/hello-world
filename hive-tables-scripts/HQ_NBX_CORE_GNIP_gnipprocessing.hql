CREATE EXTERNAL TABLE IF NOT EXISTS vv_db.HVTB_NBX_CORE_GNIP_gnipprocessing(
 id string,
 brand_name array<string>,
 SubTribe array<string>,
 tweet_count array<int>,
 prepared_topic_display_name array<string>,
 synonyms_list array<string>,
 final_subtribe string,
 topic_match_list array<string>)
STORED AS PARQUET 
LOCATION "s3a://vv-qa-emr-cluster/data/core/tribe_subtribe/gnip_processing"