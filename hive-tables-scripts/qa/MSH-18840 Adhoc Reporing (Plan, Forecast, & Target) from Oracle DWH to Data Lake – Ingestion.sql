create external table vv_db.hvtb_nbx_landing_spf_revenue_target_header (
	record_id int ,
	ship_code string ,
	package_type string ,
	sail_date timestamp ,
	sail_days int ,
	cabin_ctg_generic string ,
	forecast_version string ,
	forecast_metric string ,
	timestamp_created timestamp ,
	timestamp_changed timestamp ,
	user_created string ,
	user_changed string ,
	comments string ,
	load_data_timestamp timestamp	
) 
row format delimited
fields terminated by '\001'
lines terminated by '\n'
stored as textfile
location "s3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_spf_revenue_target_header"
;
create external table vv_db.hvtb_nbx_landing_spf_revenue_target_value (
	record_id int ,
	header_id int ,
	value_index int ,
	value double,
	timestamp_created timestamp ,
	timestamp_changed timestamp ,
	user_created string ,
	user_changed string ,
	load_data_timestamp timestamp	
) 
row format delimited
fields terminated by '\001'
lines terminated by '\n'
stored as textfile
location "s3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_spf_revenue_target_value"
;
create external table vv_db.hvtb_nbx_landing_spf_revenue_forecast_version(
	record_id int ,
	forecast_version string ,
	is_plan string ,
	comments string,
	load_data_timestamp timestamp	
) 
row format delimited
fields terminated by '\001'
lines terminated by '\n'
stored as textfile
location "s3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_spf_revenue_forecast_version"
;
