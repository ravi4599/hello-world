CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_itinerary(
  itin_record_id int, 
  package_type varchar(15), 
  day_num_from int, 
  time_from timestamp, 
  day_num_to int, 
  time_to timestamp, 
  location_type_from varchar(15), 
  location_code_from varchar(15), 
  location_type_to varchar(15), 
  location_code_to varchar(15), 
  component_type varchar(15), 
  component_code varchar(15), 
  component_subcode1 varchar(15), 
  component_subcode2 varchar(15), 
  component_subcode3 varchar(15),
  can_be_excluded varchar(1),
  is_optional varchar(1),
  package_links_inclusive varchar(1),
  comments varchar(255),
  description varchar(2000),
  seq_num int,
  city_from varchar(5),
  city_to varchar(5),
  option_group int,
  load_data_timestamp timestamp 
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n', 
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_package_itinerary';

CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type_vendor_link(
  record_id int, 
  package_type varchar(15), 
  vendor_id int, 
  date_from timestamp, 
  date_to timestamp,
  comments varchar(255),
  load_data_timestamp timestamp )
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n',
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_package_type_vendor_link';


CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_landing_sw_rpl_vendor(
  vendor_id int, 
  vendor_code varchar(15), 
  vendor_name_typed varchar(100), 
  vendor_name varchar(100), 
  contact varchar(255),
  comments varchar(255),
  is_active varchar(1),
  email varchar(80),
  load_data_timestamp timestamp )
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n',
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_vendor';


CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type(
  package_type_id int, 
  package_type varchar(15), 
  land_days int, 
  sail_days int, 
  comments varchar(255), 
  product_type varchar(15), 
  tc_package_type varchar(15), 
  land_days_post int, 
  is_land_only varchar(1), 
  is_multi_sail varchar(1), 
  is_shorex varchar(1), 
  is_secondary varchar(1), 
  is_active varchar(1), 
  shorex_timing varchar(15), 
  extra_seat_question varchar(255), 
  pre_post_mode varchar(15), 
  package_class varchar(15), 
  sail_segments varchar(255), 
  capacity int, 
  allow_segments varchar(1), 
  package_type_name varchar(100), 
  load_data_timestamp timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n', 
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-qa-emr-cluster/data/landing/spf/hvtb_nbx_landing_seaware_rpl_package_type'
;

CREATE OR REPLACE VIEW seaware.hvtb_nbx_landing_sw_rpl_package_itinerary_vw      
AS SELECT * FROM hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_itinerary with no schema binding;

CREATE OR REPLACE VIEW seaware.hvtb_nbx_landing_sw_rpl_package_type_vendor_link_vw      
AS SELECT * FROM hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type_vendor_link with no schema binding;

CREATE OR REPLACE VIEW seaware.hvtb_nbx_landing_sw_rpl_vendor_vw      
AS SELECT * FROM hive_schema_stg.hvtb_nbx_landing_sw_rpl_vendor with no schema binding;

CREATE OR REPLACE VIEW seaware.hvtb_nbx_landing_sw_rpl_package_type_vw      
AS SELECT * FROM hive_schema_stg.hvtb_nbx_landing_sw_rpl_package_type with no schema binding;


GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;

