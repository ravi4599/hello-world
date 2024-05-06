drop table is exists hive_schema_stg.crm_agency_master;
CREATE EXTERNAL TABLE hive_schema_stg.crm_agency_master(
  agency_id int, 
  rec_start_dttm timestamp, 
  rec_end_dttm timestamp, 
  id varchar(100),
  seaware_agency_id__c varchar(100),
  name varchar(100),
  lastname varchar(100),
  firstname varchar(100),
  billingcity varchar(100),
  billingcountry varchar(100),
  billingpostalcode varchar(100),
  billingstate varchar(100),
  billingstreet varchar(100),
  createddate timestamp, 
  status__c varchar(50),
  alternate_dba_name__c varchar(100),
  iata_number__c varchar(100),
  clia_number__c varchar(100),
  asta_number__c varchar(100),
  abta_number__c varchar(100),
  territorynametext__c varchar(100),
  ownerid varchar(100),
  primary_affiliation__c varchar(100),
  parentid varchar(100),
  agency_classification__c varchar(100),
  agency_sub_type__c varchar(100),
  is_touroperator__c boolean,
  client_id__c varchar(100),
  personemail varchar(100),
  tgender__c varchar(100),
  personmailingcity varchar(100),
  personmailingstate varchar(100),
  personmailingpostalcode varchar(100),
  personmailingcountry varchar(100),
  createdbyid varchar(100),
  lastmodifiedbyid varchar(100),
  personhasoptedoutofemail boolean,
  hand_raiser__c boolean,
  vip_tier__c varchar(100),
  Webuser__c boolean,
  currency_type__c varchar(255),
  md5_dim varchar(100),
  etl_ld_status varchar(100),
  etl_ld_dt timestamp, 
  etl_upd_dt timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://vv-staging-emr-cluster/data/core/crm_kpi/hvtb_nbx_core_crm_agency_master_dim'
;
