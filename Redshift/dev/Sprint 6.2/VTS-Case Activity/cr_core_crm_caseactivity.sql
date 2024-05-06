CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_core_crm_caseactivity(
  id varchar(100), 
  case_activity_queue__c varchar(100), 
  case_activity_status__c varchar(100), 
  case_closed__c boolean, 
  case_close_datetime__c timestamp, 
  case_opened_datetime__c timestamp, 
  case__c varchar(100), 
  change_type__c varchar(100), 
  createdbyid varchar(100), 
  createddate timestamp, 
  currencyisocode varchar(100), 
  durationformula__c float, 
  duration__c float, 
  isdeleted boolean, 
  is_active__c boolean, 
  is_current_value__c boolean, 
  lastmodifiedbyid varchar(100), 
  lastmodifieddate timestamp, 
  name varchar(240), 
  systemmodstamp timestamp, 
  value_changed_from__c varchar(100), 
  value_changed_to__c varchar(100), 
  value_duration__c float, 
  value_in_time__c timestamp, 
  value_out_time__c timestamp)
STORED AS PARQUET  
LOCATION
  's3://vv-dev-emr-cluster/data/core/crm/hvtb_nbx_core_crm_caseactivity'
;