CREATE EXTERNAL TABLE hive_schema_stg.hvtb_nbx_core_crm_senddefinition(
load_data_timestamp varchar(100),
id	varchar(100),
et4ae5__datesent__c timestamp,
et4ae5__allsources__c	varchar(100),
et4ae5__emailid__c	varchar(100),
et4ae5__emailname__c	varchar(100),
et4ae5__fromemail__c	varchar(100),
et4ae5__numbernotclicked__c	float,
et4ae5__numberofexistingundeliverables__c	float,
et4ae5__numberofexistingunsubscribes__c	float,
et4ae5__numberofhardbounces__c	float,
et4ae5__numberofsoftbounces__c	float,
et4ae5__numberofsubscribersforwardingemail__c	float,
et4ae5__numberoftotalclicks__c	float,
et4ae5__numberoftotalopens__c	float,
et4ae5__numberofuniqueclicks__c	float,
et4ae5__numberofuniqueopens__c	float,
et4ae5__numbersent__c	float,
et4ae5__numberunsubscribed__c	float,
et4ae5__number_bounced__c	float,
et4ae5__number_delivered__c	float,
et4ae5__number_not_opened__c	float,
et4ae5__open_rate__c	float,
et4ae5__click_through_rate__c	float,
et4ae5__unsubscribe_rate__c	float,
et4ae5__subject__c	varchar(500),
isdeleted	boolean
  )
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://vv-dev-emr-cluster/data/core/crm/hvtb_nbx_core_crm_senddefinition'
;