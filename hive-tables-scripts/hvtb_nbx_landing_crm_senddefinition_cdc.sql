CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_landing_crm_senddefinition_cdc`(
`load_data_timestamp` string,
`id`	string,
`et4ae5__datesent__c` timestamp,
`et4ae5__allsources__c`	string,
`et4ae5__emailid__c`	string,
`et4ae5__emailname__c`	string,
`et4ae5__fromemail__c`	string,
`et4ae5__numbernotclicked__c`	double,
`et4ae5__numberofexistingundeliverables__c`	double,
`et4ae5__numberofexistingunsubscribes__c`	double,
`et4ae5__numberofhardbounces__c`	double,
`et4ae5__numberofsoftbounces__c`	double,
`et4ae5__numberofsubscribersforwardingemail__c`	double,
`et4ae5__numberoftotalclicks__c`	double,
`et4ae5__numberoftotalopens__c`	double,
`et4ae5__numberofuniqueclicks__c`	double,
`et4ae5__numberofuniqueopens__c`	double,
`et4ae5__numbersent__c`	double,
`et4ae5__numberunsubscribed__c`	double,
`et4ae5__number_bounced__c`	double,
`et4ae5__number_delivered__c`	double,
`et4ae5__number_not_opened__c`	double,
`et4ae5__open_rate__c`	double,
`et4ae5__click_through_rate__c`	double,
`et4ae5__unsubscribe_rate__c`	double,
`et4ae5__subject__c`	string,
`isdeleted`	boolean)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='', 
  'line.delim'='\n', 
  'serialization.format'='', 
  'timestamp.formats'='yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-dev-emr-cluster/data/landing/crm/hvtb_nbx_landing_crm_senddefinition_cdc'
;