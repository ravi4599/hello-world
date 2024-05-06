CREATE EXTERNAL TABLE `vv_db.hvtb_nbx_landing_crm_livechattranscript_cdc`(
  `load_data_timestamp` timestamp, 
  `id` string, 
  `caseid` string, 
  `leadid` string, 
  `name` string, 
  `createddate` timestamp, 
  `starttime` timestamp, 
  `endtime` timestamp, 
  `waittime` int, 
  `abandoned` int, 
  `abandoned__c` string, 
  `status` string, 
  `chat_transferred__c` string, 
  `of_chat_transfers_request_cancelled__c` string, 
  `chat_accepted__c` string, 
  `chatduration` int, 
  `ownerid` string, 
  `contactid` string, 
  `lastmodifieddate` timestamp, 
  `systemmodstamp` timestamp, 
  `answered_in_10__c` int, 
  `averageresponsetimeoperator` int, 
  `averageresponsetimevisitor` int, 
  `booking_flow__c` string, 
  `chats_cancelled__c` int, 
  `requesttime` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='', 
  'line.delim'='\n', 
  'serialization.format'='', 
  'timestamp.formats'='yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://vv-prod-emr-cluster/data/landing/crm/hvtb_nbx_landing_crm_livechattranscript_cdc'
TBLPROPERTIES (
  'transient_lastDdlTime'='1603980077');
