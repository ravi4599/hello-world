************** Database create ***************
CREATE DATABASE vv_metadata;


***************load_dt ***********************

CREATE TABLE vv_metadata.load_dt(
load_id	INT NOT NULL AUTO_INCREMENT,
load_start_dt	TIMESTAMP,
load_end_dt	TIMESTAMP,
load_exec_start_dt	TIMESTAMP,
load_exec_end_dt	TIMESTAMP,
load_status	VARCHAR(20),
load_ind	CHAR(1),
error_details	VARCHAR(100),
load_type	VARCHAR(20),
project_name	VARCHAR(30),
load_frequency	VARCHAR(20),
PRIMARY KEY (load_id)
);

************* job_info ************************

CREATE TABLE vv_metadata.job_info(
job_id INT NOT NULL AUTO_INCREMENT,
job_name VARCHAR(40),
target_table_name VARCHAR(50),
target_table_type VARCHAR(30),
source_table_name VARCHAR(50),
source_table_type VARCHAR(30),
load_frequency VARCHAR(20),
created_on TIMESTAMP,
PRIMARY KEY (job_id)
);

******** Load_Job_Status_Log *********************

CREATE TABLE vv_metadata.load_job_status_log(
load_job_id INT NOT NULL AUTO_INCREMENT,
load_id	INT,
job_id	INT,
job_name VARCHAR(40),
load_job_exec_start_dt	TIMESTAMP,
load_job_exec_end_dt	TIMESTAMP,
src_count	INT,
tgt_count	INT,
load_job_status	VARCHAR(2),
error_details	VARCHAR(1024),
PRIMARY KEY (load_job_id),
FOREIGN KEY (load_id) REFERENCES vv_metadata.load_dt(load_id),
FOREIGN KEY (job_id) REFERENCES vv_metadata.job_info(job_id)
);

************** load_dt insert scripts *********************

insert into vv_metadata.load_dt(load_start_dt, load_end_dt, load_exec_start_dt, load_status, load_ind, load_type, project_name, load_frequency)
values ('1990-01-01 00:00:00', now(), now(), 'In Progress', 'P', 'Incremental', 'VV_NBX_ETL', 'realtime');

insert into vv_metadata.load_dt(load_start_dt, load_end_dt, load_exec_start_dt, load_status, load_ind, load_type, project_name, load_frequency)
values ('1990-01-01 00:00:00', now(), now(), 'In Progress', 'P', 'Incremental', 'VV_NBX_ETL', 'daily');


insert into vv_metadata.load_dt(load_start_dt, load_end_dt, load_exec_start_dt, load_status, load_ind, load_type, project_name, load_frequency)
values ('1990-01-01 00:00:00', now(), now(), 'In Progress', 'P', 'Incremental', 'VV_NBX_ETL', 'infegy-monthly');


*************** job_info insert scripts ********************

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type,load_frequency ,created_on) 
values('jActiveMQKafkaProducer','qa-shore-seaware.reservation','Kafka Topic','VVQAORA.SEAWARE.PnrUpdateTopic','Seaware Event','realtime',now());

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jKafkaSalesforceConsumer','Reservation_Event','Salesforce Table','qa-shore-seaware.reservation','Kafka Topic','realtime',now()); 

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaOppUpdates','qa-shore-salesforce.opportunity','Kafka Topic','Pushtopic_Oppupdates','Salesforce PushTopic','realtime',now());  

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaSailorUpdates','qa-shore-salesforce.sailor','Kafka Topic','PushTopic_SailorUpdates','Salesforce PushTopic','realtime',now());

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaVoyages','qa-shore-salesforce.voyage','Kafka Topic','PushTopic_Voyages','Salesforce PushTopic','realtime',now());  

***************BELOW 2 RECORDS ARE DELETED AS A PART OF 27TH JULY QA DEPLOYMENT***********************
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaAddressUpdates','qa-shore-salesforce.address','Kafka Topic','PushTopic_AddressUpdates','Salesforce PushTopic','realtime',now());  

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaContactDetails','qa-shore-salesforce.contact-details','Kafka Topic','PushTopic_ContactDetails','Salesforce PushTopic','realtime',now());  
******************************************************************************************************

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaSailorPrefs','qa-shore-salesforce.preference','Kafka Topic','PushTopic_SailorPrefs','Salesforce PushTopic','realtime',now());  

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaTPMUpdates','qa-shore-salesforce.travel-party-member','Kafka Topic','PushTopic_TPM','Salesforce PushTopic','realtime',now());  

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jSFStreamingtoKafkaClientMerge','qa-shore-salesforce.client-merge','Kafka Topic','PushTopic_ClientMerge','Salesforce PushTopic','realtime',now());  

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) values ('jSFStreamingtoKafkaCommPref','qa-shore-salesforce.communicationpreference','Kafka Topic','PushTopic_Comm_Prefs','SFDC PushTopic','realtime',now());  

********************************************************************************************************************************************************

/*jTwitterGnipJsonFileCreation*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipJsonFileCreation','TextFile','File','stg_brand_value_tag','Hive','daily',now());
commit;

/*jTwitterGnipCreatePowerTrackJob */
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipCreatePowerTrackJob','GNIP Post','API','TextFile','File','daily',now());
commit;

/*jTwitterGnipBrandSampleOptimization*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipBrandSampleOptimization','gnip_brand_optimized_quotation ','Hive','stg_brandsquotations_withuuiddate','Hive','daily',now());
commit;


/*jTwitterGnipDeleteEpochFolder*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipDeleteEpochFolder',' ','','','','daily',now());
commit;

/*jTwitterGnipDownloadFiles*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipDownloadFiles','S3','S3Landing','GNIP API','API','daily',now());
commit;

/*jTwitterGnipFileCreationWithSample*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipFileCreationWithSample','TextFile','File','gnip_brand_optimized_quotation','Hive','daily',now());
commit;

/*jTwitterGnipGetFinalQuotations*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipGetFinalQuotations','S3','S3 Staging','GNIP API','API','daily',now());
commit;

/*jTwitterGnipFinalQuotationstoS3Accept */
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipFinalQuotationstoS3Accept','GNIP-API','API','Text File','File','daily',now());
commit;

/*jTwitterGnipGetQuotations*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipGetQuotations','S3','S3 Staging','GNIP API','API','daily',now());
commit;


/*jTwitterGnipWriteJobQuotationstoS3*/
insert into job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency ,created_on) values('jTwitterGnipWriteJobQuotationstoS3','S3','S3 Staging','GNIP API','API','daily',now());
commit;

/*jTwitterGnipDataIngest*/

INSERT INTO job_info(job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on)
VALUES
('jTwitterGnipDataIngest','','Hive','','S3 Landing','daily',now());

/*jTwitterGnipMergeMultipleJsonFiles*/

INSERT INTO job_info(job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on)
VALUES
('jTwitterGnipMergeMultipleJsonFiles','','S3 Landing','','S3 Landing','daily',now());

********************************************************************************************************************************************************

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) values 
('jKafkaCRMAcxiomDataRefresh','hbtb_nbx_acxiom','Hbase','qa-shore-nbxapi.demographics-data-request','CRM Topic','realtime',now()); commit;

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) values 
('jPersonCreatedEventtoHbase','hbt_nbx_c360_xref','Hbase','qa-shore-salesforce.sailor','CRM Topic','realtime',now()); commit;

********************************************************************************************************************************************************

*******************************************CRM Jobs Metadata********************************************************************************************

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgAccount','hvtb_nbx_core_crm_account','Hive','Account','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgBookableActivities','hvtb_nbx_core_crm_bookable_activities','Hive','Bookable_Activities__c','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgBookingPayments','hvtb_nbx_core_crm_booking_payments','Hive','Booking_Payments__c','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgCampaign','hvtb_nbx_core_crm_campaign','Hive','Campaign','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgCase','hvtb_nbx_core_crm_case','Hive','Case','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgContact','hvtb_nbx_core_crm_contact','Hive','Contact','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgLead','hvtb_nbx_core_crm_lead','Hive','Lead','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgOpportunity','hvtb_nbx_core_crm_opportunity ','Hive','Opportunity','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgSelectedCategory','hvtb_nbx_core_crm_selected_category ','Hive','SelectedCategory__c','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgTravelPartyMember','hvtb_nbx_core_crm_travel_party_member ','Hive','Travel_Party_Member__c','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgTravelWith','hvtb_nbx_core_crm_travel_with','Hive','TravelWith__c','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgUser','hvtb_nbx_core_crm_user','Hive','User','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgVoyage','hvtb_nbx_core_crm_voyage ','Hive','Voyage__c','Salesforce Object','daily',now()); 
commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values ('jSFStgVoyageMessage','hvtb_nbx_core_crm_voyage_message','Hive','VoyageMessage__c','Salesforce Object','daily',now()); 
commit;
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on)
values ('jSFStgDeployChanges','','Hive','','Hive','realtime',now()); 
commit;  

*********************************************************************************************************************************************

********************************************New Reservation Metadata*************************************************************************
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values 
('jNewReservationTribeSubTribe','Acxiom API Call','Acxiom API','HBTB_SEAWARE_RESERVATION','Hbase Table','realtime',now()); 


insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values 
('jAcxiomwrapperNewUser','HBTB_NBX_ACXIOM_RESERVATION','HBase Table','Acxiom API Call','Acxiom API','realtime',now()); 

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) 
values 
('jTribeSubtribeClassifierSingleUser','HBTB_NBX_SAILOR_TRIBES','HBase Table','HVTB_NBX_CORE_SAILORATTRIBUTES_ACXIOM','Hive Table','realtime',now()); 


commit;

*********************************************************************************************************************************************
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type,load_frequency ,created_on) 
values('jKafkaProducerCircleReservation','qa-shore-seaware.circle-reservation','Kafka Topic','SWQA.SEAWARE.Subscriptions ','Seaware Event','realtime',now());

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jKafkaConsumerCircleReservation','Circle_Reservation_Event__e','Salesforce Table','qa-shore-seaware.circle-reservation','Kafka Topic','realtime',now()); 


insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type,load_frequency ,created_on) 
values('jKafkaProducerClientMerge','qa-shore-seaware.client-merge','Kafka Topic','SWQA.SEAWARE.Subscriptions','Seaware Event','realtime',now());

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jKafkaConsumerClientMerge','Clientmerge_Event__e','Salesforce Table','qa-shore-seaware.client-merge','Kafka Topic','realtime',now()); 

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type,load_frequency ,created_on) 
values('jKafkaProducerTableChange','qa-shore-seaware.table-change','Kafka Topic','VVSEAQAORA.SEAWARE.TableChange','Seaware Event','realtime',now());

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jKafkaConsumerTableChange','MasterTableSync_Event__e','Salesforce Table','qa-shore-seaware.table-change','Kafka Topic','realtime',now()); 


insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name, source_table_type, load_frequency ,created_on) 
values('jKafkaConsumerKnowledgeArticles','Knowledge_Articles__e','Salesforce Table','qa-shore-cms.content','Kafka Topic','realtime',now());

***********************************************************************************************************************************************

***Infegy Data Ingestion****************************************************************************************************

insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) values ('jInfegyApiCall','S3','S3 Landing','vv_db.hvtb_nbx_core_infegy_brand_boolean_queries','Hive','infegy-monthly',now()); commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) values ('jInfegyDataParserInterests','vv_db.hvtb_nbx_core_infegy_interests','Hive','S3','S3 Landing','infegy-monthly',now()); commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) values ('jInfegyDataParserPostInterests','vv_db.hvtb_nbx_core_infegy_postinterests','Hive','S3','S3 Landing','infegy-monthly',now()); commit; 
insert into vv_metadata.job_info (job_name,target_table_name,target_table_type,source_table_name,source_table_type,load_frequency,created_on) values ('jInfegyDeleteFolder','','','','','infegy-monthly',now()); commit; 


***************************************************************************








