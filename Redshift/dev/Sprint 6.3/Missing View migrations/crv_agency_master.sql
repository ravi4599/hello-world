CREATE OR REPLACE VIEW CRM.CRM_AGENCY_MASTER
AS 
SELECT 
   agency_id                ,
   rec_start_dttm           ,
   rec_end_dttm              ,
   id                       ,
   seaware_agency_id__c    ,
   name                     ,
   lastname                 ,
   firstname                ,
   billingcity              ,
   billingcountry           ,
   billingpostalcode        ,
   billingstate             ,
   billingstreet            ,
   createddate 		    ,	
   status__c ,
   alternate_dba_name__c ,
   iata_number__c ,
  clia_number__c ,
  asta_number__c ,
  abta_number__c ,
  territorynametext__c ,
  ownerid ,
  primary_affiliation__c ,
  parentid ,
  agency_classification__c ,
  agency_sub_type__c ,
  is_touroperator__c ,
  client_id__c ,
  personemail ,
  tgender__c ,
  personmailingcity ,
  personmailingstate ,
  personmailingpostalcode ,
  personmailingcountry ,
  createdbyid ,
  lastmodifiedbyid ,
  personhasoptedoutofemail ,
  hand_raiser__c ,
  vip_tier__c,
  Webuser__c,
  currency_type__c 	
FROM hive_schema_stg.crm_agency_master
UNION
SELECT 
-1,
'1900-01-01 00:00:00',
'1900-01-01 00:00:00'
,'NA'
,'NA'
,'NA'
,'NA'
,'NA'
,'NA',
'NA'
,'NA'
,'NA',
'NA',
'1900-01-01 00:00:00',
'NA',
'NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA',FALSE,'NA','NA','NA','NA','NA','NA','NA','NA','NA',FALSE,FALSE,'NA',FALSE,'NA' 
with no schema binding;


GRANT SELECT ON crm.crm_agency_master TO group tableau_poweruser_group;
GRANT SELECT ON crm.crm_agency_master TO group integration_user_group;


