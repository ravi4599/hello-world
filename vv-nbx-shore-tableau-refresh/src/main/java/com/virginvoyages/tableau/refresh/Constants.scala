package com.virginvoyages.tableau.refresh

import java.sql.Timestamp

class StreamConstants {
 // var static appName = "PersonCreatorJson"
   case class SAILOR(C360ID: String,MASTER_ID: String,CRM_ID: String,SEAWARE_ID: String,VXP_ID: String,TIMESTAMP: String,C360REF_ID:String)
   case class VoyageOper(ROWKEY:String, VOYAGEID:String,SRCCONFIGID: String,TGTCONFIGID:String,TYPE:String,STARTTIME: Timestamp,
       ENDTIME:Timestamp,STATUS:String,LASTLOADDATETIME:String)
       
    def APP_NAME = "PersonCreatorJson"
    def PERSON_TYPE_CREATED:String  = "updated"
    def SELECT_SALESFORCE_ID:String = "SalesforceID"
    def ACCESS_TOKEN:String  = "access_token"
    def ACCESS_TOKEN_ERR:String  = "error"
    def REFERENCE_TYPE = "referenceType"
    def REFERENCE_TYPE_ID:String = "referenceTypeID"
    def MASTER_ID:String = "masterID"
    def EMBEDED:String = "_embedded"
    def REFERENCES = "references"
    def NARIVESOURCEIDVALUE = "nativeSourceIDValue"
    def C360:String ="C-360"
    def CLIENT_CREDENTIALS = "client_credentials"
     
    def RESV_APP_NAME = "kafkaConsumer"
    def SPARK_DRIVER_RESV ="spark.driver.allowMultipleContexts"
    def TRUE = "true"
    def LOALTYMEMKEYWORD = "LoyaltyMembershipID"
    
    def CONTENTAPP_NAME="kafkaConsumer"
    def CONTENT_DRIVER="spark.driver.allowMultipleContexts"
    
    def CALAPP_NAME="kafkaConsumer"
    def CAL_DRIVER="spark.driver.allowMultipleContexts"
    
   case class FEEDBACK(SAILORID: String, ACTIVITYID: String, REQUESTSOURCE: String, SAILORSELECTION:String,NBXUNIQUEKEY:String,TIMESTAMP: String, ROWKEY: String)
  
}