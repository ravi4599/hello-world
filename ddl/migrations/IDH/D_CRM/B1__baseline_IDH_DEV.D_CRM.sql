create TABLE IF NOT EXISTS FLYWAY_SCHEMA_HISTORY (
	INSTALLED_RANK NUMBER(38,0) NOT NULL,
	VERSION VARCHAR(50),
	DESCRIPTION VARCHAR(200),
	TYPE VARCHAR(20) NOT NULL,
	SCRIPT VARCHAR(1000) NOT NULL,
	CHECKSUM NUMBER(38,0),
	INSTALLED_BY VARCHAR(100) NOT NULL,
	INSTALLED_ON TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	EXECUTION_TIME NUMBER(38,0) NOT NULL,
	SUCCESS BOOLEAN NOT NULL,
	primary key (INSTALLED_RANK)
);
create view IF NOT EXISTS CAMPAIGN_HOLDOUT_LOG(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber. This serves as the primary key.',
	PROFILE_ID COMMENT 'The unique ID associated with a profile',
	EXTERNAL_ID COMMENT '''''',
	CUSTOMER_ID COMMENT '''''',
	OFFER_ID COMMENT 'All values currently null',
	EVENT_DATE COMMENT 'The date the affected subscriber was withheld from a promotion sent out',
	OFFER_CODE COMMENT 'Description of the offer code sent',
	EMAIL_NAME COMMENT 'Description of the email sent out (NOTE: I do not think the column should be called email_address)',
	CREATIVE_VARIANT COMMENT 'Identifies which content blocks and images were used in an email',
	DATA_SOURCE_NAME COMMENT 'Datasource name specifies source of the emails.  For example, Epsilon or Harmony.',
	OFFER_NAME COMMENT 'All values currently null',
	PARENT_OFFER_ID COMMENT 'All values currently null',
	JOURNEY_NAME COMMENT 'All values currently null',
	JOURNEY_STEP COMMENT 'All values currently null',
	STRENGTH_OF_CUSTOMER COMMENT 'Not a current column????',
	USER_DEFINED_SEGMENT_1 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_2 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_3 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_4 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_5 COMMENT 'All values currently null',
	OFFER_DECISION_LOGIC COMMENT '''''',
	POINT_BALANCE COMMENT 'Numer of points earned associated with that account',
	EMAIL_ADDRESS COMMENT 'EMAIL_ADDRESS. .',
	CAMPAIGN_ID COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_NAME COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_TYPE COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_CATEGORY COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESC COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_HOLDOUT_LOG_VAPP is a list of who has not resoponded to the marketing emails.'
 as 
SELECT CRM_HOLDOUT_LOG_BV.BRAND_ID,CRM_HOLDOUT_LOG_BV.SOURCE_SYSTEM_NM,CRM_HOLDOUT_LOG_BV.ACCOUNT_ID,CRM_HOLDOUT_LOG_BV.JOB_ID,CRM_HOLDOUT_LOG_BV.SUBSCRIBER_KEY,CRM_HOLDOUT_LOG_BV.PROFILE_ID,CRM_HOLDOUT_LOG_BV.EXTERNAL_ID,CRM_HOLDOUT_LOG_BV.CUSTOMER_ID,CRM_HOLDOUT_LOG_BV.OFFER_ID,CRM_HOLDOUT_LOG_BV.EVENT_DATE,CRM_HOLDOUT_LOG_BV.OFFER_CODE,CRM_HOLDOUT_LOG_BV.EMAIL_NAME,CRM_HOLDOUT_LOG_BV.CREATIVE_VARIANT,CRM_HOLDOUT_LOG_BV.DATASOURCE_NAME,CRM_HOLDOUT_LOG_BV.OFFER_NAME,CRM_HOLDOUT_LOG_BV.PARENT_OFFER_ID,CRM_HOLDOUT_LOG_BV.JOURNEY_NAME,CRM_HOLDOUT_LOG_BV.JOURNEY_STEP,CRM_HOLDOUT_LOG_BV.STRENGTH_OF_CUSTOMER,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_1,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_2,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_3,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_4,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_5,CRM_HOLDOUT_LOG_BV.OFFER_DECISION_LOGIC,CRM_HOLDOUT_LOG_BV.POINT_BALANCE,CRM_HOLDOUT_LOG_BV.EMAIL_ADDRESS,CRM_HOLDOUT_LOG_BV.CAMPAIGN_ID,CRM_HOLDOUT_LOG_BV.CAMPAIGN_NAME,CRM_HOLDOUT_LOG_BV.CAMPAIGN_TYPE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_CATEGORY,CRM_HOLDOUT_LOG_BV.CAMPAIGN_DESCRIPTION,CRM_HOLDOUT_LOG_BV.CAMPAIGN_OBJECTIVE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_START_DATE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_END_DATE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_DURATION,CRM_HOLDOUT_LOG_BV.LOAD_ID,CRM_HOLDOUT_LOG_BV.LOAD_DTTM,CRM_HOLDOUT_LOG_BV.UPDATE_ID,CRM_HOLDOUT_LOG_BV.UPDATE_DTTM,CRM_HOLDOUT_LOG_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_HOLDOUT_LOG_BV ;
create view IF NOT EXISTS CAMPAIGN_SEND_LOG(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber.',
	PROFILE_ID COMMENT 'The unique ID associated with a profile',
	EXTERNAL_ID COMMENT '''''',
	CUSTOMER_ID COMMENT '''''',
	OFFER_ID COMMENT 'All values currently null',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	CREATIVE_VARIANT COMMENT 'Identifies which content blocks and images were used in an email',
	EVENT_DATE COMMENT 'The date the offer is sent',
	OFFER_CODE COMMENT 'Offer code sent out in the job',
	SUB_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	TRIGGERED_SEND_ID COMMENT 'Automatically created by Salesforce when sends go out',
	ERROR_CODE COMMENT 'Code for error, if any',
	DATA_SOURCE_NAME COMMENT 'Which data source the email recipients are coming from.  For example, Epsilon or Harmony.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	OFFER_NAME COMMENT 'All values currently null',
	PARENT_OFFER_ID COMMENT 'All values currently null',
	JOURNEY_NAME COMMENT 'All values currently null',
	JOURNEY_STEP COMMENT 'All values currently null',
	STRENGTH_OF_CUSTOMER COMMENT 'The strength of customer segment based on buying habits.',
	USER_DEFINED_SEGMENT_1 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_2 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_3 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_4 COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_5 COMMENT 'All values currently null',
	OFFER_DECISION_LOGIC COMMENT 'OFFER_DECISION_LOGIC',
	POINT_BALANCE COMMENT '''''',
	EMAIL_NAME COMMENT 'Description of the email sent out (NOTE: I do not think the column should be called email_address)',
	CAMPAIGN_ID COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_NAME COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_TYPE COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_CATEGORY COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESC COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_SEND_LOG_VAPP is record of every email sent, the offer it contained, and what customer it was sent to.'
 as 
SELECT CRM_SEND_LOG_BV.BRAND_ID,CRM_SEND_LOG_BV.SOURCE_SYSTEM_NM,CRM_SEND_LOG_BV.ACCOUNT_ID,CRM_SEND_LOG_BV.JOB_ID,CRM_SEND_LOG_BV.SUBSCRIBER_KEY,CRM_SEND_LOG_BV.PROFILE_ID,CRM_SEND_LOG_BV.EXTERNAL_ID,CRM_SEND_LOG_BV.CUSTOMER_ID,CRM_SEND_LOG_BV.OFFER_ID,CRM_SEND_LOG_BV.BATCH_ID,CRM_SEND_LOG_BV.LIST_ID,CRM_SEND_LOG_BV.CREATIVE_VARIANT,CRM_SEND_LOG_BV.EVENT_DATE,CRM_SEND_LOG_BV.OFFER_CODE,CRM_SEND_LOG_BV.SUB_ID,CRM_SEND_LOG_BV.TRIGGERED_SEND_ID,CRM_SEND_LOG_BV.ERROR_CODE,CRM_SEND_LOG_BV.DATASOURCE_NAME,CRM_SEND_LOG_BV.EMAIL_ADDRESS,CRM_SEND_LOG_BV.OFFER_NAME,CRM_SEND_LOG_BV.PARENT_OFFER_ID,CRM_SEND_LOG_BV.JOURNEY_NAME,CRM_SEND_LOG_BV.JOURNEY_STEP,CRM_SEND_LOG_BV.STRENGTH_OF_CUSTOMER,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_1,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_2,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_3,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_4,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_5,CRM_SEND_LOG_BV.OFFER_DECISION_LOGIC,CRM_SEND_LOG_BV.POINT_BALANCE,CRM_SEND_LOG_BV.EMAIL_NAME,CRM_SEND_LOG_BV.CAMPAIGN_ID,CRM_SEND_LOG_BV.CAMPAIGN_NAME,CRM_SEND_LOG_BV.CAMPAIGN_TYPE,CRM_SEND_LOG_BV.CAMPAIGN_CATEGORY,CRM_SEND_LOG_BV.CAMPAIGN_DESCRIPTION,CRM_SEND_LOG_BV.CAMPAIGN_OBJECTIVE,CRM_SEND_LOG_BV.CAMPAIGN_START_DATE,CRM_SEND_LOG_BV.CAMPAIGN_END_DATE,CRM_SEND_LOG_BV.CAMPAIGN_DURATION,CRM_SEND_LOG_BV.LOAD_ID,CRM_SEND_LOG_BV.LOAD_DTTM,CRM_SEND_LOG_BV.UPDATE_ID,CRM_SEND_LOG_BV.UPDATE_DTTM,CRM_SEND_LOG_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_SEND_LOG_BV ;
create view IF NOT EXISTS EMAIL_BOUNCE(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber. This serves as the primary key.',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANS_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'Unique ID of each subscriber',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE COMMENT 'The data the bounce took place',
	EVENT_TYPE COMMENT 'Bounced emails are the only type in this table',
	BOUNCE_CATEGORY COMMENT 'Category of why the email bounced',
	SMTP_CODE COMMENT 'The error code for the bounce from the mail system',
	BOUNCE_REASON COMMENT 'Reason why the email bounced relayed from the mail system',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'Why the email was sent',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_BOUNCE_VAPP contains the marketing emails that did not make it to the customer and the reason why.'
 as 
SELECT CRM_BOUNCE_BV.BRAND_ID,CRM_BOUNCE_BV.SOURCE_SYSTEM_NM,CRM_BOUNCE_BV.ACCOUNT_ID,CRM_BOUNCE_BV.JOB_ID,CRM_BOUNCE_BV.SUBSCRIBER_KEY,CRM_BOUNCE_BV.MESSAGE_ID,CRM_BOUNCE_BV.SERVICE_TRANSACTION_ID,CRM_BOUNCE_BV.EMAIL_ADDRESS,CRM_BOUNCE_BV.SUBSCRIBER_ID,CRM_BOUNCE_BV.LIST_ID,CRM_BOUNCE_BV.EVENT_DATE,CRM_BOUNCE_BV.EVENT_TYPE,CRM_BOUNCE_BV.BOUNCE_CATEGORY,CRM_BOUNCE_BV.SMTP_CODE,CRM_BOUNCE_BV.BOUNCE_REASON,CRM_BOUNCE_BV.BATCH_ID,CRM_BOUNCE_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_BOUNCE_BV.LOAD_ID,CRM_BOUNCE_BV.LOAD_DTTM,CRM_BOUNCE_BV.UPDATE_ID,CRM_BOUNCE_BV.UPDATE_DTTM,CRM_BOUNCE_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_BOUNCE_BV ;
create view IF NOT EXISTS EMAIL_CLICK(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANS_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE COMMENT 'The date the click took place',
	EVENT_TYPE COMMENT 'The event = click for this table',
	SEND_URL_ID COMMENT 'A unique ID for a sent URL',
	URL_ID COMMENT 'A unique ID for a URL',
	URL_TXT COMMENT 'The URL for the link clicked. No AMPscript or variables are populated in this column, for example, www.example.com?%attribute%',
	ALIAS_TXT COMMENT 'Brief description of the URL sent via email',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'An ID used by external partners use to identify the data source',
	IS_UNIQUE_IND COMMENT 'Whether the event is unique or repeated. NOTE: The IsUnique value is TRUE when any link is first clicked in a JobID by a subscriber. Any clicks afterwards are FALSE even if different URLs',
	IS_UNIQUE_FOR_URL_IND COMMENT 'Whether the event is unique or repeated. NOTE: The IsUniqueForURL value is TRUE when any link is first clicked in a JobID by a subscriber. Unlike IsUnique, it is not FALSE for different URLs',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_CLICK_VAPP specifies the links in an email that were clicked and by whom.'
 as 
SELECT CRM_CLICK_BV.BRAND_ID,CRM_CLICK_BV.SOURCE_SYSTEM_NM,CRM_CLICK_BV.ACCOUNT_ID,CRM_CLICK_BV.JOB_ID,CRM_CLICK_BV.SUBSCRIBER_KEY,CRM_CLICK_BV.MESSAGE_ID,CRM_CLICK_BV.SERVICE_TRANSACTION_ID,CRM_CLICK_BV.EMAIL_ADDRESS,CRM_CLICK_BV.SUBSCRIBER_ID,CRM_CLICK_BV.LIST_ID,CRM_CLICK_BV.EVENT_DATE,CRM_CLICK_BV.EVENT_TYPE,CRM_CLICK_BV.SEND_URL_ID,CRM_CLICK_BV.URL_ID,CRM_CLICK_BV.URL,CRM_CLICK_BV.ALIAS,CRM_CLICK_BV.BATCH_ID,CRM_CLICK_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_CLICK_BV.IS_UNIQUE,CRM_CLICK_BV.IS_UNIQUE_FOR_URL,CRM_CLICK_BV.LOAD_ID,CRM_CLICK_BV.LOAD_DTTM,CRM_CLICK_BV.UPDATE_ID,CRM_CLICK_BV.UPDATE_DTTM,CRM_CLICK_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_CLICK_BV ;
create view IF NOT EXISTS EMAIL_COMPLAINT(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANS_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE COMMENT 'The date the complaint took place.',
	EVENT_TYPE COMMENT 'Short description for type of complaint',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'An ID used by external partners use to identify the data source',
	DOMAIN COMMENT 'The domain at which the complaint occurred',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_COMPLAINT_BV contains a list of users who submitted a complaint.'
 as 
SELECT CRM_COMPLAINT_BV.BRAND_ID,CRM_COMPLAINT_BV.SOURCE_SYSTEM_NM,CRM_COMPLAINT_BV.ACCOUNT_ID,CRM_COMPLAINT_BV.JOB_ID,CRM_COMPLAINT_BV.SUBSCRIBER_KEY,CRM_COMPLAINT_BV.MESSAGE_ID,CRM_COMPLAINT_BV.SERVICE_TRANSACTION_ID,CRM_COMPLAINT_BV.EMAIL_ADDRESS,CRM_COMPLAINT_BV.SUBSCRIBER_ID,CRM_COMPLAINT_BV.LIST_ID,CRM_COMPLAINT_BV.EVENT_DATE,CRM_COMPLAINT_BV.EVENT_TYPE,CRM_COMPLAINT_BV.BATCH_ID,CRM_COMPLAINT_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_COMPLAINT_BV.DOMAIN,CRM_COMPLAINT_BV.LOAD_ID,CRM_COMPLAINT_BV.LOAD_DTTM,CRM_COMPLAINT_BV.UPDATE_ID,CRM_COMPLAINT_BV.UPDATE_DTTM,CRM_COMPLAINT_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_COMPLAINT_BV ;
create view IF NOT EXISTS EMAIL_NOT_SENT(
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	ACCOUNT_ID,
	JOB_ID,
	SUBSCRIBER_KEY,
	EMAIL_ADDRESS,
	SUBSCRIBER_ID,
	LIST_ID,
	EVENT_DATE,
	EVENT_TYPE,
	BATCH_ID,
	TRIGGERED_SEND_EXTERNAL_KEY,
	REASON,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	LOAD_FILE_NAME
) as 
SELECT CRM_NOT_SENT_BV.BRAND_ID,CRM_NOT_SENT_BV.SOURCE_SYSTEM_NM,CRM_NOT_SENT_BV.ACCOUNT_ID,CRM_NOT_SENT_BV.JOB_ID,CRM_NOT_SENT_BV.SUBSCRIBER_KEY,CRM_NOT_SENT_BV.EMAIL_ADDRESS,CRM_NOT_SENT_BV.SUBSCRIBER_ID,CRM_NOT_SENT_BV.LIST_ID,CRM_NOT_SENT_BV.EVENT_DATE,CRM_NOT_SENT_BV.EVENT_TYPE,CRM_NOT_SENT_BV.BATCH_ID,CRM_NOT_SENT_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_NOT_SENT_BV.REASON,CRM_NOT_SENT_BV.LOAD_ID,CRM_NOT_SENT_BV.LOAD_DTTM,CRM_NOT_SENT_BV.UPDATE_ID,CRM_NOT_SENT_BV.UPDATE_DTTM,CRM_NOT_SENT_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_NOT_SENT_BV ;
create view IF NOT EXISTS EMAIL_OPEN(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANS_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE COMMENT 'The date the open took place',
	EVENT_TYPE COMMENT 'Event type = opened email',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'An ID used by external partners use to identify the data source',
	IS_UNIQUE_IND COMMENT 'Whether the event is unique or repeated',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_OPEN_VAPP contains the list of users that opened emails they received.'
 as 
SELECT CRM_OPEN.BRAND_ID,CRM_OPEN.SOURCE_SYSTEM_NM,CRM_OPEN.ACCOUNT_ID,CRM_OPEN.JOB_ID,CRM_OPEN.SUBSCRIBER_KEY,CRM_OPEN.MESSAGE_ID,CRM_OPEN.SERVICE_TRANSACTION_ID,CRM_OPEN.EMAIL_ADDRESS,CRM_OPEN.SUBSCRIBER_ID,CRM_OPEN.LIST_ID,CRM_OPEN.EVENT_DATE,CRM_OPEN.EVENT_TYPE,CRM_OPEN.BATCH_ID,CRM_OPEN.TRIGGERED_SEND_EXTERNAL_KEY,CRM_OPEN.IS_UNIQUE,CRM_OPEN.LOAD_ID,CRM_OPEN.LOAD_DTTM,CRM_OPEN.UPDATE_ID,CRM_OPEN.UPDATE_DTTM,CRM_OPEN.LOAD_FILENAME
FROM IDS_DEV.CUST.CRM_OPEN ;
create view IF NOT EXISTS EMAIL_SEND_JOB(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	FROM_NAME COMMENT 'Name of the overarching category sending the email',
	FROM_EMAIL COMMENT 'Actual email address sending the email',
	SCHED_TIME COMMENT 'Time the email was scheduled to send',
	SUBJECT_LINE COMMENT 'Subject line of the sent email',
	EMAIL_NAME COMMENT 'Category of email sent',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'An ID used by external partners use to identify the data source',
	SEND_DEFINITION_EXTERNAL_KEY COMMENT 'External key for the type of send going out. ',
	JOB_STATUS COMMENT 'Status of the job:,Scheduled,Sending,Completed,Stopped,Canceled,Error,Deleted,PostSendCallout,New',
	PREVIEW_URL COMMENT 'Preview of URL in the email',
	IS_MULTIPART_IND COMMENT 'True/False for whether or not the job has multiple parts.',
	ADDITIONAL_TXT COMMENT 'All values currently null',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_SEND_JOB_VAPP is list of all jobs created to send emails (including to whom).'
 as 
SELECT CRM_SEND_JOB_BV.BRAND_ID,CRM_SEND_JOB_BV.SOURCE_SYSTEM_NM,CRM_SEND_JOB_BV.ACCOUNT_ID,CRM_SEND_JOB_BV.JOB_ID,CRM_SEND_JOB_BV.MESSAGE_ID,CRM_SEND_JOB_BV.FROM_NAME,CRM_SEND_JOB_BV.FROM_EMAIL,CRM_SEND_JOB_BV.SCHED_TIME,CRM_SEND_JOB_BV.SUBJECT_LINE,CRM_SEND_JOB_BV.EMAIL_NAME,CRM_SEND_JOB_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_SEND_JOB_BV.SEND_DEFINITION_EXTERNAL_KEY,CRM_SEND_JOB_BV.JOB_STATUS,CRM_SEND_JOB_BV.PREVIEW_URL,CRM_SEND_JOB_BV.IS_MULTIPART,CRM_SEND_JOB_BV.ADDITIONAL,CRM_SEND_JOB_BV.LOAD_ID,CRM_SEND_JOB_BV.LOAD_DTTM,CRM_SEND_JOB_BV.UPDATE_ID,CRM_SEND_JOB_BV.UPDATE_DTTM,CRM_SEND_JOB_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_SEND_JOB_BV ;
create view IF NOT EXISTS EMAIL_SENT(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANS_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE COMMENT 'The date the email was sent',
	EVENT_TYPE COMMENT 'Event type = sent email',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'The TriggeredSendExternalKey text used for any batches used in the send',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_SENT_VAPP contains a list of emails that were sent successfully.'
 as 
SELECT CRM_SENT_BV.BRAND_ID,CRM_SENT_BV.SOURCE_SYSTEM_NM,CRM_SENT_BV.ACCOUNT_ID,CRM_SENT_BV.JOB_ID,CRM_SENT_BV.SUBSCRIBER_KEY,CRM_SENT_BV.MESSAGE_ID,CRM_SENT_BV.SERVICE_TRANSACTION_ID,CRM_SENT_BV.EMAIL_ADDRESS,CRM_SENT_BV.SUBSCRIBER_ID,CRM_SENT_BV.LIST_ID,CRM_SENT_BV.EVENT_DATE,CRM_SENT_BV.EVENT_TYPE,CRM_SENT_BV.BATCH_ID,CRM_SENT_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_SENT_BV.LOAD_ID,CRM_SENT_BV.LOAD_DTTM,CRM_SENT_BV.UPDATE_ID,CRM_SENT_BV.UPDATE_DTTM,CRM_SENT_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_SENT_BV ;
create view IF NOT EXISTS EMAIL_STATUS_CHANGE(
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	ACCOUNT_ID,
	SUBSCRIBER_KEY,
	OLD_STATUS,
	NEW_STATUS,
	DATE_CHANGED,
	SERVICE_TRANS_ID,
	EMAIL_ADDRESS,
	SUBSCRIBER_ID,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	LOAD_FILE_NAME
) as 
SELECT CRM_STATUS_CHANGE_BV.BRAND_ID,CRM_STATUS_CHANGE_BV.SOURCE_SYSTEM_NM,CRM_STATUS_CHANGE_BV.ACCOUNT_ID,CRM_STATUS_CHANGE_BV.SUBSCRIBER_KEY,CRM_STATUS_CHANGE_BV.OLD_STATUS,CRM_STATUS_CHANGE_BV.NEW_STATUS,CRM_STATUS_CHANGE_BV.DATE_CHANGED,CRM_STATUS_CHANGE_BV.SERVICE_TRANSACTION_ID,CRM_STATUS_CHANGE_BV.EMAIL_ADDRESS,CRM_STATUS_CHANGE_BV.SUBSCRIBER_ID,CRM_STATUS_CHANGE_BV.LOAD_ID,CRM_STATUS_CHANGE_BV.LOAD_DTTM,CRM_STATUS_CHANGE_BV.UPDATE_ID,CRM_STATUS_CHANGE_BV.UPDATE_DTTM,CRM_STATUS_CHANGE_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_STATUS_CHANGE_BV ;
create view IF NOT EXISTS EMAIL_UNSUBSCRIBE(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANS_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID COMMENT '''''',
	EVENT_DATE COMMENT 'Date click event occurred',
	EVENT_TYPE COMMENT '''''',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT '''''',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_UNSUBSCRIBE_VAPP list of users who unsubscribed from email communication.'
 as 
SELECT CRM_UNSUBSCRIBE_BV.BRAND_ID,CRM_UNSUBSCRIBE_BV.SOURCE_SYSTEM_NM,CRM_UNSUBSCRIBE_BV.ACCOUNT_ID,CRM_UNSUBSCRIBE_BV.JOB_ID,CRM_UNSUBSCRIBE_BV.SUBSCRIBER_KEY,CRM_UNSUBSCRIBE_BV.MESSAGE_ID,CRM_UNSUBSCRIBE_BV.SERVICE_TRANSACTION_ID,CRM_UNSUBSCRIBE_BV.EMAIL_ADDRESS,CRM_UNSUBSCRIBE_BV.SUBSCRIBER_ID,CRM_UNSUBSCRIBE_BV.LIST_ID,CRM_UNSUBSCRIBE_BV.EVENT_DATE,CRM_UNSUBSCRIBE_BV.EVENT_TYPE,CRM_UNSUBSCRIBE_BV.BATCH_ID,CRM_UNSUBSCRIBE_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_UNSUBSCRIBE_BV.LOAD_ID,CRM_UNSUBSCRIBE_BV.LOAD_DTTM,CRM_UNSUBSCRIBE_BV.UPDATE_ID,CRM_UNSUBSCRIBE_BV.UPDATE_DTTM,CRM_UNSUBSCRIBE_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_UNSUBSCRIBE_BV ;
create view IF NOT EXISTS LOYALTY_ACTIVITY(
	BRAND_ID,
	CUSTOMER_ACTIVITY_ID,
	PROFILE_ID,
	POINT_ACTIVITY_ID,
	TRANS_ID,
	MEMBER_CARD_NBR,
	POINTS,
	ACTIVITY_TYPE_DESC,
	REST_ID,
	TRANS_NBR,
	CERTIFICATE_NBR,
	ACTIVITY_TYPE_GROUP,
	ACTIVITY_TYPE,
	PRIVACY_IND,
	CREATE_DATE,
	CDM_LOAD_DATE,
	SOURCE,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as 
SELECT c.BRAND_ID AS BRAND_ID
, c.CUST_ACTVY_ID AS CUSTOMER_ACTIVITY_ID
, c.PROFILE_ID AS PROFILE_ID
, c.POINT_ACTVY_ID AS POINT_ACTIVITY_ID
, c.TRX_ID AS TRANS_ID
, c.MBR_CARD_NBR AS MEMBER_CARD_NBR
, c.POINT_QTY AS POINTS
, c.ACTVY_TYP_DESC AS ACTIVITY_TYPE_DESC
, c.STORE_LOC_NBR AS REST_ID
, c.TRX_NBR AS TRANS_NBR
, c.CERTIFICATE_NBR AS CERTIFICATE_NBR
, c.ACTVY_TYP_GROUP_CD AS ACTIVITY_TYPE_GROUP
, c.ACTVY_TYP_CD AS ACTIVITY_TYPE
, c.PRIVACY_IND AS PRIVACY_IND
, c.CREATE_DTTM AS CREATE_DATE
, c.CDM_LOAD_DT AS CDM_LOAD_DATE
, c.SOURCE_SYSTEM_NM AS SOURCE
, c.LOAD_ID AS LOAD_ID
, c.LOAD_DTTM AS LOAD_DTTM
, c.UPDATE_ID AS UPDATE_ID
, c.UPDATE_DTTM AS UPDATE_DTTM
FROM IDM_DEV.COREDIM_BV.LOYALTY_ACTVY_FACT_BV c;
create view IF NOT EXISTS LOYALTY_CERTIFICATE(
	BRAND_ID,
	CUSTOMER_CERTIFICATE_ID,
	PROFILE_ID,
	TRANS_ID,
	MEMBER_CARD_NBR,
	REWARD_NAME,
	CERTIFICATE_NBR,
	PRICE_IN_POINTS,
	REWARD_CODE,
	TRANS_NBR,
	CERTIFICATE_TYPE,
	CERTIFICATE_STATUS,
	PLU_NBR,
	ITEM_DOLLAR_VAL,
	QTY_ORDERED,
	PRIVACY_IND,
	START_DATE,
	END_DATE,
	REWARD_ORDER_DATE,
	MODIFIED_DATE,
	LOAD_DATE,
	CDM_LOAD_DATE,
	SOURCE,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as 
SELECT c.BRAND_ID AS BRAND_ID
, c.CUST_CERTIFICATE_ID AS CUSTOMER_CERTIFICATE_ID
, c.PROFILE_ID AS PROFILE_ID
, c.TRX_ID AS TRANS_ID
, c.MBR_CARD_NBR AS MEMBER_CARD_NBR
, c.REWARD_NM AS REWARD_NAME
, c.CERTIFICATE_NBR AS CERTIFICATE_NBR
, c.PRICE_IN_POINT_QTY AS PRICE_IN_POINTS
, c.REWARD_CD AS REWARD_CODE
, c.TRX_NBR AS TRANS_NBR
, c.CERTIFICATE_TYP_CD AS CERTIFICATE_TYPE
, c.CERTIFICATE_STATUS_CD AS CERTIFICATE_STATUS
, c.PLU_NBR AS PLU_NBR
, c.ITEM_DOLLAR_AMT AS ITEM_DOLLAR_VAL
, c.ORDER_QTY AS QTY_ORDERED
, c.PRIVACY_IND AS PRIVACY_IND
, c.START_DTTIME AS START_DATE
, c.END_DTTIME AS END_DATE
, c.REWARD_ORDER_DTTM AS REWARD_ORDER_DATE
, c.SOURCE_MODIFIED_DTTM AS MODIFIED_DATE
, c.SOURCE_LOAD_DTTM AS LOAD_DATE
, c.CDM_LOAD_DT AS CDM_LOAD_DATE
, c.SOURCE_SYSTEM_NM AS SOURCE
, c.LOAD_ID AS LOAD_ID
, c.LOAD_DTTM AS LOAD_DTTM
, c.UPDATE_ID AS UPDATE_ID
, c.UPDATE_DTTM AS UPDATE_DTTM
FROM IDM_DEV.COREDIM_BV.LOYALTY_CERTIFICATE_FACT_BV c;
create view IF NOT EXISTS LOYALTY_DISCOUNT(
	BRAND_ID,
	TRAN_DISCOUNT_ID,
	TRANS_ID,
	SOURCE,
	OFFER_CODE,
	SOURCE_ITEM_ID,
	DISCOUNT_AMT,
	DISCOUNT_DESC,
	CDM_LOAD_DATE,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as 
SELECT c.BRAND_ID AS BRAND_ID
, c.TRX_DISCOUNT_ID AS TRAN_DISCOUNT_ID
, c.TRX_ID AS TRANS_ID
, c.SOURCE_SYSTEM_NM AS SOURCE
, c.SOURCE_ITEM_ID AS OFFER_CODE
, c.OFFER_CD AS SOURCE_ITEM_ID
, c.DISCOUNT_AMT AS DISCOUNT_AMT
, c.DISCOUNT_DESC AS DISCOUNT_DESC
, c.CDM_LOAD_DT AS CDM_LOAD_DATE
, c.LOAD_ID AS LOAD_ID
, c.LOAD_DTTM AS LOAD_DTTM
, c.UPDATE_ID AS UPDATE_ID
, c.UPDATE_DTTM AS UPDATE_DTTM
FROM IDM_DEV.COREDIM_BV.LOYALTY_DISCOUNT_FACT_BV c;
create view IF NOT EXISTS LOYALTY_TRANSACTION(
	TRANS_ID,
	ORDER_ID,
	EMPLOYEE_ID,
	PROFILE_ID,
	REST_ID,
	BRAND_ID,
	BUSINESS_DATE,
	EMPLOYEE_NAME,
	CHECK_NBR,
	MEMBER_CARD_NBR,
	DOLLAR_NET_VALUE,
	ELIGIBLE_REVENUE,
	METHOD_OF_ATTACHMENT,
	TRANS_STATUS,
	MEMBER_PHONE_NBR,
	REASON_CODE,
	MIN_DAY_PART_ID,
	MAX_DAY_PART_ID,
	SUSPEND_TRANS_ID,
	SUSPEND_TRANS_STATUS,
	SOURCE_SYSTEM_NM,
	LOAD_TYPE,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	FILE_NAME
) as
SELECT
c.TRANS_ID AS TRANS_ID
,c.ORDER_ID AS ORDER_ID
,c.EMPLOYEE_ID AS EMPLOYEE_ID
,c.PROFILE_ID AS PROFILE_ID
,c.REST_ID AS REST_ID
,c.BRAND_ID AS BRAND_ID
,c.BUSINESS_DATE AS BUSINESS_DATE
,c.EMPLOYEE_NAME AS EMPLOYEE_NAME
,c.CHECK_NBR AS CHECK_NBR
,c.MEMBER_CARD_NBR AS MEMBER_CARD_NBR
,c.DOLLAR_NET_VALUE AS DOLLAR_NET_VALUE
,c.ELIGIBLE_REVENUE AS ELIGIBLE_REVENUE
,c.METHOD_OF_ATTACHMENT AS METHOD_OF_ATTACHMENT
,c.TRANS_STATUS AS TRANS_STATUS
,c.MEMBER_PHONE_NBR AS MEMBER_PHONE_NBR
,c.REASON_CODE AS REASON_CODE
,c.MIN_DAY_PART_ID AS MIN_DAY_PART_ID
,c.MAX_DAY_PART_ID AS MAX_DAY_PART_ID
,c.SUSPEND_TRANS_ID AS SUSPEND_TRANS_ID
,c.SUSPEND_TRANS_STATUS AS SUSPEND_TRANS_STATUS
,c.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM
,c.LOAD_TYPE AS LOAD_TYPE
,c.LOAD_ID AS LOAD_ID
,c.LOAD_DTTM AS LOAD_DTTM
,c.UPDATE_ID AS UPDATE_ID
,c.UPDATE_DTTM AS UPDATE_DTTM
,c.FILE_NAME AS FILE_NAME
FROM IDS_DEV.TXN_BV.LOYALTY_TRANSACTION_BV C;
create view IF NOT EXISTS OMS_MEMBER_OFFER(
	BRAND_ID,
	MEMBER_OFFER_ID,
	MEMBER_ID,
	SOURCE_SYSTEM_NAME,
	OFFER_CODE,
	PRIVACY_IND,
	OPT_IN_DATE,
	CREATE_DATE,
	EXPIRATION_DATE,
	LOAD_DATE,
	MODIFIED_DATE,
	CDM_LOAD_DATE,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as 
SELECT c.BRAND_ID AS BRAND_ID
, c.MBR_OFFER_ID AS MEMBER_OFFER_ID
, c.MBR_ID AS MEMBER_ID
, c.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NAME
, c.OFFER_CD AS OFFER_CODE
, c.PRIVACY_IND AS PRIVACY_IND
, c.OPT_IN_DTTM AS OPT_IN_DATE
, c.CREATE_DTTM AS CREATE_DATE
, c.EXPIRATION_DTTM AS EXPIRATION_DATE
, c.SOURCE_LOADED_DTTM AS LOAD_DATE
, c.SOURCE_MODIFIED_DTTM AS MODIFIED_DATE
, c.CDM_LOAD_DT AS CDM_LOAD_DATE
, c.LOAD_ID AS LOAD_ID
, c.LOAD_DTTM AS LOAD_DTTM
, c.UPDATE_ID AS UPDATE_ID
, c.UPDATE_DTTM AS UPDATE_DTTM
FROM IDM_DEV.COREDIM_BV.LOYALTY_MBR_OFFER_BV c;
create view IF NOT EXISTS OMS_OFFER(
	BRAND_ID,
	OFFER_CODE,
	UNIQ_OFFER_ID,
	VARIANT_ID,
	OFFER_SEQ_KEY,
	OFFER_NAME,
	OFFER_REPORT_DESC,
	OFFER_SHORT_DESC,
	OFFER_DESC,
	OPTIN_REQUIRED_IND,
	LOCATION_NAME,
	EXCLUSIONS_TXT,
	OFFER_IMAGE_NAME,
	TERMS_AND_CONDITIONS,
	MOMENT_ELIGIBILITY,
	STANDARD_EXCLUSIONS_TXT,
	OTHER_EXCLUSIONS,
	POINTS_AWARDED_NBR,
	LIMITS_NBR,
	REST_LOCATION_NBR,
	DAYPART,
	MEMBER_LIST_NEEDED,
	REST_TYPE,
	ONE_TIME_OFFER_IND,
	TRIVIA_NEEDED_IND,
	CHECK_IN_IND,
	OFFER_PRIORITY_NBR,
	OFFER_TYPE,
	AWARD_NAME,
	PRODUCT_PLU_DESC,
	TRIVIA_QUESTION_IND,
	TRIVIA_ANSWERS_IND,
	TARGET_OFFER_IND,
	PRIVACY_IND,
	VALIDITY_PERIOD,
	VALIDITY_UNIT_DATE_NAME,
	DNA_INCENTIVE_TYPE,
	DNA_CAMPAIGN_TYPE,
	DNA_DISCOUNT_PRODUCT_NAME,
	DNA_PRODUCT_CATEGORY,
	DNA_ACTION_REQUIRED,
	DNA_MINIMUM_SPEND,
	DNA_OFFER_WEEK_IN_CAMPAIGN,
	OFFER_VISIBLE_DATE,
	OFFER_START_DATE,
	OFFER_END_DATE,
	DNA_INCENTIVE_VAL,
	SYSTEM_OFFER_ID,
	DNA_DISCOUNT_PRODUCT_L_CODE,
	DNA_OFFER_REDEMPTION_CHANNEL,
	STATUS_CODE,
	OFFER_IMAGE2_NAME,
	SOURCE_APPLICATION_TYPE,
	SOURCE_SYSTEM_NAME,
	CDM_LOAD_DATE,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	OFFER_TAG,
	PARENT_OFFER_ID
) as 
SELECT c.BRAND_ID AS BRAND_ID
, c.OFFER_CD AS OFFER_CODE
, c.UNIQUE_OFFER_ID AS UNIQ_OFFER_ID
, c.VARIANT_ID AS VARIANT_ID
, c.OFFER_SEQ_KEY AS OFFER_SEQ_KEY
, c.OFFER_NM AS OFFER_NAME
, c.OFFER_REPORT_DESC AS OFFER_REPORT_DESC
, c.OFFER_SHORT_DESC AS OFFER_SHORT_DESC
, c.OFFER_DESC AS OFFER_DESC
, c.OPT_IN_REQUIRED_IND AS OPTIN_REQUIRED_IND
, c.LOC_NM AS LOCATION_NAME
, c.EXCLUSION_TXT AS EXCLUSIONS_TXT
, c.OFFER_IMG_NM AS OFFER_IMAGE_NAME
, c.TERM_AND_CONDITION_TXT AS TERMS_AND_CONDITIONS
, c.MOMENT_ELIGIBILITY_TXT AS MOMENT_ELIGIBILITY
, c.STANDARD_EXCLUSION_TXT AS STANDARD_EXCLUSIONS_TXT
, c.OTHER_EXCLUSION_TXT AS OTHER_EXCLUSIONS
, c.POINT_AWARDED_QTY AS POINTS_AWARDED_NBR
, c.LIMIT_NBR AS LIMITS_NBR
, c.STORE_LOC_LIST_NBR AS REST_LOCATION_NBR
, c.DAY_PART_LIST_NBR AS DAYPART
, c.MBR_LIST_NEEDED_IND AS MEMBER_LIST_NEEDED
, c.STORE_TYP AS REST_TYPE
, c.ONE_TM_OFFER_IND AS ONE_TIME_OFFER_IND
, c.TRIVIA_NEEDED_IND AS TRIVIA_NEEDED_IND
, c.CHECK_IN_IND AS CHECK_IN_IND
, c.OFFER_PRIORITY_NBR AS OFFER_PRIORITY_NBR
, c.OFFER_TYP AS OFFER_TYPE
, c.AWARD_NM AS AWARD_NAME
, c.PRODUCT_PLUS_DESC AS PRODUCT_PLU_DESC
, c.TRIVIA_QUESTION_IND AS TRIVIA_QUESTION_IND
, c.TRIVIA_ANSWER_IND AS TRIVIA_ANSWERS_IND
, c.TARGET_OFFER_IND AS TARGET_OFFER_IND
, c.PRIVACY_IND AS PRIVACY_IND
, c.VALIDITY_PERIOD_NBR AS VALIDITY_PERIOD
, c.VALIDITY_UNIT_DT_NM AS VALIDITY_UNIT_DATE_NAME
, c.DNA_INCENTIVE_TYP AS DNA_INCENTIVE_TYPE
, c.DNA_CAMPAIGN_TYP AS DNA_CAMPAIGN_TYPE
, c.DNA_DISCOUNT_PRODUCT_NM AS DNA_DISCOUNT_PRODUCT_NAME
, c.DNA_PRODUCT_CATEGORY_NM AS DNA_PRODUCT_CATEGORY
, c.DNA_ACTION_REQUIRED_TXT AS DNA_ACTION_REQUIRED
, c.DNA_MINIMUM_SPEND_AMT AS DNA_MINIMUM_SPEND
, c.DNA_OFFER_WEEK_IN_CAMPAIGN_NBR AS DNA_OFFER_WEEK_IN_CAMPAIGN
, c.OFFER_VISIBLE_DTTM AS OFFER_VISIBLE_DATE
, c.OFFER_START_DTTM AS OFFER_START_DATE
, c.OFFER_END_DTTM AS OFFER_END_DATE
, c.DNA_INCENTIVE_AMT AS DNA_INCENTIVE_VAL
, c.SYSTEM_OFFER_ID AS SYSTEM_OFFER_ID
, c.DNA_DISCOUNT_PRODUCT_LEVEL_CD AS DNA_DISCOUNT_PRODUCT_L_CODE
, c.DNA_OFFER_REDEMPTION_CHANNEL_TYP AS DNA_OFFER_REDEMPTION_CHANNEL
, c.STATUS_CD AS STATUS_CODE
, c.OFFER_IMAGE2_NM AS OFFER_IMAGE2_NAME
, c.SOURCE_APPLICATION_TYP AS SOURCE_APPLICATION_TYPE
, c.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NAME
, c.CDM_LOAD_DT AS CDM_LOAD_DATE
, c.LOAD_ID AS LOAD_ID
, c.LOAD_DTTM AS LOAD_DTTM
, c.UPDATE_ID AS UPDATE_ID
, c.UPDATE_DTTM AS UPDATE_DTTM
, c.OFFER_TAG AS OFFER_TAG
, c.PARENT_OFFER_ID AS PARENT_OFFER_ID
FROM IDM_DEV.COREDIM_BV.LOYALTY_OFFER_BV c;
create view IF NOT EXISTS PUSH_MESSAGE_DETAIL(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	EID COMMENT 'Eid is ',
	APP_NAME COMMENT 'App Name is Name of the mobile app message was sent to; configured in MobilePush Administration',
	MESSAGE_NAME COMMENT 'Message Name is Name of the sent message; populated by the name of the Push Activity in Journey Builder',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	CAMPAIGNS COMMENT 'Campaigns is Name of the campaign associated with the sent message',
	DEVICE_ID COMMENT 'Device Id is ID of the device that received the message. The Device ID is a unique identifier assigned to a specific mobile device owned by a contact',
	DATE_TIME_SEND COMMENT 'Date Time Send is Date and time, in UTC-6, that the message was sen',
	MESSAGE_CONTENT COMMENT 'Message Content is Content of the message. This field shows AMPscript, if used',
	MESSAGE_OPENED COMMENT 'Message Opened is Whether the message was opened on the device corresponding to the Device ID',
	OPEN_DATE COMMENT 'Open Date is When the message was opened',
	TIME_IN_APP COMMENT 'Time In App is How long the user spent in the app after opening the message. Measured from the time the subscriber opens the app to the time the app goes into the background',
	PLATFORM COMMENT 'Platform is Operating system of the device',
	PLATFORM_VERSION COMMENT 'Platform Version is Version of the operating system',
	STATUS COMMENT 'Status is Status of the send job at the device level. This field tells you whether the job succeeded or failed for the device. * Expected values are Success, Failed, and Blank only if theres no returned data on the send.',
	SERVICE_RESPONSE COMMENT 'Service Response is ',
	GEOFENCE_NAME COMMENT 'Geofence Name is Name of the geofence that triggered the message',
	TEMPLATE COMMENT 'Template is Template of the sent message: Outbound, Location Entry, Location Exit, or Beacon',
	FORMAT COMMENT 'Format is Format of the sent message: Application Alert or Application Alert & Landing Page',
	PAGE_NAME COMMENT 'Page Name is Subject field of the cloud page that is associated with the message',
	PUSH_JOB_ID COMMENT 'Push Job Id is ID of the job to use when troubleshooting with support',
	SYSTEM_TOKEN COMMENT 'System Token is Unique key that is used by Google or Apple to identify the device for sending',
	INBOX_DOWNLOAD COMMENT 'Inbox Download is Date and time the message was downloaded',
	INBOX_OPEN COMMENT 'Inbox Open is Date and time Inbox was opened',
	IOS_MEDIA_URL COMMENT 'Ios Media Url is ',
	ANDROID_MEDIA_URL COMMENT 'Android Media Url is ',
	MEDIA_ALT COMMENT 'Media Alt is ',
	CONTACT_KEY COMMENT 'Contact Key is ID of the contact that owns the device that received the message but not the ID of the device; one contact could have multiple devices. The ContactKey matches the Contacts and Devices ID. The ContactKey indicates the subscriber associated with the device when the report is run',
	REQUEST_ID COMMENT 'Request Id is Marketing Clouds unique identifier for every request',
	SERVICE_TRANS_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='Push Message Detail contains information about push notification interactions.'
 as 
SELECT CRM_PUSH_MESSAGE_DETAIL_BV.BRAND_ID,CRM_PUSH_MESSAGE_DETAIL_BV.SOURCE_SYSTEM_NM,CRM_PUSH_MESSAGE_DETAIL_BV.ACCOUNT_ID,CRM_PUSH_MESSAGE_DETAIL_BV.EID,CRM_PUSH_MESSAGE_DETAIL_BV.APP_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_ID,CRM_PUSH_MESSAGE_DETAIL_BV.CAMPAIGNS,CRM_PUSH_MESSAGE_DETAIL_BV.DEVICE_ID,CRM_PUSH_MESSAGE_DETAIL_BV.DATE_TIME_SEND,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_CONTENT,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_OPENED,CRM_PUSH_MESSAGE_DETAIL_BV.OPEN_DATE,CRM_PUSH_MESSAGE_DETAIL_BV.TIME_IN_APP,CRM_PUSH_MESSAGE_DETAIL_BV.PLATFORM,CRM_PUSH_MESSAGE_DETAIL_BV.PLATFORM_VERSION,CRM_PUSH_MESSAGE_DETAIL_BV.STATUS,CRM_PUSH_MESSAGE_DETAIL_BV.SERVICE_RESPONSE,CRM_PUSH_MESSAGE_DETAIL_BV.GEOFENCE_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.TEMPLATE,CRM_PUSH_MESSAGE_DETAIL_BV.FORMAT,CRM_PUSH_MESSAGE_DETAIL_BV.PAGE_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.PUSH_JOB_ID,CRM_PUSH_MESSAGE_DETAIL_BV.SYSTEM_TOKEN,CRM_PUSH_MESSAGE_DETAIL_BV.INBOX_DOWNLOAD,CRM_PUSH_MESSAGE_DETAIL_BV.INBOX_OPEN,CRM_PUSH_MESSAGE_DETAIL_BV.IOS_MEDIA_URL,CRM_PUSH_MESSAGE_DETAIL_BV.ANDROID_MEDIA_URL,CRM_PUSH_MESSAGE_DETAIL_BV.MEDIA_ALT,CRM_PUSH_MESSAGE_DETAIL_BV.CONTACT_KEY,CRM_PUSH_MESSAGE_DETAIL_BV.REQUEST_ID,CRM_PUSH_MESSAGE_DETAIL_BV.SERVICE_TRANSACTION_ID,CRM_PUSH_MESSAGE_DETAIL_BV.LOAD_ID,CRM_PUSH_MESSAGE_DETAIL_BV.LOAD_DTTM,CRM_PUSH_MESSAGE_DETAIL_BV.UPDATE_ID,CRM_PUSH_MESSAGE_DETAIL_BV.UPDATE_DTTM,CRM_PUSH_MESSAGE_DETAIL_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_PUSH_MESSAGE_DETAIL_BV ;
create view IF NOT EXISTS PUSH_SEND_LOG(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	PUSH_JOB_ID COMMENT 'Push Job Id is the ID of the job, or bundle of send, that included the push notification ',
	PUSH_TRIGGERED_SEND_REQUEST_ID COMMENT 'Push Triggered Send Request Id is TokenID that is returned from the API call. When you use list and data extension sends, is the same as the PushJobID ',
	PUSH_BATCH_ID COMMENT 'Push Batch Id is The ID of the batch, or bundle of jobs, for batched sends',
	SUBSCRIBER_ID COMMENT 'Subscriber Id is the ID of the subscriber to whom the message was sent',
	DEVICE_ID COMMENT 'Device Id is the ID of the device that received the push notification',
	APPLICATION_ID COMMENT 'Application Id is the ID of the app that received the push notification ',
	LOG_DATE COMMENT 'Log Date is The date that this data was written to the send log ',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILE_NAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='Push Send Log lists all the push notifications that occur to customers by day.'
 as 
SELECT CRM_PUSH_SEND_LOG_BV.BRAND_ID,CRM_PUSH_SEND_LOG_BV.SOURCE_SYSTEM_NM,CRM_PUSH_SEND_LOG_BV.PUSH_JOB_ID,CRM_PUSH_SEND_LOG_BV.PUSH_TRIGGERED_SEND_REQUEST_ID,CRM_PUSH_SEND_LOG_BV.PUSH_BATCH_ID,CRM_PUSH_SEND_LOG_BV.SUB_ID,CRM_PUSH_SEND_LOG_BV.DEVICE_ID,CRM_PUSH_SEND_LOG_BV.APP_ID,CRM_PUSH_SEND_LOG_BV.LOG_DATE,CRM_PUSH_SEND_LOG_BV.LOAD_ID,CRM_PUSH_SEND_LOG_BV.LOAD_DTTM,CRM_PUSH_SEND_LOG_BV.UPDATE_ID,CRM_PUSH_SEND_LOG_BV.UPDATE_DTTM,CRM_PUSH_SEND_LOG_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_PUSH_SEND_LOG_BV ;