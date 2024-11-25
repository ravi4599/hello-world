create TABLE IF NOT EXISTS CHANNEL_STATUS_TYPE (
	CHANNEL_STATUS_TYPE_CD VARCHAR(16777216) NOT NULL COMMENT 'The Code value for the Channel',
	CHANNEL_STATUS_TYPE_DESC VARCHAR(16777216) COMMENT 'The Description for the Channel',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) COMMENT 'The POS System used to load the transaction  information\n',
	LOAD_TYPE VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKCHANNEL_STATUS_TYPE primary key (CHANNEL_STATUS_TYPE_CD)
);
create TABLE IF NOT EXISTS COLORS (
	YEAR NUMBER(4,0),
	COLOR VARCHAR(6),
	FAVORITE BOOLEAN
);
create TABLE IF NOT EXISTS CRM_BOUNCE (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber. This serves as the primary key.',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'Unique ID of each subscriber',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The data the bounce took place',
	EVENT_TYPE VARCHAR(16777216) COMMENT 'Bounced emails are the only type in this table',
	BOUNCE_CATEGORY VARCHAR(16777216) COMMENT 'Category of why the email bounced',
	SMTP_CODE NUMBER(38,0) COMMENT 'The error code for the bounce from the mail system',
	BOUNCE_REASON VARCHAR(16777216) COMMENT 'Reason why the email bounced relayed from the mail system',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'Why the email was sent',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_BOUNCE unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, MESSAGE_ID, SERVICE_TRANSACTION_ID)
)COMMENT='CRM_BOUNCE contains the marketing emails that did not make it to the customer and the reason why.'
;
create TABLE IF NOT EXISTS CRM_CAMPAIGN (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CAMPAIGN_ID VARCHAR(16777216) NOT NULL COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_TYPE VARCHAR(16777216) NOT NULL COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_NAME VARCHAR(16777216) COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_CATEGORY VARCHAR(16777216) COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESCRIPTION VARCHAR(16777216) COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE VARCHAR(16777216) COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION VARCHAR(16777216) COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XPKCRM_CAMPAIGN primary key (BRAND_ID, SOURCE_SYSTEM_NM, CAMPAIGN_ID, CAMPAIGN_TYPE)
)COMMENT='CRM_Campaign contains a list of marketing events for customers.'
;
create TABLE IF NOT EXISTS CRM_CLICK (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the click took place',
	EVENT_TYPE VARCHAR(16777216) COMMENT 'The event = click for this table',
	SEND_URL_ID NUMBER(38,0) COMMENT 'A unique ID for a sent URL',
	URL_ID NUMBER(38,0) COMMENT 'A unique ID for a URL',
	URL VARCHAR(16777216) COMMENT 'The URL for the link clicked. No AMPscript or variables are populated in this column, for example, www.example.com?%attribute%',
	ALIAS VARCHAR(16777216) COMMENT 'Brief description of the URL sent via email',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'An ID used by external partners use to identify the data source',
	IS_UNIQUE BOOLEAN COMMENT 'Whether the event is unique or repeated. NOTE: The IsUnique value is TRUE when any link is first clicked in a JobID by a subscriber. Any clicks afterwards are FALSE even if different URLs',
	IS_UNIQUE_FOR_URL BOOLEAN COMMENT 'Whether the event is unique or repeated. NOTE: The IsUniqueForURL value is TRUE when any link is first clicked in a JobID by a subscriber. Unlike IsUnique, it is not FALSE for different URLs',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_CLICK unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, MESSAGE_ID, SERVICE_TRANSACTION_ID)
)COMMENT='CRM_CLICK specifies the links in an email that were clicked and by whom.'
;
create TABLE IF NOT EXISTS CRM_COMPLAINT (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the complaint took place.',
	EVENT_TYPE VARCHAR(16777216) COMMENT 'Short description for type of complaint',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'An ID used by external partners use to identify the data source',
	DOMAIN VARCHAR(16777216) COMMENT 'The domain at which the complaint occurred',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_COMPLAINT unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, MESSAGE_ID, SERVICE_TRANSACTION_ID)
)COMMENT='CRM_COMPLAINT contains a list of users who submitted a complaint.'
;
create TABLE IF NOT EXISTS CRM_CREATIVE_VARIANT (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	OFFER_ID VARCHAR(16777216) NOT NULL COMMENT 'All values currently null',
	OFFER_CODE VARCHAR(16777216) COMMENT 'All values currently null',
	OFFER_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	PARENT_OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	CAMPAIGN_TYPE VARCHAR(16777216) COMMENT 'All values currently null',
	CAMPAIGN_ID VARCHAR(16777216) COMMENT 'All values currently null',
	SUBJECT_LINE VARCHAR(16777216) COMMENT 'All values currently null',
	PRE_HEADER VARCHAR(16777216) COMMENT 'All values currently null',
	HEAD_LINE VARCHAR(16777216) COMMENT 'All values currently null',
	CTA_URL VARCHAR(16777216) COMMENT 'All values currently null',
	HERO_IMAGE VARCHAR(16777216) COMMENT 'All values currently null',
	HERO_COPY VARCHAR(16777216) COMMENT 'All values currently null',
	SECONDARY_MODULE_IMAGE VARCHAR(16777216) COMMENT 'All values currently null',
	SECONDARY_MODULE_COPY VARCHAR(16777216) COMMENT 'All values currently null',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'All values currently null',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XPKCRM_CREATIVE_VARIANT primary key (BRAND_ID, SOURCE_SYSTEM_NM, OFFER_ID)
)COMMENT='CRM_CREATIVE VARIANT is a record of the creative copy, images, subject, line, etc. used for each email sent.'
;
create TABLE IF NOT EXISTS CRM_CREATIVE_VARIANT_MVP (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CREATIVE_VARIANT_PART VARCHAR(16777216) NOT NULL COMMENT 'Creative Variant Part is ',
	CAMPAIGN_TYPE VARCHAR(16777216) COMMENT 'Campaign Type is ',
	CAMPAIGN_ID VARCHAR(16777216) COMMENT 'Campaign Id is ',
	SUBJECT_LINE VARCHAR(16777216) COMMENT 'Subject Line is ',
	PRE_HEADER VARCHAR(16777216) COMMENT 'Pre Header is ',
	IMAGE VARCHAR(16777216) COMMENT 'Image is ',
	IMAGE_LINK VARCHAR(16777216) COMMENT 'Image Link is ',
	IMAGE_ALT VARCHAR(16777216) COMMENT 'Image Alt is ',
	HEADER_TEXT VARCHAR(16777216) COMMENT 'Header Text is ',
	BODY_TEXT VARCHAR(16777216) COMMENT 'Body Text is ',
	LEFT_CTA_TEXT VARCHAR(16777216) COMMENT 'Left  Cta Text is ',
	LEFT_CTA_LINK VARCHAR(16777216) COMMENT 'Left Cta Link is ',
	RIGHT_CTA_TEXT VARCHAR(16777216) COMMENT 'Right Cta Text is ',
	RIGHT_CTA_LINK VARCHAR(16777216) COMMENT 'Right Cta Link is ',
	CENTER_CTA_TEXT VARCHAR(16777216) COMMENT 'Center Cta Text is ',
	CENTER_CTA_LINK VARCHAR(16777216) COMMENT 'Center Cta Link is ',
	TOP_LEFT_IMAGE_NAME VARCHAR(16777216) COMMENT 'Top Left Image Name is ',
	TOP_LEFT_IMAGE VARCHAR(16777216) COMMENT 'Top Left Image is ',
	TOP_LEFT_LINK VARCHAR(16777216) COMMENT 'Top Left Link is ',
	TOP_LEFT_IMAGE_ALT VARCHAR(16777216) COMMENT 'Top Left Image Alt is ',
	TOP_RIGHT_IMAGE_NAME VARCHAR(16777216) COMMENT 'Top Right Image Name is ',
	TOP_RIGHT_IMAGE VARCHAR(16777216) COMMENT 'Top Right Image is ',
	TOP_RIGHT_LINK VARCHAR(16777216) COMMENT 'Top Right Link is ',
	TOP_RIGHT_IMAGE_ALT VARCHAR(16777216) COMMENT 'Top Right Image Alt is ',
	BOTTOM_LEFT_IMAGE_NAME VARCHAR(16777216) COMMENT 'Bottom Left Image Name is ',
	BOTTOM_LEFT_IMAGE VARCHAR(16777216) COMMENT 'Bottom Left Image is ',
	BOTTOM_LEFT_LINK VARCHAR(16777216) COMMENT 'Bottom Left Link is ',
	BOTTOM_LEFT_IMAGE_ALT VARCHAR(16777216) COMMENT 'Bottom Left Image Alt is ',
	BOTTOM_RIGHT_IMAGE_NAME VARCHAR(16777216) COMMENT 'Bottom Right Image Name is ',
	BOTTOM_RIGHT_IMAGE VARCHAR(16777216) COMMENT 'Bottom Right Image is ',
	BOTTOM_RIGHT_LINK VARCHAR(16777216) COMMENT 'Bottom Right Link is ',
	BOTTOM_RIGHT_IMAGE_ALT VARCHAR(16777216) COMMENT 'Bottom Right Image Alt is ',
	ICON_HEADER_1 VARCHAR(16777216) COMMENT 'Icon Header 1 is ',
	ICON_BODY_1 VARCHAR(16777216) COMMENT 'Icon Body 1 is ',
	ICON_IMAGE_1 VARCHAR(16777216) COMMENT 'Icon Image 1 is ',
	ICON_IMAGE_LINK_1 VARCHAR(16777216) COMMENT 'Icon Image Link 1 is ',
	ICON_IMAGE_ALT_1 VARCHAR(16777216) COMMENT 'Icon Image Alt 1 is ',
	ICON_HEADER_2 VARCHAR(16777216) COMMENT 'Icon Header 2 is ',
	ICON_BODY_2 VARCHAR(16777216) COMMENT 'Icon Body 2 is ',
	ICON_IMAGE_2 VARCHAR(16777216) COMMENT 'Icon Image 2 is ',
	ICON_IMAGE_LINK_2 VARCHAR(16777216) COMMENT 'Icon Image Link 2 is ',
	ICON_IMAGE_ALT_2 VARCHAR(16777216) COMMENT 'Icon Image Alt 2 is ',
	ICON_HEADER_3 VARCHAR(16777216) COMMENT 'Icon Header 3 is ',
	ICON_BODY_3 VARCHAR(16777216) COMMENT 'Icon Body 3 is ',
	ICON_IMAGE_3 VARCHAR(16777216) COMMENT 'Icon Image 3 is ',
	ICON_IMAGE_LINK_3 VARCHAR(16777216) COMMENT 'Icon Image Link 3 is ',
	ICON_IMAGE_ALT_3 VARCHAR(16777216) COMMENT 'Icon Image Alt 3 is ',
	TERMS_AND_CONDITIONS VARCHAR(16777216) COMMENT 'Terms And Conditions is ',
	EVENT_DATE DATE COMMENT 'Event Date is ',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XPKCRM_CREATIVE_VARIANT_MVP primary key (BRAND_ID, SOURCE_SYSTEM_NM, CREATIVE_VARIANT_PART)
)COMMENT='CRM Creative Variant MVP contains the marketing content that was sent to a customer in an marketing email.'
;
create TABLE IF NOT EXISTS CRM_HOLDOUT_LOG (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber. This serves as the primary key.',
	PROFILE_ID VARCHAR(16777216) COMMENT 'The unique ID associated with a profile',
	EXTERNAL_ID VARCHAR(16777216) COMMENT '''''',
	CUSTOMER_ID VARCHAR(16777216) COMMENT '''''',
	OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the affected subscriber was withheld from a promotion sent out',
	OFFER_CODE VARCHAR(16777216) COMMENT 'Description of the offer code sent',
	EMAIL_NAME VARCHAR(16777216) COMMENT 'Description of the email sent out (NOTE: I do not think the column should be called email_address)',
	CREATIVE_VARIANT VARCHAR(16777216) COMMENT 'Identifies which content blocks and images were used in an email',
	DATASOURCE_NAME VARCHAR(16777216) COMMENT 'Datasource name specifies source of the emails.  For example, Epsilon or Harmony.',
	OFFER_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	PARENT_OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_STEP VARCHAR(16777216) COMMENT 'All values currently null',
	STRENGTH_OF_CUSTOMER VARCHAR(16777216) COMMENT 'Not a current column????',
	USER_DEFINED_SEGMENT_1 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_2 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_3 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_4 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_5 VARCHAR(16777216) COMMENT 'All values currently null',
	OFFER_DECISION_LOGIC VARCHAR(16777216) COMMENT '''''',
	POINT_BALANCE NUMBER(38,0) COMMENT 'Numer of points earned associated with that account',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'EMAIL_ADDRESS. .',
	CAMPAIGN_ID VARCHAR(16777216) COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_NAME VARCHAR(16777216) COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_TYPE VARCHAR(16777216) COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_CATEGORY VARCHAR(16777216) COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESCRIPTION VARCHAR(16777216) COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE VARCHAR(16777216) COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION VARCHAR(16777216) COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_HOLDOUT_LOG unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, PROFILE_ID, EXTERNAL_ID, CUSTOMER_ID),
	constraint CRMS_CREATIVE_VARIANT_TO_CRM_HOLDOUT_LOG foreign key (BRAND_ID, SOURCE_SYSTEM_NM, OFFER_ID) references CRM_CREATIVE_VARIANT(BRAND_ID,SOURCE_SYSTEM_NM,OFFER_ID)
)COMMENT='CRM_HOLDOUT LOG is a list of who has not resoponded to the marketing emails.'
;
create TABLE IF NOT EXISTS CRM_HOLDOUT_LOG_BKUP20211123 (
	BRAND_ID VARCHAR(20),
	SOURCE_SYSTEM_NM VARCHAR(255),
	ACCOUNT_ID NUMBER(38,0),
	JOB_ID VARCHAR(16777216),
	SUBSCRIBER_KEY VARCHAR(16777216),
	PROFILE_ID VARCHAR(16777216),
	EXTERNAL_ID VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	MESSAGE_ID VARCHAR(16777216),
	SERVICE_TRANSACTION_ID VARCHAR(16777216),
	OFFER_ID VARCHAR(16777216),
	EVENT_DATE TIMESTAMP_NTZ(9),
	OFFER_CODE VARCHAR(16777216),
	BUYING_SEG VARCHAR(16777216),
	EMAIL_NAME VARCHAR(16777216),
	CREATIVE_VARIANT VARCHAR(16777216),
	DATASOURCE_NAME VARCHAR(16777216),
	OFFER_NAME VARCHAR(16777216),
	PARENT_OFFER_ID VARCHAR(16777216),
	JOURNEY_NAME VARCHAR(16777216),
	JOURNEY_STEP VARCHAR(16777216),
	STRENGTH_OF_CUSTOMER VARCHAR(16777216),
	USER_DEFINED_SEGMENT_1 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_2 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_3 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_4 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_5 VARCHAR(16777216),
	OFFER_1_DECISION_LOGIC VARCHAR(16777216),
	POINT_BALANCE NUMBER(38,0),
	OFFER_START_DATE DATE,
	OFFER_END_DATE DATE,
	EMAIL_ADDRESS VARCHAR(16777216),
	CAMPAIGN_ID VARCHAR(16777216),
	CAMPAIGN_NAME VARCHAR(16777216),
	CAMPAIGN_TYPE VARCHAR(16777216),
	CAMPAIGN_CATEGORY VARCHAR(16777216),
	CAMPAIGN_DESCRIPTION VARCHAR(16777216),
	CAMPAIGN_OBJECTIVE VARCHAR(16777216),
	CAMPAIGN_START_DATE DATE,
	CAMPAIGN_END_DATE DATE,
	CAMPAIGN_DURATION VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS CRM_JOURNEY (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	VERSION_ID VARCHAR(16777216) NOT NULL COMMENT 'The unique identifier for the version of the journey',
	JOURNEY_ID VARCHAR(16777216) COMMENT 'The unique identifier for the journey. There are one or more VersionIDs associated to a JourneyID',
	JOURNEY_NAME VARCHAR(16777216) COMMENT 'The name of the Journey',
	CREATED_DATE TIMESTAMP_NTZ(9) COMMENT 'The date that the version of the journey was created',
	LAST_PUBLISHED_DATE TIMESTAMP_NTZ(9) COMMENT 'The date that the version of the journey was last published',
	MODIFIED_DATE TIMESTAMP_NTZ(9) COMMENT 'The date that the version of the journey was last edited',
	JOURNEY_STATUS VARCHAR(16777216) COMMENT 'The current running mode of the journey. Possible values are Draft, Running, Finishing and Stopped',
	VERSION_NUMBER VARCHAR(16777216) COMMENT 'The version number of the version of the journey',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_JOURNEY unique (BRAND_ID, SOURCE_SYSTEM_NM, JOURNEY_ID, JOURNEY_NAME, VERSION_NUMBER),
	constraint XPKCRM_JOURNEY primary key (BRAND_ID, SOURCE_SYSTEM_NM, VERSION_ID)
)COMMENT='CRM_JOURNEY is a high level of grouping of marketing interactions and their current status.'
;
create TABLE IF NOT EXISTS CRM_JOURNEY_EMAIL_ACTIVITY (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	OYB_ACCOUNT_ID NUMBER(38,0) COMMENT 'OYB_Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	VERSION_ID VARCHAR(16777216) NOT NULL COMMENT 'The unique identifier for the version of the journey',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Id in the CRM Journey Emaill Activity table is the Parentt Account Id. This value is populated with 100004555 and should be the same across brands. The On-Your-Behalf (OYB)  Account number is the account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only. The values for  BWW is 100040982 and Arbys is 100039504.',
	ACTIVITY_ID VARCHAR(16777216) COMMENT 'The unique identifier for the activity. There are one or more ActivityIDs associated to a VersionID',
	ACTIVITY_NAME VARCHAR(16777216) COMMENT 'The name of the Activity',
	ACTIVITY_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'The external key associated with the activity',
	JOURNEY_ACTIVITY_OBJECT_ID VARCHAR(16777216) COMMENT 'Use this unique identifier to join the email tracking system data views to identify a journey emails Triggered Send Definition',
	ACTIVITY_TYPE VARCHAR(16777216) COMMENT 'The type of activity',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_JOURNEY_EMAIL_ACTIVITY unique (BRAND_ID, SOURCE_SYSTEM_NM, OYB_ACCOUNT_ID, JOB_ID, VERSION_ID),
	constraint CRMS_JOURNEY_TO_CRM_JOURNEY_EMAIL_ACTIVITY foreign key (BRAND_ID, SOURCE_SYSTEM_NM, VERSION_ID) references CRM_JOURNEY(BRAND_ID,SOURCE_SYSTEM_NM,VERSION_ID),
	constraint CRMS_SEND_JOB_TO_CRM_JOURNEY_EMAIL_ACTIVITY foreign key (BRAND_ID, SOURCE_SYSTEM_NM, OYB_ACCOUNT_ID, JOB_ID) references CRM_SEND_JOB(BRAND_ID,SOURCE_SYSTEM_NM,ACCOUNT_ID,JOB_ID)
)COMMENT='CRM_JOURNEY_EMAIL_ACTIVITY contains the emails that are part of a CRM journey.'
;
create TABLE IF NOT EXISTS CRM_NOT_SENT (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affect customer. This serves as a primary key.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The event = date email was not sent',
	EVENT_TYPE VARCHAR(16777216) COMMENT 'The type = not sent emails',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'An ID used by external partners use to identify the data source',
	REASON VARCHAR(16777216) COMMENT 'The reason why the email was not sent.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_NOT_SENT unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY),
	constraint CRMS_SEND_JOB_TO_CRM_NOT_SENT foreign key (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID) references CRM_SEND_JOB(BRAND_ID,SOURCE_SYSTEM_NM,ACCOUNT_ID,JOB_ID)
)COMMENT='CRM_NOT_SENT is a list of emails that did not send and the reason why.'
;
create TABLE IF NOT EXISTS CRM_NOT_SENT_BKUP20211201 (
	BRAND_ID VARCHAR(20),
	SOURCE_SYSTEM_NM VARCHAR(255),
	ACCOUNT_ID NUMBER(38,0),
	JOB_ID VARCHAR(16777216),
	SUBSCRIBER_KEY VARCHAR(16777216),
	MESSAGE_ID VARCHAR(16777216),
	SERVICE_TRANSACTION_ID VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	SUBSCRIBER_ID NUMBER(38,0),
	LIST_ID NUMBER(38,0),
	EVENT_DATE TIMESTAMP_NTZ(9),
	EVENT_TYPE VARCHAR(16777216),
	BATCH_ID NUMBER(38,0),
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216),
	REASON VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS CRM_OPEN (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) NOT NULL COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) NOT NULL COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the open took place',
	EVENT_TYPE VARCHAR(16777216) COMMENT 'Event type = opened email',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'An ID used by external partners use to identify the data source',
	IS_UNIQUE BOOLEAN COMMENT 'Whether the event is unique or repeated',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_OPEN unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, MESSAGE_ID, SERVICE_TRANSACTION_ID)
)COMMENT='CRM_OPEN contains the list of users that opened emails they received.'
;
create TABLE IF NOT EXISTS CRM_PUSH_MESSAGE_DETAIL (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	EID NUMBER(38,0) COMMENT 'Eid is ',
	APP_NAME VARCHAR(16777216) COMMENT 'App Name is Name of the mobile app message was sent to; configured in MobilePush Administration',
	MESSAGE_NAME VARCHAR(16777216) COMMENT 'Message Name is Name of the sent message; populated by the name of the Push Activity in Journey Builder',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	CAMPAIGNS VARCHAR(16777216) COMMENT 'Campaigns is Name of the campaign associated with the sent message',
	DEVICE_ID VARCHAR(16777216) COMMENT 'Device Id is ID of the device that received the message. The Device ID is a unique identifier assigned to a specific mobile device owned by a contact',
	DATE_TIME_SEND TIMESTAMP_NTZ(9) COMMENT 'Date Time Send is Date and time, in UTC-6, that the message was sen',
	MESSAGE_CONTENT VARCHAR(16777216) COMMENT 'Message Content is Content of the message. This field shows AMPscript, if used',
	MESSAGE_OPENED VARCHAR(16777216) COMMENT 'Message Opened is Whether the message was opened on the device corresponding to the Device ID',
	OPEN_DATE TIMESTAMP_NTZ(9) COMMENT 'Open Date is When the message was opened',
	TIME_IN_APP VARCHAR(16777216) COMMENT 'Time In App is How long the user spent in the app after opening the message. Measured from the time the subscriber opens the app to the time the app goes into the background',
	PLATFORM VARCHAR(16777216) COMMENT 'Platform is Operating system of the device',
	PLATFORM_VERSION VARCHAR(16777216) COMMENT 'Platform Version is Version of the operating system',
	STATUS VARCHAR(16777216) COMMENT 'Status is Status of the send job at the device level. This field tells you whether the job succeeded or failed for the device. * Expected values are Success, Failed, and Blank only if theres no returned data on the send.',
	SERVICE_RESPONSE VARCHAR(16777216) COMMENT 'Service Response is ',
	GEOFENCE_NAME VARCHAR(16777216) COMMENT 'Geofence Name is Name of the geofence that triggered the message',
	TEMPLATE VARCHAR(16777216) COMMENT 'Template is Template of the sent message: Outbound, Location Entry, Location Exit, or Beacon',
	FORMAT VARCHAR(16777216) COMMENT 'Format is Format of the sent message: Application Alert or Application Alert & Landing Page',
	PAGE_NAME VARCHAR(16777216) COMMENT 'Page Name is Subject field of the cloud page that is associated with the message',
	PUSH_JOB_ID VARCHAR(16777216) COMMENT 'Push Job Id is ID of the job to use when troubleshooting with support',
	SYSTEM_TOKEN VARCHAR(16777216) COMMENT 'System Token is Unique key that is used by Google or Apple to identify the device for sending',
	INBOX_DOWNLOAD VARCHAR(16777216) COMMENT 'Inbox Download is Date and time the message was downloaded',
	INBOX_OPEN VARCHAR(16777216) COMMENT 'Inbox Open is Date and time Inbox was opened',
	IOS_MEDIA_URL VARCHAR(16777216) COMMENT 'Ios Media Url is ',
	ANDROID_MEDIA_URL VARCHAR(16777216) COMMENT 'Android Media Url is ',
	MEDIA_ALT VARCHAR(16777216) COMMENT 'Media Alt is ',
	CONTACT_KEY VARCHAR(16777216) COMMENT 'Contact Key is ID of the contact that owns the device that received the message but not the ID of the device; one contact could have multiple devices. The ContactKey matches the Contacts and Devices ID. The ContactKey indicates the subscriber associated with the device when the report is run',
	REQUEST_ID VARCHAR(16777216) COMMENT 'Request Id is Marketing Clouds unique identifier for every request',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_PUSH_MESSAGE_DETAIL_EPSILON unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, CONTACT_KEY, MESSAGE_ID, PUSH_JOB_ID, DEVICE_ID, SERVICE_TRANSACTION_ID)
)COMMENT='CRM Push Message Detail contains information about push notification interactions. For SFMC data there does not appear to be a Primary Key. For Epsilon data the �Unique Key: Contact_key + message_id + push_job_id + device_id.'
;
create TABLE IF NOT EXISTS CRM_PUSH_SEND_LOG (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	PUSH_JOB_ID VARCHAR(16777216) COMMENT 'Push Job Id is the ID of the job, or bundle of send, that included the push notification ',
	PUSH_TRIGGERED_SEND_REQUEST_ID VARCHAR(16777216) COMMENT 'Push Triggered Send Request Id is TokenID that is returned from the API call. When you use list and data extension sends, is the same as the PushJobID ',
	PUSH_BATCH_ID NUMBER(38,0) COMMENT 'Push Batch Id is The ID of the batch, or bundle of jobs, for batched sends',
	SUB_ID NUMBER(38,0) COMMENT 'Sub Id is the ID of the subscriber to whom the message was sent',
	DEVICE_ID VARCHAR(16777216) COMMENT 'Device Id is the ID of the device that received the push notification',
	APP_ID VARCHAR(16777216) COMMENT 'App Id is The ID of the app that received the push notification ',
	LOG_DATE TIMESTAMP_NTZ(9) COMMENT 'Log Date is The date that this data was written to the send log ',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_PUSH_SEND_LOG unique (PUSH_JOB_ID, PUSH_TRIGGERED_SEND_REQUEST_ID, PUSH_BATCH_ID, SUB_ID, DEVICE_ID, APP_ID, BRAND_ID, SOURCE_SYSTEM_NM)
)COMMENT='CRM Push Send Log lists all the push notifications that occur to customers by day.'
;
create TABLE IF NOT EXISTS CRM_SEND_JOB (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	FROM_NAME VARCHAR(16777216) COMMENT 'Name of the overarching category sending the email',
	FROM_EMAIL VARCHAR(16777216) COMMENT 'Actual email address sending the email',
	SCHED_TIME TIMESTAMP_NTZ(9) COMMENT 'Time the email was scheduled to send',
	SUBJECT_LINE VARCHAR(16777216) COMMENT 'Subject line of the sent email',
	EMAIL_NAME VARCHAR(16777216) COMMENT 'Category of email sent',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'An ID used by external partners use to identify the data source',
	SEND_DEFINITION_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'External key for the type of send going out. ',
	JOB_STATUS VARCHAR(16777216) COMMENT 'Status of the job:,Scheduled,Sending,Completed,Stopped,Canceled,Error,Deleted,PostSendCallout,New',
	PREVIEW_URL VARCHAR(16777216) COMMENT 'Preview of URL in the email',
	IS_MULTIPART BOOLEAN COMMENT 'True/False for whether or not the job has multiple parts.',
	ADDITIONAL VARCHAR(16777216) COMMENT 'All values currently null',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_SEND_JOB unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID)
)COMMENT='CRM_SEND_JOB is list of all jobs created to send emails (including to whom).'
;
create TABLE IF NOT EXISTS CRM_SEND_LOG (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber.',
	PROFILE_ID VARCHAR(16777216) COMMENT 'The unique ID associated with a profile',
	EXTERNAL_ID VARCHAR(16777216) COMMENT '''''',
	CUSTOMER_ID VARCHAR(16777216) COMMENT '''''',
	OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	CREATIVE_VARIANT VARCHAR(16777216) COMMENT 'Identifies which content blocks and images were used in an email',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the offer is sent',
	OFFER_CODE VARCHAR(16777216) COMMENT 'Offer code sent out in the job',
	SUB_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	TRIGGERED_SEND_ID VARCHAR(16777216) COMMENT 'Automatically created by Salesforce when sends go out',
	ERROR_CODE NUMBER(38,0) COMMENT 'Code for error, if any',
	DATASOURCE_NAME VARCHAR(16777216) COMMENT 'Which data source the email recipients are coming from.  For example, Epsilon or Harmony.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	OFFER_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	PARENT_OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_STEP VARCHAR(16777216) COMMENT 'All values currently null',
	STRENGTH_OF_CUSTOMER VARCHAR(16777216) COMMENT 'The strength of customer segment based on buying habits.',
	USER_DEFINED_SEGMENT_1 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_2 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_3 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_4 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_5 VARCHAR(16777216) COMMENT 'All values currently null',
	OFFER_DECISION_LOGIC VARCHAR(16777216) COMMENT 'OFFER_DECISION_LOGIC',
	POINT_BALANCE NUMBER(38,0) COMMENT '''''',
	EMAIL_NAME VARCHAR(16777216) COMMENT 'Description of the email sent out (NOTE: I do not think the column should be called email_address)',
	CAMPAIGN_ID VARCHAR(16777216) COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_NAME VARCHAR(16777216) COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_TYPE VARCHAR(16777216) COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_CATEGORY VARCHAR(16777216) COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESCRIPTION VARCHAR(16777216) COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE VARCHAR(16777216) COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION VARCHAR(16777216) COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint CRMS_CREATIVE_VARIANT_TO_CRM_SEND_LOG foreign key (BRAND_ID, SOURCE_SYSTEM_NM, OFFER_ID) references CRM_CREATIVE_VARIANT(BRAND_ID,SOURCE_SYSTEM_NM,OFFER_ID),
	constraint CRMS_SEND_JOB_TO_CRM_SEND_LOG foreign key (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID) references CRM_SEND_JOB(BRAND_ID,SOURCE_SYSTEM_NM,ACCOUNT_ID,JOB_ID),
	constraint XAK1CRM_SEND_LOG unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, PROFILE_ID, EXTERNAL_ID, CUSTOMER_ID)
)COMMENT='CRM_SEND_LOG is record of every email sent, the offer it contained, and what customer it was sent to.'
;
create TABLE IF NOT EXISTS CRM_SEND_LOG_BCKP0819 (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber.',
	PROFILE_ID VARCHAR(16777216) COMMENT 'The unique ID associated with a profile',
	EXTERNAL_ID VARCHAR(16777216) COMMENT '''''',
	CUSTOMER_ID VARCHAR(16777216) COMMENT '''''',
	OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	CREATIVE_VARIANT VARCHAR(16777216) COMMENT 'Identifies which content blocks and images were used in an email',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the offer is sent',
	OFFER_CODE VARCHAR(16777216) COMMENT 'Offer code sent out in the job',
	SUB_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	TRIGGERED_SEND_ID VARCHAR(16777216) COMMENT 'Automatically created by Salesforce when sends go out',
	ERROR_CODE NUMBER(38,0) COMMENT 'Code for error, if any',
	DATASOURCE_NAME VARCHAR(16777216) COMMENT 'Which data source the email recipients are coming from.  For example, Epsilon or Harmony.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	OFFER_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	PARENT_OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_STEP VARCHAR(16777216) COMMENT 'All values currently null',
	STRENGTH_OF_CUSTOMER VARCHAR(16777216) COMMENT 'The strength of customer segment based on buying habits.',
	USER_DEFINED_SEGMENT_1 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_2 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_3 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_4 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_5 VARCHAR(16777216) COMMENT 'All values currently null',
	OFFER_DECISION_LOGIC VARCHAR(16777216) COMMENT 'OFFER_DECISION_LOGIC',
	POINT_BALANCE NUMBER(38,0) COMMENT '''''',
	EMAIL_NAME VARCHAR(16777216) COMMENT 'Description of the email sent out (NOTE: I do not think the column should be called email_address)',
	CAMPAIGN_ID VARCHAR(16777216) COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_NAME VARCHAR(16777216) COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_TYPE VARCHAR(16777216) COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_CATEGORY VARCHAR(16777216) COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESCRIPTION VARCHAR(16777216) COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE VARCHAR(16777216) COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION VARCHAR(16777216) COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint CRMS_CREATIVE_VARIANT_TO_CRM_SEND_LOG foreign key (BRAND_ID, SOURCE_SYSTEM_NM, OFFER_ID) references CRM_CREATIVE_VARIANT(BRAND_ID,SOURCE_SYSTEM_NM,OFFER_ID),
	constraint CRMS_SEND_JOB_TO_CRM_SEND_LOG foreign key (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID) references CRM_SEND_JOB(BRAND_ID,SOURCE_SYSTEM_NM,ACCOUNT_ID,JOB_ID),
	constraint XAK1CRM_SEND_LOG unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, PROFILE_ID, EXTERNAL_ID, CUSTOMER_ID)
)COMMENT='CRM_SEND_LOG is record of every email sent, the offer it contained, and what customer it was sent to.'
;
create TABLE IF NOT EXISTS CRM_SEND_LOG_BKUP20211123 (
	BRAND_ID VARCHAR(20),
	SOURCE_SYSTEM_NM VARCHAR(255),
	ACCOUNT_ID NUMBER(38,0),
	JOB_ID VARCHAR(16777216),
	SUBSCRIBER_KEY VARCHAR(16777216),
	PROFILE_ID VARCHAR(16777216),
	EXTERNAL_ID VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	MESSAGE_ID VARCHAR(16777216),
	SERVICE_TRANSACTION_ID VARCHAR(16777216),
	OFFER_ID VARCHAR(16777216),
	BATCH_ID NUMBER(38,0),
	LIST_ID NUMBER(38,0),
	CREATIVE_VARIANT VARCHAR(16777216),
	EVENT_DATE TIMESTAMP_NTZ(9),
	OFFER_CODE VARCHAR(16777216),
	SUB_ID NUMBER(38,0),
	TRIGGERED_SEND_ID VARCHAR(16777216),
	ERROR_CODE NUMBER(38,0),
	DATASOURCE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	OFFER_NAME VARCHAR(16777216),
	PARENT_OFFER_ID VARCHAR(16777216),
	JOURNEY_NAME VARCHAR(16777216),
	JOURNEY_STEP VARCHAR(16777216),
	STRENGTH_OF_CUSTOMER VARCHAR(16777216),
	USER_DEFINED_SEGMENT_1 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_2 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_3 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_4 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_5 VARCHAR(16777216),
	OFFER_DECISION_LOGIC VARCHAR(16777216),
	POINT_BALANCE NUMBER(38,0),
	EMAIL_NAME VARCHAR(16777216),
	CAMPAIGN_ID VARCHAR(16777216),
	CAMPAIGN_NAME VARCHAR(16777216),
	CAMPAIGN_TYPE VARCHAR(16777216),
	CAMPAIGN_CATEGORY VARCHAR(16777216),
	CAMPAIGN_DESCRIPTION VARCHAR(16777216),
	CAMPAIGN_OBJECTIVE VARCHAR(16777216),
	CAMPAIGN_START_DATE DATE,
	CAMPAIGN_END_DATE DATE,
	CAMPAIGN_DURATION VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS CRM_SEND_LOG_BKUP20220829 (
	BRAND_ID VARCHAR(20) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber.',
	PROFILE_ID VARCHAR(16777216) COMMENT 'The unique ID associated with a profile',
	EXTERNAL_ID VARCHAR(16777216) COMMENT '''''',
	CUSTOMER_ID VARCHAR(16777216) COMMENT '''''',
	OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	CREATIVE_VARIANT VARCHAR(16777216) COMMENT 'Identifies which content blocks and images were used in an email',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the offer is sent',
	OFFER_CODE VARCHAR(16777216) COMMENT 'Offer code sent out in the job',
	SUB_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	TRIGGERED_SEND_ID VARCHAR(16777216) COMMENT 'Automatically created by Salesforce when sends go out',
	ERROR_CODE NUMBER(38,0) COMMENT 'Code for error, if any',
	DATASOURCE_NAME VARCHAR(16777216) COMMENT 'Which data source the email recipients are coming from.  For example, Epsilon or Harmony.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	OFFER_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	PARENT_OFFER_ID VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_NAME VARCHAR(16777216) COMMENT 'All values currently null',
	JOURNEY_STEP VARCHAR(16777216) COMMENT 'All values currently null',
	STRENGTH_OF_CUSTOMER VARCHAR(16777216) COMMENT 'The strength of customer segment based on buying habits.',
	USER_DEFINED_SEGMENT_1 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_2 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_3 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_4 VARCHAR(16777216) COMMENT 'All values currently null',
	USER_DEFINED_SEGMENT_5 VARCHAR(16777216) COMMENT 'All values currently null',
	OFFER_DECISION_LOGIC VARCHAR(16777216) COMMENT 'OFFER_DECISION_LOGIC',
	POINT_BALANCE NUMBER(38,0) COMMENT '''''',
	EMAIL_NAME VARCHAR(16777216) COMMENT 'Description of the email sent out (NOTE: I do not think the column should be called email_address)',
	CAMPAIGN_ID VARCHAR(16777216) COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_NAME VARCHAR(16777216) COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_TYPE VARCHAR(16777216) COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_CATEGORY VARCHAR(16777216) COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESCRIPTION VARCHAR(16777216) COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE VARCHAR(16777216) COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE TIMESTAMP_NTZ(9) COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION VARCHAR(16777216) COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint CRMS_CREATIVE_VARIANT_TO_CRM_SEND_LOG foreign key (BRAND_ID, SOURCE_SYSTEM_NM, OFFER_ID) references CRM_CREATIVE_VARIANT(BRAND_ID,SOURCE_SYSTEM_NM,OFFER_ID),
	constraint CRMS_SEND_JOB_TO_CRM_SEND_LOG foreign key (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID) references CRM_SEND_JOB(BRAND_ID,SOURCE_SYSTEM_NM,ACCOUNT_ID,JOB_ID),
	constraint XAK1CRM_SEND_LOG unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, PROFILE_ID, EXTERNAL_ID, CUSTOMER_ID)
)COMMENT='CRM_SEND_LOG is record of every email sent, the offer it contained, and what customer it was sent to.'
;
create TABLE IF NOT EXISTS CRM_SENT (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) NOT NULL COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) NOT NULL COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID NUMBER(38,0) COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'The date the email was sent',
	EVENT_TYPE VARCHAR(16777216) COMMENT 'Event type = sent email',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT 'The TriggeredSendExternalKey text used for any batches used in the send',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_SENT unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, MESSAGE_ID, SERVICE_TRANSACTION_ID)
)COMMENT='CRM_SENT contains a list of emails that were sent successfully.'
;
create TABLE IF NOT EXISTS CRM_STATUS_CHANGE (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) NOT NULL COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	SUBSCRIBER_KEY VARCHAR(16777216) NOT NULL COMMENT 'The subscriber key for the affected subscriber',
	OLD_STATUS VARCHAR(16777216) NOT NULL COMMENT 'The old status of affected subscribers',
	NEW_STATUS VARCHAR(16777216) NOT NULL COMMENT 'The new status of affected subscribers',
	DATE_CHANGED TIMESTAMP_NTZ(9) NOT NULL COMMENT 'The date the status changed',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_STATUS_CHANGE unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, SUBSCRIBER_KEY, OLD_STATUS, NEW_STATUS, DATE_CHANGED, SERVICE_TRANSACTION_ID),
	constraint XPKCRM_STATUS_CHANGE primary key (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, SUBSCRIBER_KEY, OLD_STATUS, NEW_STATUS, DATE_CHANGED)
)COMMENT='CRM_STATUS_CHANGE contains list of  status changes per user.'
;
create TABLE IF NOT EXISTS CRM_UNSUBSCRIBE (
	BRAND_ID VARCHAR(20) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID NUMBER(38,0) NOT NULL COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID VARCHAR(16777216) COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY VARCHAR(16777216) COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID VARCHAR(16777216) COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID NUMBER(38,0) COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID NUMBER(38,0) COMMENT '''''',
	EVENT_DATE TIMESTAMP_NTZ(9) COMMENT 'Date click event occurred',
	EVENT_TYPE VARCHAR(16777216) COMMENT '''''',
	BATCH_ID NUMBER(38,0) COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY VARCHAR(16777216) COMMENT '''''',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME VARCHAR(16777216) COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1CRM_UNSUBSCRIBE unique (BRAND_ID, SOURCE_SYSTEM_NM, ACCOUNT_ID, JOB_ID, SUBSCRIBER_KEY, MESSAGE_ID, SERVICE_TRANSACTION_ID)
)COMMENT='CRM_UNSUBSCRIBE list of users who unsubscribed from email communication.'
;
create TABLE IF NOT EXISTS CUSTOMER_CHANNEL_STATUS (
	MEMBER_ID VARCHAR(16777216) NOT NULL COMMENT 'Unique ID for the Customer/Member',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'The Brand Identifier',
	CHANNEL_STATUS_TYPE_CD VARCHAR(16777216) NOT NULL COMMENT 'The Code value for the Channel',
	CHANNEL_STATUS_EXPIRY_DATE DATE NOT NULL COMMENT 'This is the Date that the Customer Channel Status Becomes ineffective',
	CHANNEL_STATUS_EFFECTIVE_DATE DATE COMMENT 'The Date the Customer Channel Status goes into effect',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) COMMENT 'The POS System used to load the transaction  information\n',
	LOAD_TYPE VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKCUSTOMER_CHANNEL_STATUS primary key (MEMBER_ID, BRAND_ID, CHANNEL_STATUS_TYPE_CD, CHANNEL_STATUS_EXPIRY_DATE)
);
create TABLE IF NOT EXISTS EXCEPTION (
	EXCEPTION_ID VARCHAR(16777216) NOT NULL COMMENT 'Unique identifier for an exception',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	EXCEPTION_TYP_CD VARCHAR(16777216) COMMENT 'This is the code that represents the Exception Type. Sample data is MM.',
	EXCEPTION_DOMAIN_TYPE VARCHAR(16777216) COMMENT 'Types of Domain- Cust,Media etc',
	TRANSACTION_ID VARCHAR(16777216) COMMENT 'The transaction identifier for that unique record that has the exception',
	EXCEPTION_DT DATE COMMENT 'Tha date of the exception',
	TABLE_NAME VARCHAR(16777216) COMMENT 'Name of database table that had the exception',
	COLUMN_NAME VARCHAR(16777216) COMMENT 'Name of database table column that had the exception',
	COLUMN_VAL VARCHAR(16777216) COMMENT 'database table column value that contains the exception',
	TABLE_PRIMARY_KEY VARCHAR(16777216) COMMENT 'primary key of the table where the exception occured',
	EXCEPTION_REASON_TXT VARCHAR(16777216) COMMENT 'Reason for the exception',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_TYPE VARCHAR(16777216) COMMENT 'The load stratedy. Daily vs one-time',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKEXCEPTION primary key (EXCEPTION_ID, BRAND_ID)
);
create TABLE IF NOT EXISTS EXCEPTION_DOMAIN_TYPE (
	EXCEPTION_DOMAIN_TYPE VARCHAR(16777216) NOT NULL COMMENT 'Types of Domain- Cust,Media etc',
	EXCEPTION_DOMAIN_TYPE_NAME VARCHAR(16777216) COMMENT 'Domain Types Name - Customer,Media etc',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_TYPE VARCHAR(16777216) COMMENT 'The load stratedy. Daily vs one-time',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKEXCEPTION_DOMAIN_TYPE primary key (EXCEPTION_DOMAIN_TYPE)
);
create TABLE IF NOT EXISTS EXCEPTION_RESOLUTION (
	EXCEPTION_RESOLUTION_ID VARCHAR(16777216) NOT NULL COMMENT 'Unique identifier generated per exception resolution',
	EXCEPTION_ID VARCHAR(16777216) COMMENT 'Unique identifier for an exception',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	EXCEPTION_RESOLUTION_DT DATE COMMENT 'The exception resolution date of the exception',
	EXCEPTION_RESOLUTION_PARTY_NAME VARCHAR(16777216) COMMENT 'The exception resolution party name',
	EXCEPTION_RESOLUTION_COMMENT VARCHAR(16777216) COMMENT 'The comment on the resolution ',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_TYPE VARCHAR(16777216) COMMENT 'The load stratedy. Daily vs one-time',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKEXCEPTION_RESOLUTION primary key (EXCEPTION_RESOLUTION_ID)
);
create TABLE IF NOT EXISTS EXCEPTION_TYPE (
	EXCEPTION_TYP_CD VARCHAR(16777216) NOT NULL COMMENT 'This is the code that represents the Exception Type. Sample data is MM.',
	EXCEPTION_TYP_DESC VARCHAR(16777216) COMMENT 'This is the description of the code that represents the Exception Type. Sample data is MM- Mismatched Amount',
	EXCEPTION_PRIORITY_NBR VARCHAR(16777216) COMMENT 'The priosity number for the exception',
	EXCEPTION_OWNER_NAME VARCHAR(16777216) COMMENT 'The owner of the exception',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_TYPE VARCHAR(16777216) COMMENT 'The load stratedy. Daily vs one-time',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKEXCEPTION_TYPE primary key (EXCEPTION_TYP_CD)
)COMMENT='This Table contains the standard values for the exception types'
;
create TABLE IF NOT EXISTS FASHION (
	YEAR NUMBER(4,0),
	FASHION_COLOR VARCHAR(6)
);
create TABLE IF NOT EXISTS FAVORITE_YEARS (
	YEAR NUMBER(4,0),
	D VARCHAR(8)
);
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
create TABLE IF NOT EXISTS GUEST_EXPERIENCE_SURVEY_RESPONSE_ANSWER (
	SURVEY_RESPONSE_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Response Identifier uniquely identifies an individuals response to a set of questions.',
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_QUESTION_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	SURVEY_QUESTION_CHOICE_ID NUMBER(38,0) COMMENT 'Survey Question Choice Identifier uniquely an option for a quesiton within a survey.',
	SURVEY_RESPONSE_ANSWER_VAL VARCHAR(16777216) COMMENT 'Survey Response Answer contains values that are not populated by predfined selections. For example, 01/02/2021 or 7PM.',
	SOURCE_SURVEY_RESPONSE_VAL VARCHAR(16777216) COMMENT 'Source Survey Answer Value is the answer from the survey in raw form.',
	SOURCE_SURVEY_QUESTION_ID VARCHAR(16777216) COMMENT 'Source Survey Question Identifier is the question id that is on the existing survey in order to tie back answers.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XAK1GUEST_EXPERIENCE_SURVEY_RESPONSE_ANSWER unique (SURVEY_RESPONSE_ID, SURVEY_ID, SURVEY_QUESTION_ID, SURVEY_QUESTION_CHOICE_ID),
	constraint GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER_TO_GUEST_EXPERIENCE_SURVEY_RESPONSE_ANSWER foreign key (SURVEY_RESPONSE_ID) references GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER(SURVEY_RESPONSE_ID),
	constraint SURVEY_QUESTION_CHOICE_TO_GUEST_EXPERIENCE_SURVEY_RESPONSE_ANSWER foreign key (SURVEY_ID, SURVEY_QUESTION_ID, SURVEY_QUESTION_CHOICE_ID) references IDS_DEV.INT_REF.SURVEY_QUESTION_CHOICE(SURVEY_ID,SURVEY_QUESTION_ID,SURVEY_QUESTION_CHOICE_ID)
)COMMENT='Guest Experience Survey Response Answer are the answeres provided by the customer for a guest experience survey and mapped to Inspire\"s standard set of questions and attributes.'
;
create TABLE IF NOT EXISTS GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER (
	SURVEY_RESPONSE_ID NUMBER(38,0) NOT NULL autoincrement COMMENT 'Survey Response Identifier uniquely identifies an individuals response to a set of questions.',
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SOURCE_SURVEY_RESPONSE_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Response Identifier uniquley identifies an individuals response from the system the survey originated from. For example, R_1lsCMU9Besc5hy6.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	DAY_PART_NM VARCHAR(16777216) COMMENT 'Day Part Name is label for a set portion of the day.  For example, Breakfast, Lunch, Mid-Day.',
	SURVEY_CHANNEL_ID NUMBER(38,0) COMMENT 'Survey Channel Identifier uniquely identifies a survey channel.',
	SURVEY_ORDER_PLACEMENT_ID NUMBER(38,0) COMMENT 'Survey Order Placement Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_FULFILLMENT_ID NUMBER(38,0) COMMENT 'Survey Order Channel Identifier uniquely identifies how an order was placed at a store.',
	STORE_ID VARCHAR(16777216) COMMENT 'Store Identifier uniquely identifies a store within a brand.',
	BUSINESS_DT DATE COMMENT 'Business Date is the day in which the transaction occured.',
	SURVEY_DTTM TIMESTAMP_NTZ(9) COMMENT 'Survey Datetime is the time the customer survey was recieved.',
	LOYALTY_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Loyalty Transaction Identifier is the transaction ID for loyalty digital transactions.',
	DIGITAL_ORDER_ID VARCHAR(16777216) COMMENT 'Digital Order Identifier uniquely specifies an e-commerce order.',
	POS_ORDER_ID VARCHAR(16777216) COMMENT 'Pos Order Identifier is used to link a digitial order to the pos within a store. Sample Value: 3730.',
	LOYALTY_GUEST_NBR VARCHAR(16777216) COMMENT 'Loyalty Guest Number',
	OVERALL_SATISFACTION_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Cleanliness Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_ACCURACY_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Accuracy Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfacton Food Taste Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Friendliness Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Service Speed Score Number. Valid values 1 through 5.',
	CUST_RETURN_LIKELY_SCORE_NBR NUMBER(38,0) COMMENT 'Customer Return Likely Score Number. Valid values 1 through 5. ',
	CUST_LIKE_CONTACT_IND BOOLEAN COMMENT 'Customer Like Contact Indicator specifies that the customer would like to be folowed up with.',
	SOURCE_SURVEY_ID VARCHAR(16777216) COMMENT 'Source Survey Identifier is the unique identifier for a set of questions from the system the responses are sent from.',
	SOURCE_SURVEY_CHANNEL_CD VARCHAR(16777216) COMMENT 'Survey Channel Code is how the survey is received by the individual. This value comes with the survey response. For example,  Anonymous or email. Anonymous is often associated with surveys that originated from a QR code or receipt.',
	SOURCE_SURVEY_SUB_CHANNEL_CD VARCHAR(16777216) COMMENT 'Source Survey Sub Channel further refines the source of the survey channell and is the value that come with the survey response. Sample Values Email Invite, Email Invite - ecommerce, Receipt, and QR Code.',
	USER_LANGUAGE_CD VARCHAR(16777216) COMMENT 'User Language Code specifies the language of the responder of the survey.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKGUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER primary key (SURVEY_RESPONSE_ID),
	constraint XAK1GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER unique (SOURCE_SURVEY_RESPONSE_ID, SOURCE_SYSTEM_NM, BRAND_ID),
	constraint R_528 foreign key (SURVEY_ID) references IDS_DEV.INT_REF.SURVEY(SURVEY_ID),
	constraint R_541 foreign key (SURVEY_CHANNEL_ID) references IDS_DEV.INT_REF.SURVEY_CHANNEL(SURVEY_CHANNEL_ID),
	constraint R_554 foreign key (SURVEY_ORDER_PLACEMENT_ID) references IDS_DEV.INT_REF.SURVEY_ORDER_PLACEMENT(SURVEY_ORDER_PLACEMENT_ID),
	constraint R_555 foreign key (SURVEY_ORDER_FULFILLMENT_ID) references IDS_DEV.INT_REF.SURVEY_ORDER_FULFILLMENT(SURVEY_ORDER_FULFILLMENT_ID)
)COMMENT='Guest Experience Survey Response Header is the response from a customer to a set of questions based on their satisfaction and experience at an establishment and mapped to Inspires standard set of questions and attributes.'
;
create TABLE IF NOT EXISTS GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER_TEST (
	SURVEY_RESPONSE_ID NUMBER(38,0) NOT NULL autoincrement COMMENT 'Survey Response Identifier uniquely identifies an individuals response to a set of questions.',
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SOURCE_SURVEY_RESPONSE_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Response Identifier uniquley identifies an individuals response from the system the survey originated from. For example, R_1lsCMU9Besc5hy6.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	DAY_PART_NM VARCHAR(16777216) COMMENT 'Day Part Name is label for a set portion of the day.  For example, Breakfast, Lunch, Mid-Day.',
	SURVEY_CHANNEL_ID NUMBER(38,0) COMMENT 'Survey Channel Identifier uniquely identifies a survey channel.',
	SURVEY_ORDER_PLACEMENT_ID NUMBER(38,0) COMMENT 'Survey Order Placement Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_FULFILLMENT_ID NUMBER(38,0) COMMENT 'Survey Order Channel Identifier uniquely identifies how an order was placed at a store.',
	STORE_ID VARCHAR(16777216) COMMENT 'Store Identifier uniquely identifies a store within a brand.',
	BUSINESS_DT DATE COMMENT 'Business Date is the day in which the transaction occured.',
	SURVEY_DTTM TIMESTAMP_NTZ(9) COMMENT 'Survey Datetime is the time the customer survey was recieved.',
	LOYALTY_TRANSACTION_ID VARCHAR(16777216) COMMENT 'Loyalty Transaction Identifier is the transaction ID for loyalty digital transactions.',
	DIGITAL_ORDER_ID VARCHAR(16777216) COMMENT 'Digital Order Identifier uniquely specifies an e-commerce order.',
	POS_ORDER_ID VARCHAR(16777216) COMMENT 'Pos Order Identifier is used to link a digitial order to the pos within a store. Sample Value: 3730.',
	LOYALTY_GUEST_NBR VARCHAR(16777216) COMMENT 'Loyalty Guest Number',
	OVERALL_SATISFACTION_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Cleanliness Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_ACCURACY_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Accuracy Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfacton Food Taste Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Friendliness Score Number. Valid values 1 through 5.',
	OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR NUMBER(38,0) COMMENT 'Overall Satisfaction Service Speed Score Number. Valid values 1 through 5.',
	CUST_RETURN_LIKELY_SCORE_NBR NUMBER(38,0) COMMENT 'Customer Return Likely Score Number. Valid values 1 through 5. ',
	CUST_LIKE_CONTACT_IND VARCHAR(16777216) COMMENT 'Customer Like Contact Indicator specifies that the customer would like to be folowed up with.',
	SOURCE_SURVEY_ID VARCHAR(16777216) COMMENT 'Source Survey Identifier is the unique identifier for a set of questions from the system the responses are sent from.',
	SOURCE_SURVEY_CHANNEL_CD VARCHAR(16777216) COMMENT 'Survey Channel Code is how the survey is received by the individual. This value comes with the survey response. For example,  Anonymous or email. Anonymous is often associated with surveys that originated from a QR code or receipt.',
	SOURCE_SURVEY_SUB_CHANNEL_CD VARCHAR(16777216) COMMENT 'Source Survey Sub Channel further refines the source of the survey channell and is the value that come with the survey response. Sample Values Email Invite, Email Invite - ecommerce, Receipt, and QR Code.',
	USER_LANGUAGE_CD VARCHAR(16777216) COMMENT 'User Language Code specifies the language of the responder of the survey.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XAK1GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER unique (SOURCE_SURVEY_RESPONSE_ID, SOURCE_SYSTEM_NM, BRAND_ID),
	constraint XPKGUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER primary key (SURVEY_RESPONSE_ID),
	constraint R_528 foreign key (SURVEY_ID) references IDS_DEV.INT_REF.SURVEY(SURVEY_ID),
	constraint R_541 foreign key (SURVEY_CHANNEL_ID) references IDS_DEV.INT_REF.SURVEY_CHANNEL(SURVEY_CHANNEL_ID),
	constraint R_554 foreign key (SURVEY_ORDER_PLACEMENT_ID) references IDS_DEV.INT_REF.SURVEY_ORDER_PLACEMENT(SURVEY_ORDER_PLACEMENT_ID),
	constraint R_555 foreign key (SURVEY_ORDER_FULFILLMENT_ID) references IDS_DEV.INT_REF.SURVEY_ORDER_FULFILLMENT(SURVEY_ORDER_FULFILLMENT_ID)
)COMMENT='Guest Experience Survey Response Header is the response from a customer to a set of questions based on their satisfaction and experience at an establishment and mapped to Inspires standard set of questions and attributes.'
;
create TABLE IF NOT EXISTS INSPIRE_DAILY_PRODUCT_OFFER_SALES_TBL (
	BUSINESS_DT DATE,
	BRAND VARCHAR(16777216),
	STORE_NBR VARCHAR(16777216),
	DMA_CD VARCHAR(16777216),
	DMA_NM VARCHAR(16777216),
	L1NAME VARCHAR(16777216),
	L2NAME VARCHAR(16777216),
	L3NAME VARCHAR(16777216),
	L4NAME VARCHAR(16777216),
	L5NAME VARCHAR(16777216),
	OWNERSHIP_TYP VARCHAR(16777216),
	STATE VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	FISC_YEAR_NBR NUMBER(38,0),
	FISC_QUARTER_NBR NUMBER(38,0),
	FISC_PERIOD_NBR NUMBER(38,0),
	FISC_WEEK_NBR NUMBER(38,0),
	DAY_OF_WEEK VARCHAR(16777216),
	HOLIDAY_IND_CY BOOLEAN,
	HOLIDAY_NM_CY VARCHAR(16777216),
	MENU_ITEM_ID NUMBER(38,0),
	MENU_ITEM_TYP VARCHAR(16777216),
	MENU_ITEM_NM VARCHAR(16777216),
	MDM_PRODUCT_ID NUMBER(38,0),
	MDM_PRODUCT_NM VARCHAR(16777216),
	MDM_OFFER_ID NUMBER(38,0),
	MDM_OFFER_NM VARCHAR(16777216),
	PRODUCT_SALES_CY NUMBER(38,2),
	DISCOUNT_AMT_CY NUMBER(38,2),
	MENU_ITEM_CNT_CY NUMBER(38,2),
	CHECK_SALES_CY NUMBER(38,2),
	CHECK_CNT_CY NUMBER(38,2),
	REST_SALES_CY NUMBER(38,2),
	REST_TRANS_CY NUMBER(38,2),
	FISC_DATE_LY DATE,
	FISC_HOLIDAY_IND_LY BOOLEAN,
	FISC_HOLIDAY_NM_LY VARCHAR(16777216),
	FISC_PRODUCT_SALES_LY NUMBER(38,2),
	FISC_DISCOUNT_AMT_LY NUMBER(38,2),
	FISC_MENU_ITEM_CNT_LY NUMBER(38,2),
	FISC_CHECK_SALES_LY NUMBER(38,2),
	FISC_CHECK_CNT_LY NUMBER(38,2),
	FISC_REST_SALES_LY NUMBER(38,2),
	FISC_REST_TRANS_LY NUMBER(38,2),
	CAL_COMP_DATE_LY DATE,
	CAL_HOLIDAY_IND_LY BOOLEAN,
	CAL_HOLIDAY_NM_LY VARCHAR(16777216),
	CAL_PRODUCT_SALES_LY NUMBER(38,2),
	CAL_DISCOUNT_AMT_LY NUMBER(38,2),
	CAL_MENU_ITEM_CNT_LY NUMBER(38,2),
	CAL_CHECK_SALES_LY NUMBER(38,2),
	CAL_CHECK_CNT_LY NUMBER(38,2),
	CAL_REST_SALES_LY NUMBER(38,2),
	CAL_REST_TRANS_LY NUMBER(38,2),
	PRODUCT_HIERARCHY_ID NUMBER(38,0),
	LEVEL5_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL4_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL3_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL2_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL1_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	OFFER_HIERARCHY_ID NUMBER(38,0),
	LEVEL5_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL4_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL3_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL2_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL1_OFFER_HIERARCHY_NM VARCHAR(16777216),
	PRODUCT_MISC1 VARCHAR(16777216),
	PRODUCT_MISC2 VARCHAR(16777216),
	PRODUCT_MISC3 VARCHAR(16777216),
	PRODUCT_MISC4 VARCHAR(16777216),
	PRODUCT_MISC5 VARCHAR(16777216),
	PRODUCT_MISC6 VARCHAR(16777216),
	PRODUCT_MISC7 VARCHAR(16777216),
	PRODUCT_MISC8 VARCHAR(16777216),
	PRODUCT_MISC9 VARCHAR(16777216),
	PRODUCT_MISC10 VARCHAR(16777216),
	OFFER_MISC1 VARCHAR(16777216),
	OFFER_MISC2 VARCHAR(16777216),
	OFFER_MISC3 VARCHAR(16777216),
	OFFER_MISC4 VARCHAR(16777216),
	OFFER_MISC5 VARCHAR(16777216),
	OFFER_MISC6 VARCHAR(16777216),
	OFFER_MISC7 VARCHAR(16777216),
	OFFER_MISC8 VARCHAR(16777216),
	OFFER_MISC9 VARCHAR(16777216),
	OFFER_MISC10 VARCHAR(16777216)
);
create TABLE IF NOT EXISTS LOYALTY_MEMBER_TIER (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand associated with store\n',
	MEMBER_ID VARCHAR(16777216) NOT NULL COMMENT 'Member Identifier is the Loyalty Member Identifier.',
	LOYALTY_TIER_EFFECTIVE_END_DATE DATE NOT NULL COMMENT 'Loyalty Tier Effective End Date is the date the loyalty tier status ended A 12/31/9999 value reflects the tier is currently in effect.',
	LOYALTY_TIER_EFFECTIVE_BEGIN_DATE DATE NOT NULL COMMENT 'Loyalty Tier Effective Begin Date is the date the new loyalty tier went into effect.',
	LOYALTY_TIER_NAME VARCHAR(16777216) NOT NULL COMMENT 'Loyalty Tier Name. Values for Dunkin'' are CLUBDUNKIN or CLUBDUNKINBOOSTED.',
	LOYALTY_TIER_STATUS_ACTION_NM VARCHAR(16777216) COMMENT 'Loyalty Tier Status Action Name is a label for a membership level change. Sample values are: Awarded, Extended, Expired, CSRDowngraded.',
	LOYALTY_TIER_REASON_DESC VARCHAR(16777216) COMMENT 'Loyalty Tier Reason Description describes the reason for a membership level change. Sample values are: CSR Tier Downgrade, CSR Tier Upgrade, Requalification Downgrade, Transaction Tier Extend, Transaction Tier Upgrade, CSR Tier Extension, Tier Conv BASE to CLUBDUNKIN.',
	CURRENT_IND BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Current Indicator specifies the latest record for a ',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) NOT NULL COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XAK1LOYALTY_MEMBER_TIER unique (BRAND_ID, MEMBER_ID, LOYALTY_TIER_EFFECTIVE_BEGIN_DATE),
	constraint XPKLOYALTY_MEMBER_TIER primary key (BRAND_ID, MEMBER_ID, LOYALTY_TIER_EFFECTIVE_END_DATE)
)COMMENT='Loyalty Member Tier contains the history of a members status within a loyalty program. \nFor Example, in Dunkn\nMembers start at base status (Club Dunkin).\nIf a member makes 12 transactions within 1 calendar month, they achieve Boosted status.\nOnce achieved, Boosted status is set to last for the rest of the current month + 3 calendar months (always expires at the end of a calendar month).\nAny time the member makes at least 12 transactions within a calendar month, their Boosted status is extended for an additional 3 months.\nIf the member doesn�t make at least 12 transactions in any month prior to their expiration date, status expires and they return to base Club Dunkin status.\n'
;
create or replace TRANSIENT TABLE OFFER_CODE_FIX_FULL (
	JOB_ID VARCHAR(16777216),
	LIST_ID NUMBER(38,0),
	BATCH_ID NUMBER(38,0),
	SUB_ID NUMBER(38,0),
	TRIGGERED_SEND_ID VARCHAR(16777216),
	ERROR_CODE NUMBER(38,0),
	EVENT_DATE VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	SUBSCRIBER_KEY VARCHAR(16777216),
	DATASOURCE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	OFFER_CODE VARCHAR(16777216),
	OFFER_NAME VARCHAR(16777216),
	PARENT_OFFER_ID VARCHAR(16777216),
	JOURNEY_NAME VARCHAR(16777216),
	JOURNEY_STEP VARCHAR(16777216),
	SOC VARCHAR(16777216),
	USER_DEFINED_SEGMENT_1 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_2 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_3 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_4 VARCHAR(16777216),
	USER_DEFINED_SEGMENT_5 VARCHAR(16777216),
	OFFER_DECISION_LOGIC VARCHAR(16777216),
	EMAIL_NAME VARCHAR(16777216),
	CREATIVE_VARIANT VARCHAR(16777216),
	CAMPAIGN_ID VARCHAR(16777216),
	CAMPAIGN_NAME VARCHAR(16777216),
	CAMPAIGN_TYPE VARCHAR(16777216),
	CAMPAIGN_CATEGORY VARCHAR(16777216),
	CAMPAIGN_DESCRIPTION VARCHAR(16777216),
	CAMPAIGN_OBJECTIVE VARCHAR(16777216),
	CAMPAIGN_START_DATE VARCHAR(16777216),
	CAMPAIGN_END_DATE VARCHAR(16777216),
	CAMPAIGN_DURATION VARCHAR(16777216),
	POINTBALANCE NUMBER(38,0),
	MEMBER_ID VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TEST_3 (
	NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	PHONENO NUMBER(38,0)
);
create TABLE IF NOT EXISTS XML_INGEST (
	XMLDATA VARIANT
);
CREATE FILE FORMAT IF NOT EXISTS XML_INGEST_FORMAT
	TYPE = XML
;
CREATE PROCEDURE IF NOT EXISTS BASE_VIEW_CREATION("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
        
    var select_table_names = `SELECT table_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_TYPE = ''BASE TABLE'' limit 3;`
    
    var select_column_names = `SELECT column_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_NAME = :3;`
           
    var select_table_names_stmt = snowflake.createStatement(
                {
                sqlText: select_table_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM]
                }
        );
 
    var table_names = select_table_names_stmt.execute();
        
    var table_name_array=[];
    while(table_names.next())
    {
        table_name_array.push(table_names.getColumnValue(1));
    }
    
    for (const table_name of table_name_array) {
    
        var select_column_names_stmt = snowflake.createStatement(
                {
                sqlText: select_column_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM, table_name]
                }
        );
        
        var column_names = select_column_names_stmt.execute();
        var column_name_array=[];
        while(column_names.next())
        {
            column_name_array.push(column_names.getColumnValue(1));
        }
        
        var source_tb_columns = column_name_array.join('', '');
        
        var view_template = `CREATE VIEW IF NOT EXISTS ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}_BV COPY GRANTS
                            AS SELECT
                               ${source_tb_columns}
                            FROM ` + SRC_DB_PARAM + `.` + SRC_SCHEMA_PARAM + `.${table_name};`
        var execute_base_view_stmt = snowflake.createStatement(
        {
            sqlText: view_template
        });
        execute_base_view_stmt.execute();
//        return view_template;
    }
    try {

        return view_template;
        }
    catch (err)  {
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS BASE_VIEW_CREATION("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216), "SRC_TB_PREFIXES_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    function prepareSelectTableNames(src_tb_prefixes) {
      
      var where_table_name_stmt = "";

      if (src_tb_prefixes) {
         var src_db_prefixes_array = src_tb_prefixes.split('','')

         if (src_db_prefixes_array.length > 1){
             where_table_name_stmt += ` AND TABLE_NAME LIKE ''${src_db_prefixes_array.shift()}%''`
             for (const prefix of src_db_prefixes_array) {
                 where_table_name_stmt += ` OR table_name LIKE ''${prefix}%''`;
             }
         } else {
             where_table_name_stmt += ` AND TABLE_NAME LIKE ''${SRC_TB_PREFIXES_PARAM}%''`;
         }
      }
      return `SELECT table_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_TYPE = ''BASE TABLE'' ${where_table_name_stmt};`;
    }    

    var select_table_names = prepareSelectTableNames(SRC_TB_PREFIXES_PARAM);
            
    var select_column_names = `SELECT column_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_NAME = :3;`
           
    var select_table_names_stmt = snowflake.createStatement(
                {
                sqlText: select_table_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM]
                }
        );
 
    var table_names = select_table_names_stmt.execute();
        
    var table_name_array=[];
    while(table_names.next())
    {
        table_name_array.push(table_names.getColumnValue(1));
    }
    
    for (const table_name of table_name_array) {
    
        var select_column_names_stmt = snowflake.createStatement(
                {
                sqlText: select_column_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM, table_name]
                }
        );
        
        var column_names = select_column_names_stmt.execute();
        var column_name_array=[];
        while(column_names.next())
        {
            column_name_array.push(column_names.getColumnValue(1));
        }
        
        var source_tb_columns = column_name_array.join('', '');
        
        var view_template = `CREATE VIEW IF NOT EXISTS ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}_BV COPY GRANTS
                            AS SELECT
                               ${source_tb_columns}
                            FROM ` + SRC_DB_PARAM + `.` + SRC_SCHEMA_PARAM + `."${table_name}";`
        var execute_base_view_stmt = snowflake.createStatement(
        {
            sqlText: view_template
        });
        execute_base_view_stmt.execute();
    }
    try {

        return `Base view are created in the database: ${DST_DB_PARAM} and schema: ${DST_SCHEMA_PARAM}`;
        }
    catch (err)  {
        throw err;
        }
    ';
CREATE FUNCTION IF NOT EXISTS FAVORITE_COLORS("THE_YEAR" NUMBER(38,0))
RETURNS TABLE ("COLOR" VARCHAR(16777216))
LANGUAGE SQL
AS 'select color from colors where year=the_year /*and favorite=true*/';
CREATE FUNCTION IF NOT EXISTS GUEST_EXPERIENCE_OSAT_INIT("AGG_LVL" VARCHAR(16777216))
RETURNS TABLE ("BRAND_ID" VARCHAR(16777216), "STORE_ID" VARCHAR(16777216), "BUSINESS_DT" DATE, "DAY_PART_NM" VARCHAR(16777216), "SURVEY_ORDER_FULFILLMENT_ID" NUMBER(38,0), "SURVEY_ORDER_PLACEMENT_ID" NUMBER(38,0), "SURVEY_CHANNEL_ID" NUMBER(38,0), "DAY_OF_WEEK" VARCHAR(16777216), "FISC_WEEK_NBR" NUMBER(38,0), "FISC_WEEK_START_DT" DATE, "FISC_PERIOD_NBR" NUMBER(38,0), "FISC_PERIOD_START_DT" DATE, "FISC_QUARTER_NBR" NUMBER(38,0), "FISC_YEAR_NBR" NUMBER(38,0), "DMA_CD" VARCHAR(16777216), "DMA_NM" VARCHAR(16777216), "OWNERSHIP_TYP" VARCHAR(16777216), "OSAT_RESPONSE_CNT" NUMBER(38,0), "OSAT_5_CNT" NUMBER(38,0), "OSAT_5_PCT" NUMBER(38,1), "OSAT_4_CNT" NUMBER(38,0), "OSAT_4_PCT" NUMBER(38,1), "OSAT_3_CNT" NUMBER(38,0), "OSAT_3_PCT" NUMBER(38,1), "OSAT_2_CNT" NUMBER(38,0), "OSAT_2_PCT" NUMBER(38,1), "OSAT_1_CNT" NUMBER(38,0), "OSAT_1_PCT" NUMBER(38,1), "OSAT_CLEANLINESS_RESPONSE_CNT" NUMBER(38,0), "OSAT_CLEANLINESS_5_CNT" NUMBER(38,0), "OSAT_CLEANLINESS_5_PCT" NUMBER(38,1), "OSAT_CLEANLINESS_4_CNT" NUMBER(38,0), "OSAT_CLEANLINESS_4_PCT" NUMBER(38,1), "OSAT_CLEANLINESS_3_CNT" NUMBER(38,0), "OSAT_CLEANLINESS_3_PCT" NUMBER(38,1), "OSAT_CLEANLINESS_2_CNT" NUMBER(38,0), "OSAT_CLEANLINESS_2_PCT" NUMBER(38,1), "OSAT_CLEANLINESS_1_CNT" NUMBER(38,0), "OSAT_CLEANLINESS_1_PCT" NUMBER(38,1), "OSAT_ACCURACY_RESPONSE_CNT" NUMBER(38,0), "OSAT_ACCURACY_5_CNT" NUMBER(38,0), "OSAT_ACCURACY_5_PCT" NUMBER(38,1), "OSAT_ACCURACY_4_CNT" NUMBER(38,0), "OSAT_ACCURACY_4_PCT" NUMBER(38,1), "OSAT_ACCURACY_3_CNT" NUMBER(38,0), "OSAT_ACCURACY_3_PCT" NUMBER(38,1), "OSAT_ACCURACY_2_CNT" NUMBER(38,0), "OSAT_ACCURACY_2_PCT" NUMBER(38,1), "OSAT_ACCURACY_1_CNT" NUMBER(38,0), "OSAT_ACCURACY_1_PCT" NUMBER(38,1), "OSAT_FOOD_TASTE_RESPONSE_CNT" NUMBER(38,0), "OSAT_FOOD_TASTE_5_CNT" NUMBER(38,0), "OSAT_FOOD_TASTE_5_PCT" NUMBER(38,1), "OSAT_FOOD_TASTE_4_CNT" NUMBER(38,0), "OSAT_FOOD_TASTE_4_PCT" NUMBER(38,1), "OSAT_FOOD_TASTE_3_CNT" NUMBER(38,0), "OSAT_FOOD_TASTE_3_PCT" NUMBER(38,1), "OSAT_FOOD_TASTE_2_CNT" NUMBER(38,0), "OSAT_FOOD_TASTE_2_PCT" NUMBER(38,1), "OSAT_FOOD_TASTE_1_CNT" NUMBER(38,0), "OSAT_FOOD_TASTE_1_PCT" NUMBER(38,1), "OSAT_FRIENDLINESS_RESPONSE_CNT" NUMBER(38,0), "OSAT_FRIENDLINESS_5_CNT" NUMBER(38,0), "OSAT_FRIENDLINESS_5_PCT" NUMBER(38,1), "OSAT_FRIENDLINESS_4_CNT" NUMBER(38,0), "OSAT_FRIENDLINESS_4_PCT" NUMBER(38,1), "OSAT_FRIENDLINESS_3_CNT" NUMBER(38,0), "OSAT_FRIENDLINESS_3_PCT" NUMBER(38,1), "OSAT_FRIENDLINESS_2_CNT" NUMBER(38,0), "OSAT_FRIENDLINESS_2_PCT" NUMBER(38,1), "OSAT_FRIENDLINESS_1_CNT" NUMBER(38,0), "OSAT_FRIENDLINESS_1_PCT" NUMBER(38,1), "OSAT_SERVICE_SPEED_RESPONSE_CNT" NUMBER(38,0), "OSAT_SERVICE_SPEED_5_CNT" NUMBER(38,0), "OSAT_SERVICE_SPEED_5_PCT" NUMBER(38,1), "OSAT_SERVICE_SPEED_4_CNT" NUMBER(38,0), "OSAT_SERVICE_SPEED_4_PCT" NUMBER(38,1), "OSAT_SERVICE_SPEED_3_CNT" NUMBER(38,0), "OSAT_SERVICE_SPEED_3_PCT" NUMBER(38,1), "OSAT_SERVICE_SPEED_2_CNT" NUMBER(38,0), "OSAT_SERVICE_SPEED_2_PCT" NUMBER(38,1), "OSAT_SERVICE_SPEED_1_CNT" NUMBER(38,0), "OSAT_SERVICE_SPEED_1_PCT" NUMBER(38,1), "LIKE_TO_RETURN_RESPONSE_CNT" NUMBER(38,0), "LIKE_TO_RETURN_SCORE_5_CNT" NUMBER(38,0), "LIKE_TO_RETURN_SCORE_5_PCT" NUMBER(38,1), "LIKE_TO_RETURN_SCORE_4_CNT" NUMBER(38,0), "LIKE_TO_RETURN_SCORE_4_PCT" NUMBER(38,1), "LIKE_TO_RETURN_SCORE_3_CNT" NUMBER(38,0), "LIKE_TO_RETURN_SCORE_3_PCT" NUMBER(38,1), "LIKE_TO_RETURN_SCORE_2_CNT" NUMBER(38,0), "LIKE_TO_RETURN_SCORE_2_PCT" NUMBER(38,1), "LIKE_TO_RETURN_SCORE_1_CNT" NUMBER(38,0), "LIKE_TO_RETURN_SCORE_1_PCT" NUMBER(38,1), "LOC_LEVEL1_NM" VARCHAR(16777216), "LOC_LEVEL1_MANAGER_NM" VARCHAR(16777216), "LOC_LEVEL2_NM" VARCHAR(16777216), "LOC_LEVEL2_MANAGER_NM" VARCHAR(16777216), "LOC_LEVEL3_NM" VARCHAR(16777216), "LOC_LEVEL3_MANAGER_NM" VARCHAR(16777216), "LOC_LEVEL4_NM" VARCHAR(16777216), "LOC_LEVEL4_MANAGER_NM" VARCHAR(16777216), "LOC_LEVEL5_NM" VARCHAR(16777216), "LOC_LEVEL5_MANAGER_NM" VARCHAR(16777216), "STORE_CONFIG" VARCHAR(16777216))
LANGUAGE SQL
AS '

WITH SRH_COUNTS_CTE AS
(
  SELECT
  BRAND_ID
  , STORE_ID
  , BUSINESS_DT
  , CASE 
      WHEN AGG_LVL = ''DAY_PART'' THEN DAY_PART_NM 
      ELSE NULL 
    END AS DAY_PART_NM
  , CASE 
      WHEN AGG_LVL = ''ORDER_FULFILLMENT'' THEN SURVEY_ORDER_FULFILLMENT_ID 
      ELSE NULL 
    END AS SURVEY_ORDER_FULFILLMENT_ID
  , CASE 
      WHEN AGG_LVL = ''ORDER_PLACEMENT'' THEN SURVEY_ORDER_PLACEMENT_ID 
      ELSE NULL 
    END AS SURVEY_ORDER_PLACEMENT_ID
  , CASE 
      WHEN AGG_LVL = ''SURVEY_CHANNEL'' THEN SURVEY_CHANNEL_ID 
      ELSE NULL 
    END AS SURVEY_CHANNEL_ID
  , SUM(CASE WHEN OVERALL_SATISFACTION_SCORE_NBR = 5 THEN 1 ELSE 0 END) AS OSAT_5_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SCORE_NBR = 4 THEN 1 ELSE 0 END) AS OSAT_4_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SCORE_NBR = 3 THEN 1 ELSE 0 END) AS OSAT_3_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SCORE_NBR = 2 THEN 1 ELSE 0 END) AS OSAT_2_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SCORE_NBR = 1 THEN 1 ELSE 0 END) AS OSAT_1_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR = 5 THEN 1 ELSE 0 END) AS OSAT_CLEANLINESS_5_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR = 4 THEN 1 ELSE 0 END) AS OSAT_CLEANLINESS_4_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR = 3 THEN 1 ELSE 0 END) AS OSAT_CLEANLINESS_3_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR = 2 THEN 1 ELSE 0 END) AS OSAT_CLEANLINESS_2_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR = 1 THEN 1 ELSE 0 END) AS OSAT_CLEANLINESS_1_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_ACCURACY_SCORE_NBR = 5 THEN 1 ELSE 0 END) AS OSAT_ACCURACY_5_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_ACCURACY_SCORE_NBR = 4 THEN 1 ELSE 0 END) AS OSAT_ACCURACY_4_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_ACCURACY_SCORE_NBR = 3 THEN 1 ELSE 0 END) AS OSAT_ACCURACY_3_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_ACCURACY_SCORE_NBR = 2 THEN 1 ELSE 0 END) AS OSAT_ACCURACY_2_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_ACCURACY_SCORE_NBR = 1 THEN 1 ELSE 0 END) AS OSAT_ACCURACY_1_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR = 5 THEN 1 ELSE 0 END) AS OSAT_FOOD_TASTE_5_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR = 4 THEN 1 ELSE 0 END) AS OSAT_FOOD_TASTE_4_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR = 3 THEN 1 ELSE 0 END) AS OSAT_FOOD_TASTE_3_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR = 2 THEN 1 ELSE 0 END) AS OSAT_FOOD_TASTE_2_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR = 1 THEN 1 ELSE 0 END) AS OSAT_FOOD_TASTE_1_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR = 5 THEN 1 ELSE 0 END) AS OSAT_FRIENDLINESS_5_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR = 4 THEN 1 ELSE 0 END) AS OSAT_FRIENDLINESS_4_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR = 3 THEN 1 ELSE 0 END) AS OSAT_FRIENDLINESS_3_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR = 2 THEN 1 ELSE 0 END) AS OSAT_FRIENDLINESS_2_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR = 1 THEN 1 ELSE 0 END) AS OSAT_FRIENDLINESS_1_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR = 5 THEN 1 ELSE 0 END) AS OSAT_SERVICE_SPEED_5_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR = 4 THEN 1 ELSE 0 END) AS OSAT_SERVICE_SPEED_4_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR = 3 THEN 1 ELSE 0 END) AS OSAT_SERVICE_SPEED_3_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR = 2 THEN 1 ELSE 0 END) AS OSAT_SERVICE_SPEED_2_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR = 1 THEN 1 ELSE 0 END) AS OSAT_SERVICE_SPEED_1_CNT
  , SUM(CASE WHEN CUST_RETURN_LIKELY_SCORE_NBR = 5 THEN 1 ELSE 0 END) AS LIKE_TO_RETURN_SCORE_5_CNT
  , SUM(CASE WHEN CUST_RETURN_LIKELY_SCORE_NBR = 4 THEN 1 ELSE 0 END) AS LIKE_TO_RETURN_SCORE_4_CNT
  , SUM(CASE WHEN CUST_RETURN_LIKELY_SCORE_NBR = 3 THEN 1 ELSE 0 END) AS LIKE_TO_RETURN_SCORE_3_CNT
  , SUM(CASE WHEN CUST_RETURN_LIKELY_SCORE_NBR = 2 THEN 1 ELSE 0 END) AS LIKE_TO_RETURN_SCORE_2_CNT
  , SUM(CASE WHEN CUST_RETURN_LIKELY_SCORE_NBR = 1 THEN 1 ELSE 0 END) AS LIKE_TO_RETURN_SCORE_1_CNT
  
  , SUM(CASE WHEN OVERALL_SATISFACTION_SCORE_NBR IS NOT NULL THEN 1 ELSE 0 END) AS TOTAL_OSAT_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_CLEANLINESS_SCORE_NBR IS NOT NULL THEN 1 ELSE 0 END) AS TOTAL_OSAT_CLEANLINESS_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_ACCURACY_SCORE_NBR IS NOT NULL THEN 1 ELSE 0 END) AS TOTAL_OSAT_ACCURACY_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FOOD_TASTE_SCORE_NBR IS NOT NULL THEN 1 ELSE 0 END) AS TOTAL_OSAT_FOOD_TASTE_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_FRIENDLINESS_SCORE_NBR IS NOT NULL THEN 1 ELSE 0 END) AS TOTAL_OSAT_FRIENDLINESS_CNT
  , SUM(CASE WHEN OVERALL_SATISFACTION_SERVICE_SPEED_SCORE_NBR IS NOT NULL THEN 1 ELSE 0 END) AS TOTAL_OSAT_SERVICE_SPEED_CNT
  , SUM(CASE WHEN CUST_RETURN_LIKELY_SCORE_NBR IS NOT NULL THEN 1 ELSE 0 END) AS TOTAL_LIKE_TO_RETURN_SCORE_CNT
  FROM IDS_DEV.CUST_BV.GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER_BV
  GROUP BY 
  BRAND_ID
  , STORE_ID
  , BUSINESS_DT
  , CASE 
      WHEN AGG_LVL = ''DAY_PART'' THEN DAY_PART_NM 
      ELSE NULL 
    END
  , CASE 
      WHEN AGG_LVL = ''ORDER_FULFILLMENT'' THEN SURVEY_ORDER_FULFILLMENT_ID 
      ELSE NULL 
    END
  , CASE 
      WHEN AGG_LVL = ''ORDER_PLACEMENT'' THEN SURVEY_ORDER_PLACEMENT_ID 
      ELSE NULL 
    END 
  , CASE 
      WHEN AGG_LVL = ''SURVEY_CHANNEL'' THEN SURVEY_CHANNEL_ID 
      ELSE NULL 
    END
)
, STORE_CFG_DUN_BR AS
(
    SELECT
        l.STORE_ID
        , l.REF_DATE
        , LISTAGG(DISTINCT l.LIST_BRANDS, '', '') WITHIN GROUP (ORDER BY LIST_BRANDS) AS LIST_BRANDS
        , CASE WHEN LISTAGG(DISTINCT l.LIST_BRANDS, '', '') WITHIN GROUP (ORDER BY LIST_BRANDS) = ''bskn, dnkn'' THEN 1 ELSE 0 END AS IS_DUN_BR
        , bsk.OPEN_DT AS BSKN_OPEN_DT
        , dnk.OPEN_DT AS DNKN_OPEN_DT
    FROM
    (
        SELECT
            lst.STORE_ID
            , lst.CLOSURE_DT
            , lst.LIST_BRANDS
            , MAX(lst.CLOSURE_DT) OVER (PARTITION BY lst.STORE_ID) AS Ref_Date
        FROM
        (
        SELECT
            st.STORE_ID
            , st.CLOSURE_DT
            , LISTAGG(DISTINCT st.BRAND_ID, '', '')  WITHIN GROUP (ORDER BY BRAND_ID) AS LIST_BRANDS
        FROM
            IDH_DEV.SHARED.STORE_V st
        WHERE
            st.BRAND_ID IN (''bskn'', ''dnkn'')
        GROUP BY
            st.STORE_ID
            , st.CLOSURE_DT
        ) lst
    ) l
    LEFT OUTER JOIN
    (
        SELECT
            st.STORE_ID
            , MIN(st.OPEN_DT) AS OPEN_DT
        FROM
            IDH_DEV.SHARED.STORE_V st
        WHERE
            st.BRAND_ID IN (''bskn'')
        GROUP BY
            st.STORE_ID
    ) bsk
    ON  bsk.STORE_ID = l.STORE_ID
    LEFT OUTER JOIN
    (
        SELECT
            st.STORE_ID
            , MIN(st.OPEN_DT) AS OPEN_DT
        FROM
            IDH_DEV.SHARED.STORE_V st
        WHERE
            st.BRAND_ID IN (''dnkn'')
        GROUP BY
            st.STORE_ID
    ) dnk
    ON  dnk.STORE_ID = l.STORE_ID
    GROUP BY
        l.STORE_ID
        , l.REF_DATE
        , bsk.OPEN_DT
        , dnk.OPEN_DT
)

SELECT
SRH.BRAND_ID
, SRH.STORE_ID
, SRH.BUSINESS_DT
, SRH.DAY_PART_NM
, SRH.SURVEY_ORDER_FULFILLMENT_ID
, SRH.SURVEY_ORDER_PLACEMENT_ID
, SRH.SURVEY_CHANNEL_ID
, DD.CALENDAR_DAY_NM AS DAY_OF_WEEK
, DD.FISCAL_WEEK_NBR AS FISC_WEEK_NBR
, DD.FISCAL_WEEK_START_DT AS FISC_WEEK_START_DT
, DD.FISCAL_PERIOD_NBR AS FISC_PERIOD_NBR
, DD.FISCAL_PERIOD_START_DT AS FISC_PERIOD_START_DT
, DD.FISCAL_QUARTER_NBR AS FISC_QUARTER_NBR
, DD.FISCAL_YEAR_NBR AS FISC_YEAR_NBR
, SV.DMA_CD
, SV.DMA_NM
, SV.OWNERSHIP_TYP
, SRH.TOTAL_OSAT_CNT AS OSAT_RESPONSE_CNT
, SRH.OSAT_5_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_5_CNT, SRH.TOTAL_OSAT_CNT) AS OSAT_5_PCT
, SRH.OSAT_4_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_4_CNT, SRH.TOTAL_OSAT_CNT) AS OSAT_4_PCT
, SRH.OSAT_3_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_3_CNT, SRH.TOTAL_OSAT_CNT) AS OSAT_3_PCT
, SRH.OSAT_2_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_2_CNT, SRH.TOTAL_OSAT_CNT) AS OSAT_2_PCT
, SRH.OSAT_1_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_1_CNT, SRH.TOTAL_OSAT_CNT) AS OSAT_1_PCT
, SRH.TOTAL_OSAT_CLEANLINESS_CNT AS OSAT_CLEANLINESS_RESPONSE_CNT
, SRH.OSAT_CLEANLINESS_5_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_CLEANLINESS_5_CNT, SRH.TOTAL_OSAT_CLEANLINESS_CNT) AS OSAT_CLEANLINESS_5_PCT
, SRH.OSAT_CLEANLINESS_4_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_CLEANLINESS_4_CNT, SRH.TOTAL_OSAT_CLEANLINESS_CNT) AS OSAT_CLEANLINESS_4_PCT
, SRH.OSAT_CLEANLINESS_3_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_CLEANLINESS_3_CNT, SRH.TOTAL_OSAT_CLEANLINESS_CNT) AS OSAT_CLEANLINESS_3_PCT
, SRH.OSAT_CLEANLINESS_2_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_CLEANLINESS_2_CNT, SRH.TOTAL_OSAT_CLEANLINESS_CNT) AS OSAT_CLEANLINESS_2_PCT
, SRH.OSAT_CLEANLINESS_1_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_CLEANLINESS_1_CNT, SRH.TOTAL_OSAT_CLEANLINESS_CNT) AS OSAT_CLEANLINESS_1_PCT
, SRH.TOTAL_OSAT_ACCURACY_CNT AS OSAT_ACCURACY_RESPONSE_CNT
, SRH.OSAT_ACCURACY_5_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_ACCURACY_5_CNT, SRH.TOTAL_OSAT_ACCURACY_CNT) AS OSAT_ACCURACY_5_PCT
, SRH.OSAT_ACCURACY_4_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_ACCURACY_4_CNT, SRH.TOTAL_OSAT_ACCURACY_CNT) AS OSAT_ACCURACY_4_PCT
, SRH.OSAT_ACCURACY_3_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_ACCURACY_3_CNT, SRH.TOTAL_OSAT_ACCURACY_CNT) AS OSAT_ACCURACY_3_PCT
, SRH.OSAT_ACCURACY_2_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_ACCURACY_2_CNT, SRH.TOTAL_OSAT_ACCURACY_CNT) AS OSAT_ACCURACY_2_PCT
, SRH.OSAT_ACCURACY_1_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_ACCURACY_1_CNT, SRH.TOTAL_OSAT_ACCURACY_CNT) AS OSAT_ACCURACY_1_PCT
, SRH.TOTAL_OSAT_FOOD_TASTE_CNT AS OSAT_FOOD_TASTE_RESPONSE_CNT
, SRH.OSAT_FOOD_TASTE_5_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FOOD_TASTE_5_CNT, SRH.TOTAL_OSAT_FOOD_TASTE_CNT) AS OSAT_FOOD_TASTE_5_PCT
, SRH.OSAT_FOOD_TASTE_4_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FOOD_TASTE_4_CNT, SRH.TOTAL_OSAT_FOOD_TASTE_CNT) AS OSAT_FOOD_TASTE_4_PCT
, SRH.OSAT_FOOD_TASTE_3_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FOOD_TASTE_3_CNT, SRH.TOTAL_OSAT_FOOD_TASTE_CNT) AS OSAT_FOOD_TASTE_3_PCT
, SRH.OSAT_FOOD_TASTE_2_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FOOD_TASTE_2_CNT, SRH.TOTAL_OSAT_FOOD_TASTE_CNT) AS OSAT_FOOD_TASTE_2_PCT
, SRH.OSAT_FOOD_TASTE_1_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FOOD_TASTE_1_CNT, SRH.TOTAL_OSAT_FOOD_TASTE_CNT) AS OSAT_FOOD_TASTE_1_PCT
, SRH.TOTAL_OSAT_FRIENDLINESS_CNT AS OSAT_FRIENDLINESS_RESPONSE_CNT
, SRH.OSAT_FRIENDLINESS_5_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FRIENDLINESS_5_CNT, SRH.TOTAL_OSAT_FRIENDLINESS_CNT) AS OSAT_FRIENDLINESS_5_PCT
, SRH.OSAT_FRIENDLINESS_4_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FRIENDLINESS_4_CNT, SRH.TOTAL_OSAT_FRIENDLINESS_CNT) AS OSAT_FRIENDLINESS_4_PCT
, SRH.OSAT_FRIENDLINESS_3_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FRIENDLINESS_3_CNT, SRH.TOTAL_OSAT_FRIENDLINESS_CNT) AS OSAT_FRIENDLINESS_3_PCT
, SRH.OSAT_FRIENDLINESS_2_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FRIENDLINESS_2_CNT, SRH.TOTAL_OSAT_FRIENDLINESS_CNT) AS OSAT_FRIENDLINESS_2_PCT
, SRH.OSAT_FRIENDLINESS_1_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_FRIENDLINESS_1_CNT, SRH.TOTAL_OSAT_FRIENDLINESS_CNT) AS OSAT_FRIENDLINESS_1_PCT
, SRH.TOTAL_OSAT_SERVICE_SPEED_CNT AS OSAT_SERVICE_SPEED_RESPONSE_CNT
, SRH.OSAT_SERVICE_SPEED_5_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_SERVICE_SPEED_5_CNT, SRH.TOTAL_OSAT_SERVICE_SPEED_CNT) AS OSAT_SERVICE_SPEED_5_PCT
, SRH.OSAT_SERVICE_SPEED_4_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_SERVICE_SPEED_4_CNT, SRH.TOTAL_OSAT_SERVICE_SPEED_CNT) AS OSAT_SERVICE_SPEED_4_PCT
, SRH.OSAT_SERVICE_SPEED_3_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_SERVICE_SPEED_3_CNT, SRH.TOTAL_OSAT_SERVICE_SPEED_CNT) AS OSAT_SERVICE_SPEED_3_PCT
, SRH.OSAT_SERVICE_SPEED_2_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_SERVICE_SPEED_2_CNT, SRH.TOTAL_OSAT_SERVICE_SPEED_CNT) AS OSAT_SERVICE_SPEED_2_PCT
, SRH.OSAT_SERVICE_SPEED_1_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.OSAT_SERVICE_SPEED_1_CNT, SRH.TOTAL_OSAT_SERVICE_SPEED_CNT) AS OSAT_SERVICE_SPEED_1_PCT
, SRH.TOTAL_LIKE_TO_RETURN_SCORE_CNT AS LIKE_TO_RETURN_RESPONSE_CNT
, SRH.LIKE_TO_RETURN_SCORE_5_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.LIKE_TO_RETURN_SCORE_5_CNT, SRH.TOTAL_LIKE_TO_RETURN_SCORE_CNT) AS LIKE_TO_RETURN_SCORE_5_PCT
, SRH.LIKE_TO_RETURN_SCORE_4_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.LIKE_TO_RETURN_SCORE_4_CNT, SRH.TOTAL_LIKE_TO_RETURN_SCORE_CNT) AS LIKE_TO_RETURN_SCORE_4_PCT
, SRH.LIKE_TO_RETURN_SCORE_3_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.LIKE_TO_RETURN_SCORE_3_CNT, SRH.TOTAL_LIKE_TO_RETURN_SCORE_CNT) AS LIKE_TO_RETURN_SCORE_3_PCT
, SRH.LIKE_TO_RETURN_SCORE_2_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.LIKE_TO_RETURN_SCORE_2_CNT, SRH.TOTAL_LIKE_TO_RETURN_SCORE_CNT) AS LIKE_TO_RETURN_SCORE_2_PCT
, SRH.LIKE_TO_RETURN_SCORE_1_CNT
, IDH_DEV.SHARED.CALCULATE_PERCENTAGE(SRH.LIKE_TO_RETURN_SCORE_1_CNT, SRH.TOTAL_LIKE_TO_RETURN_SCORE_CNT) AS LIKE_TO_RETURN_SCORE_1_PCT
, LEVEL1_NM AS LOC_LEVEL1_NM
, LEVEL1_MANAGER_NM AS LOC_LEVEL1_MANAGER_NM
, LEVEL2_NM AS LOC_LEVEL2_NM
, LEVEL2_MANAGER_NM AS LOC_LEVEL2_MANAGER_NM
, LEVEL3_NM AS LOC_LEVEL3_NM
, LEVEL3_MANAGER_NM AS LOC_LEVEL3_MANAGER_NM
, LEVEL4_NM AS LOC_LEVEL4_NM
, L4_MANAGER_NM AS LOC_LEVEL4_MANAGER_NM
, LEVEL5_NM AS LOC_LEVEL5_NM
, LEVEL5_MANAGER_NM AS LOC_LEVEL5_MANAGER_NM
, CASE
    WHEN CFG.IS_DUN_BR = 1 AND UPPER(CFG.LIST_BRANDS) IN (''BSKN, DNKN'') AND IFNULL(CFG.REF_DATE, ''9999-12-31'') >= SRH.BUSINESS_DT AND IFNULL(CFG.BSKN_OPEN_DT, ''9999-12-31'') <= SRH.BUSINESS_DT AND IFNULL(CFG.DNKN_OPEN_DT, ''9999-12-31'') <= SRH.BUSINESS_DT THEN ''DUN/BR''
    WHEN UPPER(SV.BRAND_ID) IN (''DNKN'') THEN ''DUN''
    WHEN UPPER(SV.BRAND_ID) IN (''BSKN'') THEN ''BR''
    WHEN UPPER(SV.BRAND_ID) IN (''ARBYS'') THEN ''ARB''
    WHEN UPPER(SV.BRAND_ID) IN (''BWW'') THEN ''BWW''
    WHEN UPPER(SV.BRAND_ID) IN (''JJ'') THEN ''JJ''
    WHEN UPPER(SV.BRAND_ID) IN (''SONIC'') THEN ''SON''
  END AS STORE_CONFIG
FROM 
SRH_COUNTS_CTE AS SRH
LEFT OUTER JOIN IDH_DEV.SHARED.DATE_DIM_V AS DD ON SRH.BUSINESS_DT = DD.CALENDAR_DT
LEFT OUTER JOIN IDH_DEV.SHARED.STORE_V AS SV 
  ON SRH.STORE_ID = SV.STORE_ID 
  AND SRH.BRAND_ID = SV.BRAND_ID
LEFT OUTER JOIN IDH_DEV.OPERATION.LOCATION_HIERARCHY_V AS LH
  ON SRH.STORE_ID = LH.LOCATION_ID
  AND SRH.BRAND_ID = LH.BRAND_ID
LEFT OUTER JOIN STORE_CFG_DUN_BR AS CFG ON SV.STORE_ID = CFG.STORE_ID
';
CREATE FUNCTION IF NOT EXISTS SUMMARY_SALES("BUSINESS_DATE" VARCHAR(16777216))
RETURNS TABLE ("STORE_ID" VARCHAR(16777216), "NET_SALES_AMT" NUMBER(12,2), "TRANSACTION_CNT" NUMBER(38,0), "CALCULATIONTYPE" VARCHAR(16777216))
LANGUAGE SQL
AS '

SELECT
    dfsf.STORE_ID
    , SUM(dfsf.NET_SALES_AMT) AS NET_SALES_AMT
    , SUM(dfsf.TRANSACTION_CNT) AS TRANSACTION_CNT
    , ''WTD'' AS CalculationType
FROM
    IDS_DEV.DATASTORE.DAILY_FLASH_SALES_FACT dfsf
INNER JOIN
    IDH_DEV.SHARED.DATE_DIM_V d
ON  dfsf.BUSINESS_DT = d.CALENDAR_DT
WHERE
    dfsf.BRAND_ID = ''ARG''
AND dfsf.SOURCE_SYSTEM_NM = ''RTI''
AND NOT (dfsf.BRAND_ID = ''ARG'' AND dfsf.SOURCE_SYSTEM_NM <> ''RTI'')
GROUP BY
    dfsf.STORE_ID

';