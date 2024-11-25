create view IF NOT EXISTS CRM_BOUNCE_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber. This serves as the primary key.',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
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
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_BOUNCE_BV contains the marketing emails that did not make it to the customer and the reason why.'
 as 
SELECT CRM_BOUNCE_BV.BRAND_ID,CRM_BOUNCE_BV.SOURCE_SYSTEM_NM,CRM_BOUNCE_BV.ACCOUNT_ID,CRM_BOUNCE_BV.JOB_ID,CRM_BOUNCE_BV.SUBSCRIBER_KEY,CRM_BOUNCE_BV.MESSAGE_ID,CRM_BOUNCE_BV.SERVICE_TRANSACTION_ID,CRM_BOUNCE_BV.EMAIL_ADDRESS,CRM_BOUNCE_BV.SUBSCRIBER_ID,CRM_BOUNCE_BV.LIST_ID,CRM_BOUNCE_BV.EVENT_DATE,CRM_BOUNCE_BV.EVENT_TYPE,CRM_BOUNCE_BV.BOUNCE_CATEGORY,CRM_BOUNCE_BV.SMTP_CODE,CRM_BOUNCE_BV.BOUNCE_REASON,CRM_BOUNCE_BV.BATCH_ID,CRM_BOUNCE_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_BOUNCE_BV.LOAD_ID,CRM_BOUNCE_BV.LOAD_DTTM,CRM_BOUNCE_BV.UPDATE_ID,CRM_BOUNCE_BV.UPDATE_DTTM,CRM_BOUNCE_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_BOUNCE_BV ;
create view IF NOT EXISTS CRM_CAMPAIGN_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CAMPAIGN_ID COMMENT 'Campaign Id is Potentially another unique campaign identifer, must be generated outside of SFMC & input . Sample Value:  Welcome.',
	CAMPAIGN_TYPE COMMENT 'Campaign Type is ie. Transactional vs. Marketing . Sample Value:  AdHoc.',
	CAMPAIGN_NAME COMMENT 'Campaign Name is Campaign Name . Sample Value:  061021TuesdaySend.',
	CAMPAIGN_CATEGORY COMMENT 'Campaign Category is Category of the Campaign . Sample Value:  Transactional.',
	CAMPAIGN_DESCRIPTION COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_Campaign_BV contains a list of marketing events for customers.'
 as 
SELECT CRM_CAMPAIGN_BV.BRAND_ID,CRM_CAMPAIGN_BV.SOURCE_SYSTEM_NM,CRM_CAMPAIGN_BV.CAMPAIGN_ID,CRM_CAMPAIGN_BV.CAMPAIGN_TYPE,CRM_CAMPAIGN_BV.CAMPAIGN_NAME,CRM_CAMPAIGN_BV.CAMPAIGN_CATEGORY,CRM_CAMPAIGN_BV.CAMPAIGN_DESCRIPTION,CRM_CAMPAIGN_BV.CAMPAIGN_OBJECTIVE,CRM_CAMPAIGN_BV.CAMPAIGN_START_DATE,CRM_CAMPAIGN_BV.CAMPAIGN_END_DATE,CRM_CAMPAIGN_BV.CAMPAIGN_DURATION,CRM_CAMPAIGN_BV.LOAD_ID,CRM_CAMPAIGN_BV.LOAD_DTTM,CRM_CAMPAIGN_BV.UPDATE_ID,CRM_CAMPAIGN_BV.UPDATE_DTTM,CRM_CAMPAIGN_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_CAMPAIGN_BV ;
create view IF NOT EXISTS CRM_CLICK_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE COMMENT 'The date the click took place',
	EVENT_TYPE COMMENT 'The event = click for this table',
	SEND_URL_ID COMMENT 'A unique ID for a sent URL',
	URL_ID COMMENT 'A unique ID for a URL',
	URL COMMENT 'The URL for the link clicked. No AMPscript or variables are populated in this column, for example, www.example.com?%attribute%',
	ALIAS COMMENT 'Brief description of the URL sent via email',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'An ID used by external partners use to identify the data source',
	IS_UNIQUE COMMENT 'Whether the event is unique or repeated. NOTE: The IsUnique value is TRUE when any link is first clicked in a JobID by a subscriber. Any clicks afterwards are FALSE even if different URLs',
	IS_UNIQUE_FOR_URL COMMENT 'Whether the event is unique or repeated. NOTE: The IsUniqueForURL value is TRUE when any link is first clicked in a JobID by a subscriber. Unlike IsUnique, it is not FALSE for different URLs',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_CLICK_BV specifies the links in an email that were clicked and by whom.'
 as 
SELECT CRM_CLICK_BV.BRAND_ID,CRM_CLICK_BV.SOURCE_SYSTEM_NM,CRM_CLICK_BV.ACCOUNT_ID,CRM_CLICK_BV.JOB_ID,CRM_CLICK_BV.SUBSCRIBER_KEY,CRM_CLICK_BV.MESSAGE_ID,CRM_CLICK_BV.SERVICE_TRANSACTION_ID,CRM_CLICK_BV.EMAIL_ADDRESS,CRM_CLICK_BV.SUBSCRIBER_ID,CRM_CLICK_BV.LIST_ID,CRM_CLICK_BV.EVENT_DATE,CRM_CLICK_BV.EVENT_TYPE,CRM_CLICK_BV.SEND_URL_ID,CRM_CLICK_BV.URL_ID,CRM_CLICK_BV.URL,CRM_CLICK_BV.ALIAS,CRM_CLICK_BV.BATCH_ID,CRM_CLICK_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_CLICK_BV.IS_UNIQUE,CRM_CLICK_BV.IS_UNIQUE_FOR_URL,CRM_CLICK_BV.LOAD_ID,CRM_CLICK_BV.LOAD_DTTM,CRM_CLICK_BV.UPDATE_ID,CRM_CLICK_BV.UPDATE_DTTM,CRM_CLICK_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_CLICK_BV ;
create view IF NOT EXISTS CRM_COMPLAINT_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
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
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_COMPLAINT_BV contains a list of users who submitted a complaint.'
 as 
SELECT CRM_COMPLAINT_BV.BRAND_ID,CRM_COMPLAINT_BV.SOURCE_SYSTEM_NM,CRM_COMPLAINT_BV.ACCOUNT_ID,CRM_COMPLAINT_BV.JOB_ID,CRM_COMPLAINT_BV.SUBSCRIBER_KEY,CRM_COMPLAINT_BV.MESSAGE_ID,CRM_COMPLAINT_BV.SERVICE_TRANSACTION_ID,CRM_COMPLAINT_BV.EMAIL_ADDRESS,CRM_COMPLAINT_BV.SUBSCRIBER_ID,CRM_COMPLAINT_BV.LIST_ID,CRM_COMPLAINT_BV.EVENT_DATE,CRM_COMPLAINT_BV.EVENT_TYPE,CRM_COMPLAINT_BV.BATCH_ID,CRM_COMPLAINT_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_COMPLAINT_BV.DOMAIN,CRM_COMPLAINT_BV.LOAD_ID,CRM_COMPLAINT_BV.LOAD_DTTM,CRM_COMPLAINT_BV.UPDATE_ID,CRM_COMPLAINT_BV.UPDATE_DTTM,CRM_COMPLAINT_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_COMPLAINT_BV ;
create view IF NOT EXISTS CRM_CREATIVE_VARIANT_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	OFFER_ID COMMENT 'All values currently null',
	OFFER_CODE COMMENT 'All values currently null',
	OFFER_NAME COMMENT 'All values currently null',
	PARENT_OFFER_ID COMMENT 'All values currently null',
	CAMPAIGN_TYPE COMMENT 'All values currently null',
	CAMPAIGN_ID COMMENT 'All values currently null',
	SUBJECT_LINE COMMENT 'All values currently null',
	PRE_HEADER COMMENT 'All values currently null',
	HEAD_LINE COMMENT 'All values currently null',
	CTA_URL COMMENT 'All values currently null',
	HERO_IMAGE COMMENT 'All values currently null',
	HERO_COPY COMMENT 'All values currently null',
	SECONDARY_MODULE_IMAGE COMMENT 'All values currently null',
	SECONDARY_MODULE_COPY COMMENT 'All values currently null',
	EVENT_DATE COMMENT 'All values currently null',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_CREATIVE VARIANT_BV is a record of the creative copy, images, subject, line, etc. used for each email sent.'
 as 
SELECT CRM_CREATIVE_VARIANT_BV.BRAND_ID,CRM_CREATIVE_VARIANT_BV.SOURCE_SYSTEM_NM,CRM_CREATIVE_VARIANT_BV.OFFER_ID,CRM_CREATIVE_VARIANT_BV.OFFER_CODE,CRM_CREATIVE_VARIANT_BV.OFFER_NAME,CRM_CREATIVE_VARIANT_BV.PARENT_OFFER_ID,CRM_CREATIVE_VARIANT_BV.CAMPAIGN_TYPE,CRM_CREATIVE_VARIANT_BV.CAMPAIGN_ID,CRM_CREATIVE_VARIANT_BV.SUBJECT_LINE,CRM_CREATIVE_VARIANT_BV.PRE_HEADER,CRM_CREATIVE_VARIANT_BV.HEAD_LINE,CRM_CREATIVE_VARIANT_BV.CTA_URL,CRM_CREATIVE_VARIANT_BV.HERO_IMAGE,CRM_CREATIVE_VARIANT_BV.HERO_COPY,CRM_CREATIVE_VARIANT_BV.SECONDARY_MODULE_IMAGE,CRM_CREATIVE_VARIANT_BV.SECONDARY_MODULE_COPY,CRM_CREATIVE_VARIANT_BV.EVENT_DATE,CRM_CREATIVE_VARIANT_BV.LOAD_ID,CRM_CREATIVE_VARIANT_BV.LOAD_DTTM,CRM_CREATIVE_VARIANT_BV.UPDATE_ID,CRM_CREATIVE_VARIANT_BV.UPDATE_DTTM,CRM_CREATIVE_VARIANT_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_CREATIVE_VARIANT_BV ;
create view IF NOT EXISTS CRM_CREATIVE_VARIANT_MVP_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CREATIVE_VARIANT_PART COMMENT 'Creative Variant Part is ',
	CAMPAIGN_TYPE COMMENT 'Campaign Type is ',
	CAMPAIGN_ID COMMENT 'Campaign Id is ',
	SUBJECT_LINE COMMENT 'Subject Line is ',
	PRE_HEADER COMMENT 'Pre Header is ',
	IMAGE COMMENT 'Image is ',
	IMAGE_LINK COMMENT 'Image Link is ',
	IMAGE_ALT COMMENT 'Image Alt is ',
	HEADER_TEXT COMMENT 'Header Text is ',
	BODY_TEXT COMMENT 'Body Text is ',
	LEFT_CTA_TEXT COMMENT 'Left  Cta Text is ',
	LEFT_CTA_LINK COMMENT 'Left Cta Link is ',
	RIGHT_CTA_TEXT COMMENT 'Right Cta Text is ',
	RIGHT_CTA_LINK COMMENT 'Right Cta Link is ',
	CENTER_CTA_TEXT COMMENT 'Center Cta Text is ',
	CENTER_CTA_LINK COMMENT 'Center Cta Link is ',
	TOP_LEFT_IMAGE_NAME COMMENT 'Top Left Image Name is ',
	TOP_LEFT_IMAGE COMMENT 'Top Left Image is ',
	TOP_LEFT_LINK COMMENT 'Top Left Link is ',
	TOP_LEFT_IMAGE_ALT COMMENT 'Top Left Image Alt is ',
	TOP_RIGHT_IMAGE_NAME COMMENT 'Top Right Image Name is ',
	TOP_RIGHT_IMAGE COMMENT 'Top Right Image is ',
	TOP_RIGHT_LINK COMMENT 'Top Right Link is ',
	TOP_RIGHT_IMAGE_ALT COMMENT 'Top Right Image Alt is ',
	BOTTOM_LEFT_IMAGE_NAME COMMENT 'Bottom Left Image Name is ',
	BOTTOM_LEFT_IMAGE COMMENT 'Bottom Left Image is ',
	BOTTOM_LEFT_LINK COMMENT 'Bottom Left Link is ',
	BOTTOM_LEFT_IMAGE_ALT COMMENT 'Bottom Left Image Alt is ',
	BOTTOM_RIGHT_IMAGE_NAME COMMENT 'Bottom Right Image Name is ',
	BOTTOM_RIGHT_IMAGE COMMENT 'Bottom Right Image is ',
	BOTTOM_RIGHT_LINK COMMENT 'Bottom Right Link is ',
	BOTTOM_RIGHT_IMAGE_ALT COMMENT 'Bottom Right Image Alt is ',
	ICON_HEADER_1 COMMENT 'Icon Header 1 is ',
	ICON_BODY_1 COMMENT 'Icon Body 1 is ',
	ICON_IMAGE_1 COMMENT 'Icon Image 1 is ',
	ICON_IMAGE_LINK_1 COMMENT 'Icon Image Link 1 is ',
	ICON_IMAGE_ALT_1 COMMENT 'Icon Image Alt 1 is ',
	ICON_HEADER_2 COMMENT 'Icon Header 2 is ',
	ICON_BODY_2 COMMENT 'Icon Body 2 is ',
	ICON_IMAGE_2 COMMENT 'Icon Image 2 is ',
	ICON_IMAGE_LINK_2 COMMENT 'Icon Image Link 2 is ',
	ICON_IMAGE_ALT_2 COMMENT 'Icon Image Alt 2 is ',
	ICON_HEADER_3 COMMENT 'Icon Header 3 is ',
	ICON_BODY_3 COMMENT 'Icon Body 3 is ',
	ICON_IMAGE_3 COMMENT 'Icon Image 3 is ',
	ICON_IMAGE_LINK_3 COMMENT 'Icon Image Link 3 is ',
	ICON_IMAGE_ALT_3 COMMENT 'Icon Image Alt 3 is ',
	TERMS_AND_CONDITIONS COMMENT 'Terms And Conditions is ',
	EVENT_DATE COMMENT 'Event Date is ',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM Creative Variant MVP_BV contains the marketing content that was sent to a customer in an marketing email.'
 as 
SELECT CRM_CREATIVE_VARIANT_MVP_BV.BRAND_ID,CRM_CREATIVE_VARIANT_MVP_BV.SOURCE_SYSTEM_NM,CRM_CREATIVE_VARIANT_MVP_BV.CREATIVE_VARIANT_PART,CRM_CREATIVE_VARIANT_MVP_BV.CAMPAIGN_TYPE,CRM_CREATIVE_VARIANT_MVP_BV.CAMPAIGN_ID,CRM_CREATIVE_VARIANT_MVP_BV.SUBJECT_LINE,CRM_CREATIVE_VARIANT_MVP_BV.PRE_HEADER,CRM_CREATIVE_VARIANT_MVP_BV.IMAGE,CRM_CREATIVE_VARIANT_MVP_BV.IMAGE_LINK,CRM_CREATIVE_VARIANT_MVP_BV.IMAGE_ALT,CRM_CREATIVE_VARIANT_MVP_BV.HEADER_TEXT,CRM_CREATIVE_VARIANT_MVP_BV.BODY_TEXT,CRM_CREATIVE_VARIANT_MVP_BV.LEFT_CTA_TEXT,CRM_CREATIVE_VARIANT_MVP_BV.LEFT_CTA_LINK,CRM_CREATIVE_VARIANT_MVP_BV.RIGHT_CTA_TEXT,CRM_CREATIVE_VARIANT_MVP_BV.RIGHT_CTA_LINK,CRM_CREATIVE_VARIANT_MVP_BV.CENTER_CTA_TEXT,CRM_CREATIVE_VARIANT_MVP_BV.CENTER_CTA_LINK,CRM_CREATIVE_VARIANT_MVP_BV.TOP_LEFT_IMAGE_NAME,CRM_CREATIVE_VARIANT_MVP_BV.TOP_LEFT_IMAGE,CRM_CREATIVE_VARIANT_MVP_BV.TOP_LEFT_LINK,CRM_CREATIVE_VARIANT_MVP_BV.TOP_LEFT_IMAGE_ALT,CRM_CREATIVE_VARIANT_MVP_BV.TOP_RIGHT_IMAGE_NAME,CRM_CREATIVE_VARIANT_MVP_BV.TOP_RIGHT_IMAGE,CRM_CREATIVE_VARIANT_MVP_BV.TOP_RIGHT_LINK,CRM_CREATIVE_VARIANT_MVP_BV.TOP_RIGHT_IMAGE_ALT,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_LEFT_IMAGE_NAME,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_LEFT_IMAGE,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_LEFT_LINK,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_LEFT_IMAGE_ALT,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_RIGHT_IMAGE_NAME,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_RIGHT_IMAGE,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_RIGHT_LINK,CRM_CREATIVE_VARIANT_MVP_BV.BOTTOM_RIGHT_IMAGE_ALT,CRM_CREATIVE_VARIANT_MVP_BV.ICON_HEADER_1,CRM_CREATIVE_VARIANT_MVP_BV.ICON_BODY_1,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_1,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_LINK_1,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_ALT_1,CRM_CREATIVE_VARIANT_MVP_BV.ICON_HEADER_2,CRM_CREATIVE_VARIANT_MVP_BV.ICON_BODY_2,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_2,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_LINK_2,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_ALT_2,CRM_CREATIVE_VARIANT_MVP_BV.ICON_HEADER_3,CRM_CREATIVE_VARIANT_MVP_BV.ICON_BODY_3,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_3,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_LINK_3,CRM_CREATIVE_VARIANT_MVP_BV.ICON_IMAGE_ALT_3,CRM_CREATIVE_VARIANT_MVP_BV.TERMS_AND_CONDITIONS,CRM_CREATIVE_VARIANT_MVP_BV.EVENT_DATE,CRM_CREATIVE_VARIANT_MVP_BV.LOAD_ID,CRM_CREATIVE_VARIANT_MVP_BV.LOAD_DTTM,CRM_CREATIVE_VARIANT_MVP_BV.UPDATE_ID,CRM_CREATIVE_VARIANT_MVP_BV.UPDATE_DTTM,CRM_CREATIVE_VARIANT_MVP_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_CREATIVE_VARIANT_MVP_BV ;
create view IF NOT EXISTS CRM_HOLDOUT_LOG_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
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
	DATASOURCE_NAME COMMENT 'Datasource name specifies source of the emails.  For example, Epsilon or Harmony.',
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
	CAMPAIGN_DESCRIPTION COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_HOLDOUT_LOG_BV is a list of who has not resoponded to the marketing emails.'
 as 
SELECT CRM_HOLDOUT_LOG_BV.BRAND_ID,CRM_HOLDOUT_LOG_BV.SOURCE_SYSTEM_NM,CRM_HOLDOUT_LOG_BV.ACCOUNT_ID,CRM_HOLDOUT_LOG_BV.JOB_ID,CRM_HOLDOUT_LOG_BV.SUBSCRIBER_KEY,CRM_HOLDOUT_LOG_BV.PROFILE_ID,CRM_HOLDOUT_LOG_BV.EXTERNAL_ID,CRM_HOLDOUT_LOG_BV.CUSTOMER_ID,CRM_HOLDOUT_LOG_BV.OFFER_ID,CRM_HOLDOUT_LOG_BV.EVENT_DATE,CRM_HOLDOUT_LOG_BV.OFFER_CODE,CRM_HOLDOUT_LOG_BV.EMAIL_NAME,CRM_HOLDOUT_LOG_BV.CREATIVE_VARIANT,CRM_HOLDOUT_LOG_BV.DATASOURCE_NAME,CRM_HOLDOUT_LOG_BV.OFFER_NAME,CRM_HOLDOUT_LOG_BV.PARENT_OFFER_ID,CRM_HOLDOUT_LOG_BV.JOURNEY_NAME,CRM_HOLDOUT_LOG_BV.JOURNEY_STEP,CRM_HOLDOUT_LOG_BV.STRENGTH_OF_CUSTOMER,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_1,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_2,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_3,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_4,CRM_HOLDOUT_LOG_BV.USER_DEFINED_SEGMENT_5,CRM_HOLDOUT_LOG_BV.OFFER_DECISION_LOGIC,CRM_HOLDOUT_LOG_BV.POINT_BALANCE,CRM_HOLDOUT_LOG_BV.EMAIL_ADDRESS,CRM_HOLDOUT_LOG_BV.CAMPAIGN_ID,CRM_HOLDOUT_LOG_BV.CAMPAIGN_NAME,CRM_HOLDOUT_LOG_BV.CAMPAIGN_TYPE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_CATEGORY,CRM_HOLDOUT_LOG_BV.CAMPAIGN_DESCRIPTION,CRM_HOLDOUT_LOG_BV.CAMPAIGN_OBJECTIVE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_START_DATE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_END_DATE,CRM_HOLDOUT_LOG_BV.CAMPAIGN_DURATION,CRM_HOLDOUT_LOG_BV.LOAD_ID,CRM_HOLDOUT_LOG_BV.LOAD_DTTM,CRM_HOLDOUT_LOG_BV.UPDATE_ID,CRM_HOLDOUT_LOG_BV.UPDATE_DTTM,CRM_HOLDOUT_LOG_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_HOLDOUT_LOG_BV ;
create view IF NOT EXISTS CRM_JOURNEY_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	VERSION_ID COMMENT 'The unique identifier for the version of the journey',
	JOURNEY_ID COMMENT 'The unique identifier for the journey. There are one or more VersionIDs associated to a JourneyID',
	JOURNEY_NAME COMMENT 'The name of the Journey',
	CREATED_DATE COMMENT 'The date that the version of the journey was created',
	LAST_PUBLISHED_DATE COMMENT 'The date that the version of the journey was last published',
	MODIFIED_DATE COMMENT 'The date that the version of the journey was last edited',
	JOURNEY_STATUS COMMENT 'The current running mode of the journey. Possible values are Draft, Running, Finishing and Stopped',
	VERSION_NUMBER COMMENT 'The version number of the version of the journey',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_JOURNEY_BV is a high level of grouping of marketing interactions and their current status.'
 as 
SELECT CRM_JOURNEY_BV.BRAND_ID,CRM_JOURNEY_BV.SOURCE_SYSTEM_NM,CRM_JOURNEY_BV.VERSION_ID,CRM_JOURNEY_BV.JOURNEY_ID,CRM_JOURNEY_BV.JOURNEY_NAME,CRM_JOURNEY_BV.CREATED_DATE,CRM_JOURNEY_BV.LAST_PUBLISHED_DATE,CRM_JOURNEY_BV.MODIFIED_DATE,CRM_JOURNEY_BV.JOURNEY_STATUS,CRM_JOURNEY_BV.VERSION_NUMBER,CRM_JOURNEY_BV.LOAD_ID,CRM_JOURNEY_BV.LOAD_DTTM,CRM_JOURNEY_BV.UPDATE_ID,CRM_JOURNEY_BV.UPDATE_DTTM,CRM_JOURNEY_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_JOURNEY_BV ;
create view IF NOT EXISTS CRM_JOURNEY_EMAIL_ACTIVITY_BV(
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	OYB_ACCOUNT_ID COMMENT 'OYB_Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	VERSION_ID COMMENT 'The unique identifier for the version of the journey',
	ACCOUNT_ID COMMENT 'Account Id in the CRM Journey Emaill Activity table is the Parentt Account Id. This value is populated with 100004555 and should be the same across brands. The On-Your-Behalf (OYB)  Account number is the account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only. The values for  BWW is 100040982 and Arbys is 100039504.',
	ACTIVITY_ID COMMENT 'The unique identifier for the activity. There are one or more ActivityIDs associated to a VersionID',
	ACTIVITY_NAME COMMENT 'The name of the Activity',
	ACTIVITY_EXTERNAL_KEY COMMENT 'The external key associated with the activity',
	JOURNEY_ACTIVITY_OBJECT_ID COMMENT 'Use this unique identifier to join the email tracking system data views to identify a journey emails Triggered Send Definition',
	ACTIVITY_TYPE COMMENT 'The type of activity',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.'
) COMMENT='CRM_JOURNEY_EMAIL_ACTIVITY_BV contains the emails that are part of a CRM journey.'
 as 
SELECT CRM_JOURNEY_EMAIL_ACTIVITY_BV.SOURCE_SYSTEM_NM,CRM_JOURNEY_EMAIL_ACTIVITY_BV.OYB_ACCOUNT_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.JOB_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.VERSION_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.ACCOUNT_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.ACTIVITY_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.ACTIVITY_NAME,CRM_JOURNEY_EMAIL_ACTIVITY_BV.ACTIVITY_EXTERNAL_KEY,CRM_JOURNEY_EMAIL_ACTIVITY_BV.JOURNEY_ACTIVITY_OBJECT_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.ACTIVITY_TYPE,CRM_JOURNEY_EMAIL_ACTIVITY_BV.LOAD_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.LOAD_DTTM,CRM_JOURNEY_EMAIL_ACTIVITY_BV.UPDATE_ID,CRM_JOURNEY_EMAIL_ACTIVITY_BV.UPDATE_DTTM,CRM_JOURNEY_EMAIL_ACTIVITY_BV.LOAD_FILENAME,CRM_JOURNEY_EMAIL_ACTIVITY_BV.BRAND_ID
FROM IDS_DEV.CUST_BV.CRM_JOURNEY_EMAIL_ACTIVITY_BV ;
create view IF NOT EXISTS CRM_OPEN_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LIST_ID COMMENT 'The list ID number for the list used in the send',
	EVENT_DATE COMMENT 'The date the open took place',
	EVENT_TYPE COMMENT 'Event type = opened email',
	BATCH_ID COMMENT 'The BatchID number used for any batches used in the send',
	TRIGGERED_SEND_EXTERNAL_KEY COMMENT 'An ID used by external partners use to identify the data source',
	IS_UNIQUE COMMENT 'Whether the event is unique or repeated',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_OPEN_BV contains the list of users that opened emails they received.'
 as 
SELECT CRM_OPEN_BV.BRAND_ID,CRM_OPEN_BV.SOURCE_SYSTEM_NM,CRM_OPEN_BV.ACCOUNT_ID,CRM_OPEN_BV.JOB_ID,CRM_OPEN_BV.SUBSCRIBER_KEY,CRM_OPEN_BV.MESSAGE_ID,CRM_OPEN_BV.SERVICE_TRANSACTION_ID,CRM_OPEN_BV.EMAIL_ADDRESS,CRM_OPEN_BV.SUBSCRIBER_ID,CRM_OPEN_BV.LIST_ID,CRM_OPEN_BV.EVENT_DATE,CRM_OPEN_BV.EVENT_TYPE,CRM_OPEN_BV.BATCH_ID,CRM_OPEN_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_OPEN_BV.IS_UNIQUE,CRM_OPEN_BV.LOAD_ID,CRM_OPEN_BV.LOAD_DTTM,CRM_OPEN_BV.UPDATE_ID,CRM_OPEN_BV.UPDATE_DTTM,CRM_OPEN_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_OPEN_BV ;
create view IF NOT EXISTS CRM_PUSH_MESSAGE_DETAIL_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
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
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM Push Message Detail_BV contains information about push notification interactions.'
 as 
SELECT CRM_PUSH_MESSAGE_DETAIL_BV.BRAND_ID,CRM_PUSH_MESSAGE_DETAIL_BV.SOURCE_SYSTEM_NM,CRM_PUSH_MESSAGE_DETAIL_BV.ACCOUNT_ID,CRM_PUSH_MESSAGE_DETAIL_BV.EID,CRM_PUSH_MESSAGE_DETAIL_BV.APP_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_ID,CRM_PUSH_MESSAGE_DETAIL_BV.CAMPAIGNS,CRM_PUSH_MESSAGE_DETAIL_BV.DEVICE_ID,CRM_PUSH_MESSAGE_DETAIL_BV.DATE_TIME_SEND,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_CONTENT,CRM_PUSH_MESSAGE_DETAIL_BV.MESSAGE_OPENED,CRM_PUSH_MESSAGE_DETAIL_BV.OPEN_DATE,CRM_PUSH_MESSAGE_DETAIL_BV.TIME_IN_APP,CRM_PUSH_MESSAGE_DETAIL_BV.PLATFORM,CRM_PUSH_MESSAGE_DETAIL_BV.PLATFORM_VERSION,CRM_PUSH_MESSAGE_DETAIL_BV.STATUS,CRM_PUSH_MESSAGE_DETAIL_BV.SERVICE_RESPONSE,CRM_PUSH_MESSAGE_DETAIL_BV.GEOFENCE_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.TEMPLATE,CRM_PUSH_MESSAGE_DETAIL_BV.FORMAT,CRM_PUSH_MESSAGE_DETAIL_BV.PAGE_NAME,CRM_PUSH_MESSAGE_DETAIL_BV.PUSH_JOB_ID,CRM_PUSH_MESSAGE_DETAIL_BV.SYSTEM_TOKEN,CRM_PUSH_MESSAGE_DETAIL_BV.INBOX_DOWNLOAD,CRM_PUSH_MESSAGE_DETAIL_BV.INBOX_OPEN,CRM_PUSH_MESSAGE_DETAIL_BV.IOS_MEDIA_URL,CRM_PUSH_MESSAGE_DETAIL_BV.ANDROID_MEDIA_URL,CRM_PUSH_MESSAGE_DETAIL_BV.MEDIA_ALT,CRM_PUSH_MESSAGE_DETAIL_BV.CONTACT_KEY,CRM_PUSH_MESSAGE_DETAIL_BV.REQUEST_ID,CRM_PUSH_MESSAGE_DETAIL_BV.SERVICE_TRANSACTION_ID,CRM_PUSH_MESSAGE_DETAIL_BV.LOAD_ID,CRM_PUSH_MESSAGE_DETAIL_BV.LOAD_DTTM,CRM_PUSH_MESSAGE_DETAIL_BV.UPDATE_ID,CRM_PUSH_MESSAGE_DETAIL_BV.UPDATE_DTTM,CRM_PUSH_MESSAGE_DETAIL_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_PUSH_MESSAGE_DETAIL_BV ;
create view IF NOT EXISTS CRM_PUSH_SEND_LOG_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	PUSH_JOB_ID COMMENT 'Push Job Id is the ID of the job, or bundle of send, that included the push notification ',
	PUSH_TRIGGERED_SEND_REQUEST_ID COMMENT 'Push Triggered Send Request Id is TokenID that is returned from the API call. When you use list and data extension sends, is the same as the PushJobID ',
	PUSH_BATCH_ID COMMENT 'Push Batch Id is The ID of the batch, or bundle of jobs, for batched sends',
	SUB_ID COMMENT 'Sub Id is the ID of the subscriber to whom the message was sent',
	DEVICE_ID COMMENT 'Device Id is the ID of the device that received the push notification',
	APP_ID COMMENT 'App Id is The ID of the app that received the push notification ',
	LOG_DATE COMMENT 'Log Date is The date that this data was written to the send log ',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM Push Send Log_BV lists all the push notifications that occur to customers by day.'
 as 
SELECT CRM_PUSH_SEND_LOG.BRAND_ID,CRM_PUSH_SEND_LOG.SOURCE_SYSTEM_NM,CRM_PUSH_SEND_LOG.PUSH_JOB_ID,CRM_PUSH_SEND_LOG.PUSH_TRIGGERED_SEND_REQUEST_ID,CRM_PUSH_SEND_LOG.PUSH_BATCH_ID,CRM_PUSH_SEND_LOG.SUB_ID,CRM_PUSH_SEND_LOG.DEVICE_ID,CRM_PUSH_SEND_LOG.APP_ID,CRM_PUSH_SEND_LOG.LOG_DATE,CRM_PUSH_SEND_LOG.LOAD_ID,CRM_PUSH_SEND_LOG.LOAD_DTTM,CRM_PUSH_SEND_LOG.UPDATE_ID,CRM_PUSH_SEND_LOG.UPDATE_DTTM,CRM_PUSH_SEND_LOG.LOAD_FILENAME
FROM IDS_DEV.CUST.CRM_PUSH_SEND_LOG ;
create view IF NOT EXISTS CRM_SEND_JOB_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
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
	IS_MULTIPART COMMENT 'True/False for whether or not the job has multiple parts.',
	ADDITIONAL COMMENT 'All values currently null',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_SEND_JOB_BV is list of all jobs created to send emails (including to whom).'
 as 
SELECT CRM_SEND_JOB_BV.BRAND_ID,CRM_SEND_JOB_BV.SOURCE_SYSTEM_NM,CRM_SEND_JOB_BV.ACCOUNT_ID,CRM_SEND_JOB_BV.JOB_ID,CRM_SEND_JOB_BV.MESSAGE_ID,CRM_SEND_JOB_BV.FROM_NAME,CRM_SEND_JOB_BV.FROM_EMAIL,CRM_SEND_JOB_BV.SCHED_TIME,CRM_SEND_JOB_BV.SUBJECT_LINE,CRM_SEND_JOB_BV.EMAIL_NAME,CRM_SEND_JOB_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_SEND_JOB_BV.SEND_DEFINITION_EXTERNAL_KEY,CRM_SEND_JOB_BV.JOB_STATUS,CRM_SEND_JOB_BV.PREVIEW_URL,CRM_SEND_JOB_BV.IS_MULTIPART,CRM_SEND_JOB_BV.ADDITIONAL,CRM_SEND_JOB_BV.LOAD_ID,CRM_SEND_JOB_BV.LOAD_DTTM,CRM_SEND_JOB_BV.UPDATE_ID,CRM_SEND_JOB_BV.UPDATE_DTTM,CRM_SEND_JOB_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_SEND_JOB_BV ;
create view IF NOT EXISTS CRM_SEND_LOG_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
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
	DATASOURCE_NAME COMMENT 'Which data source the email recipients are coming from.  For example, Epsilon or Harmony.',
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
	CAMPAIGN_DESCRIPTION COMMENT 'Campaign Description is Campaign Description. Sample Value:  This is the email signup offer.',
	CAMPAIGN_OBJECTIVE COMMENT 'Campaign Objective is Objective of the Send . Sample Value:  Loyalty.',
	CAMPAIGN_START_DATE COMMENT 'Campaign Start Date is Start Date; Format instructions for CRM team on input. Sample Value:  06/07/2021.',
	CAMPAIGN_END_DATE COMMENT 'Campaign End Date is End Date; Format instructions for CRM team on input . Sample Value:  06/10/2021.',
	CAMPAIGN_DURATION COMMENT 'Campaign Duration is Duration of the Journey.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_SEND_LOG_BV is record of every email sent, the offer it contained, and what customer it was sent to.'
 as 
SELECT CRM_SEND_LOG_BV.BRAND_ID,CRM_SEND_LOG_BV.SOURCE_SYSTEM_NM,CRM_SEND_LOG_BV.ACCOUNT_ID,CRM_SEND_LOG_BV.JOB_ID,CRM_SEND_LOG_BV.SUBSCRIBER_KEY,CRM_SEND_LOG_BV.PROFILE_ID,CRM_SEND_LOG_BV.EXTERNAL_ID,CRM_SEND_LOG_BV.CUSTOMER_ID,CRM_SEND_LOG_BV.OFFER_ID,CRM_SEND_LOG_BV.BATCH_ID,CRM_SEND_LOG_BV.LIST_ID,CRM_SEND_LOG_BV.CREATIVE_VARIANT,CRM_SEND_LOG_BV.EVENT_DATE,CRM_SEND_LOG_BV.OFFER_CODE,CRM_SEND_LOG_BV.SUB_ID,CRM_SEND_LOG_BV.TRIGGERED_SEND_ID,CRM_SEND_LOG_BV.ERROR_CODE,CRM_SEND_LOG_BV.DATASOURCE_NAME,CRM_SEND_LOG_BV.EMAIL_ADDRESS,CRM_SEND_LOG_BV.OFFER_NAME,CRM_SEND_LOG_BV.PARENT_OFFER_ID,CRM_SEND_LOG_BV.JOURNEY_NAME,CRM_SEND_LOG_BV.JOURNEY_STEP,CRM_SEND_LOG_BV.STRENGTH_OF_CUSTOMER,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_1,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_2,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_3,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_4,CRM_SEND_LOG_BV.USER_DEFINED_SEGMENT_5,CRM_SEND_LOG_BV.OFFER_DECISION_LOGIC,CRM_SEND_LOG_BV.POINT_BALANCE,CRM_SEND_LOG_BV.EMAIL_NAME,CRM_SEND_LOG_BV.CAMPAIGN_ID,CRM_SEND_LOG_BV.CAMPAIGN_NAME,CRM_SEND_LOG_BV.CAMPAIGN_TYPE,CRM_SEND_LOG_BV.CAMPAIGN_CATEGORY,CRM_SEND_LOG_BV.CAMPAIGN_DESCRIPTION,CRM_SEND_LOG_BV.CAMPAIGN_OBJECTIVE,CRM_SEND_LOG_BV.CAMPAIGN_START_DATE,CRM_SEND_LOG_BV.CAMPAIGN_END_DATE,CRM_SEND_LOG_BV.CAMPAIGN_DURATION,CRM_SEND_LOG_BV.LOAD_ID,CRM_SEND_LOG_BV.LOAD_DTTM,CRM_SEND_LOG_BV.UPDATE_ID,CRM_SEND_LOG_BV.UPDATE_DTTM,CRM_SEND_LOG_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_SEND_LOG_BV ;
create view IF NOT EXISTS CRM_SENT_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
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
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_SENT_BV contains a list of emails that were sent successfully.'
 as 
SELECT CRM_SENT_BV.BRAND_ID,CRM_SENT_BV.SOURCE_SYSTEM_NM,CRM_SENT_BV.ACCOUNT_ID,CRM_SENT_BV.JOB_ID,CRM_SENT_BV.SUBSCRIBER_KEY,CRM_SENT_BV.MESSAGE_ID,CRM_SENT_BV.SERVICE_TRANSACTION_ID,CRM_SENT_BV.EMAIL_ADDRESS,CRM_SENT_BV.SUBSCRIBER_ID,CRM_SENT_BV.LIST_ID,CRM_SENT_BV.EVENT_DATE,CRM_SENT_BV.EVENT_TYPE,CRM_SENT_BV.BATCH_ID,CRM_SENT_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_SENT_BV.LOAD_ID,CRM_SENT_BV.LOAD_DTTM,CRM_SENT_BV.UPDATE_ID,CRM_SENT_BV.UPDATE_DTTM,CRM_SENT_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_SENT_BV ;
create view IF NOT EXISTS CRM_STATUS_CHANGE_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	OLD_STATUS COMMENT 'The old status of affected subscribers',
	NEW_STATUS COMMENT 'The new status of affected subscribers',
	DATE_CHANGED COMMENT 'The date the status changed',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	EMAIL_ADDRESS COMMENT 'The email address associated with the subscriber key',
	SUBSCRIBER_ID COMMENT 'The subscriber ID for the affected subscriber, This number represents the unique ID for each subscriber record',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_STATUS_CHANGE_BV contains list of  status changes per user.'
 as 
SELECT CRM_STATUS_CHANGE_BV.BRAND_ID,CRM_STATUS_CHANGE_BV.SOURCE_SYSTEM_NM,CRM_STATUS_CHANGE_BV.ACCOUNT_ID,CRM_STATUS_CHANGE_BV.SUBSCRIBER_KEY,CRM_STATUS_CHANGE_BV.OLD_STATUS,CRM_STATUS_CHANGE_BV.NEW_STATUS,CRM_STATUS_CHANGE_BV.DATE_CHANGED,CRM_STATUS_CHANGE_BV.SERVICE_TRANSACTION_ID,CRM_STATUS_CHANGE_BV.EMAIL_ADDRESS,CRM_STATUS_CHANGE_BV.SUBSCRIBER_ID,CRM_STATUS_CHANGE_BV.LOAD_ID,CRM_STATUS_CHANGE_BV.LOAD_DTTM,CRM_STATUS_CHANGE_BV.UPDATE_ID,CRM_STATUS_CHANGE_BV.UPDATE_DTTM,CRM_STATUS_CHANGE_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_STATUS_CHANGE_BV ;
create view IF NOT EXISTS CRM_UNSUBSCRIBE_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ACCOUNT_ID COMMENT 'Account Identifier represents the On-Your-Behalf (OYB)  Account number is the client/account ID number for any related On-Your-Behalf (OYB) accounts. This field applies to enterprise accounts only.  Sample values for Client ID - BWW is 100040982 and Arbys is 100039504.',
	JOB_ID COMMENT 'The Job ID number for the email send',
	SUBSCRIBER_KEY COMMENT 'The subscriber key for the affected subscriber',
	MESSAGE_ID COMMENT 'Message Identifier is a unique identifier for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. A message may have multiple service transactions.',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
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
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM_UNSUBSCRIBE_BV list of users who unsubscribed from email communication.'
 as 
SELECT CRM_UNSUBSCRIBE_BV.BRAND_ID,CRM_UNSUBSCRIBE_BV.SOURCE_SYSTEM_NM,CRM_UNSUBSCRIBE_BV.ACCOUNT_ID,CRM_UNSUBSCRIBE_BV.JOB_ID,CRM_UNSUBSCRIBE_BV.SUBSCRIBER_KEY,CRM_UNSUBSCRIBE_BV.MESSAGE_ID,CRM_UNSUBSCRIBE_BV.SERVICE_TRANSACTION_ID,CRM_UNSUBSCRIBE_BV.EMAIL_ADDRESS,CRM_UNSUBSCRIBE_BV.SUBSCRIBER_ID,CRM_UNSUBSCRIBE_BV.LIST_ID,CRM_UNSUBSCRIBE_BV.EVENT_DATE,CRM_UNSUBSCRIBE_BV.EVENT_TYPE,CRM_UNSUBSCRIBE_BV.BATCH_ID,CRM_UNSUBSCRIBE_BV.TRIGGERED_SEND_EXTERNAL_KEY,CRM_UNSUBSCRIBE_BV.LOAD_ID,CRM_UNSUBSCRIBE_BV.LOAD_DTTM,CRM_UNSUBSCRIBE_BV.UPDATE_ID,CRM_UNSUBSCRIBE_BV.UPDATE_DTTM,CRM_UNSUBSCRIBE_BV.LOAD_FILENAME
FROM IDS_DEV.CUST_BV.CRM_UNSUBSCRIBE_BV ;
create view IF NOT EXISTS CUSTOMER_BV(
	CUSTOMERID,
	LOYALTYPROGRAMID,
	CLOSESTSTOREID,
	HOUSEHOLDID,
	EMAILID,
	MOBILEDEVICEID,
	INSPIREID,
	AGENCYID,
	AGENCYSTATUS,
	INSPIRECUSTOMERTYPE,
	MDMID,
	ISMDMIDDELETED,
	LOYALTYCARDNUMBER,
	FIRSTNAME,
	LASTNAME,
	MIDDLEINITIAL,
	DOB,
	BIRTHMONTH,
	BIRTHYEAR,
	ISBIRTHDATEIMPLIED,
	GENDER,
	ENROLLMENTCHANNEL,
	CUSTOMERSTATUS,
	POINTBALANCE,
	MEMBERSHIPSTATUS,
	PROFILECOMPLETEDSTATUS,
	DELIVERABILITYSTATUS,
	MOBILENUMBER,
	ADDRESSLINE1,
	ADDRESSLINE2,
	CITY,
	STATE,
	ZIPCODE,
	COUNTRYCODE,
	EMAILOPTOUTFLAG,
	PUSHNOTIFICATIONOPTIN,
	SMSOPTIN,
	PRIVACY,
	UNSUBSCRIBEDATE,
	POINTSEXPIREDATE,
	ENROLLSTARTDATE,
	LASTLOGINDATE,
	LASTSTATUSCHANGEDATE,
	PROFILECOMPLETIONDATE,
	SUBSCRIBERKEY,
	SUBSCRIBERSOURCENAME,
	LOYALTY_TIER_CHANGE_DTTM,
	LOYALTY_TIER_NM,
	LOYALTY_TIER_EXPIRATION_DT,
	LOYALTY_ELITE_VISIT_CNT,
	BRANDID,
	SOURCE,
	CDMLOADDATE,
	LOADID,
	LOADDTTM
) as 
 SELECT 
    CUSTOMERID,
	LOYALTYPROGRAMID,
	CLOSESTSTOREID,
	HOUSEHOLDID,
	EMAILID,
	MOBILEDEVICEID,
	INSPIREID,
	AGENCYID,
	AGENCYSTATUS,
	INSPIRECUSTOMERTYPE,
	MDMID,
	ISMDMIDDELETED,
	LOYALTYCARDNUMBER,
	FIRSTNAME,
	LASTNAME,
	MIDDLEINITIAL,
	DOB,
	BIRTHMONTH,
	BIRTHYEAR,
	ISBIRTHDATEIMPLIED,
	GENDER,
	ENROLLMENTCHANNEL,
	CUSTOMERSTATUS,
	POINTBALANCE,
	MEMBERSHIPSTATUS,
	PROFILECOMPLETEDSTATUS,
	DELIVERABILITYSTATUS,
	MOBILENUMBER,
	ADDRESSLINE1,
	ADDRESSLINE2,
	CITY,
	STATE,
	ZIPCODE,
	COUNTRYCODE,
	EMAILOPTOUTFLAG,
	PUSHNOTIFICATIONOPTIN,
	SMSOPTIN,
	PRIVACY,
	UNSUBSCRIBEDATE,
	POINTSEXPIREDATE,
	ENROLLSTARTDATE,
	LASTLOGINDATE,
	LASTSTATUSCHANGEDATE,
	PROFILECOMPLETIONDATE,
	SUBSCRIBERKEY,
	SUBSCRIBERSOURCENAME,
	LOYALTY_TIER_CHANGE_DTTM,
    LOYALTY_TIER_NM,
    LOYALTY_TIER_EXPIRATION_DT,
    LOYALTY_ELITE_VISIT_CNT,
	BRANDID,
	SOURCE,
	CDMLOADDATE,
	LOADID,
	LOADDTTM
  FROM POLARIS_DEV.CUDM.CUSTOMER_DIM;
create view IF NOT EXISTS CUSTOMER_V(
	MEMBER_ID,
	LOYALTY_PROGRAM_ID,
	CLOSEST_STORE_ID,
	HOUSEHOLD_ID,
	EMAIL_ID,
	MOBILE_DEVICE_ID,
	BRAND_ID,
	MEMBER_INSPIRE_ID,
	EXPERIAN_ID,
	EXPERIAN_STATUS_TXT,
	INSPIRE_CUSTOMER_TYP,
	MDM_ID,
	MDM_ID_DELETED_IND,
	LOYALTY_CARD_NBR,
	FIRST_NM,
	LAST_NM,
	MIDDLE_INITIAL_TXT,
	DOB_DT,
	BIRTH_MONTH_NBR,
	BIRTH_YEAR_NBR,
	IS_BIRTH_DATE_IMPLIED_IND,
	GENDER_TYP,
	ENROLLMENT_CHANNEL_TYP,
	MEMBER_STATUS_CD,
	POINT_BALANCE_NBR,
	MEMBERSHIP_STATUS_CD,
	PROFILE_COMPLETED_STATUS_IND,
	DELIVER_ABILITY_STATUS_IND,
	MOBILE_NBR,
	ADDRESS_LINE_1_TXT,
	ADDRESS_LINE_2_TXT,
	CITY_NM,
	STATE_CD,
	ZIP_CD,
	COUNTRY_CD,
	EMAIL_OPT_OUT_IND,
	PUSH_NOTIFICATION_OPT_IN_IND,
	SMS_OPT_IN_IND,
	PRIVACY_IND,
	UNSUBSCRIBE_DT,
	POINTS_EXPIRE_DT,
	ENROLL_START_DT,
	LAST_LOGIN_DT,
	LAST_STATUS_CHANGE_DT,
	PROFILE_COMPLETION_DT,
	SOURCE_SYSTEM_NM,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM,
	SUBSCRIBER_KEY,
	SUBSCRIBER_SOURCE_NM,
	LOYALTY_TIER_CHANGE_DTTM,
	LOYALTY_TIER_NM,
	LOYALTY_TIER_EXPIRATION_DT,
	LOYALTY_ELITE_VISIT_CNT
) as SELECT 
CUSTOMER_DIM.CUSTOMERID,CUSTOMER_DIM.LOYALTYPROGRAMID,CUSTOMER_DIM.CLOSESTSTOREID,CUSTOMER_DIM.HOUSEHOLDID,CUSTOMER_DIM.EMAILID
,CUSTOMER_DIM.MOBILEDEVICEID,CUSTOMER_DIM.BRANDID,CUSTOMER_DIM.INSPIREID,CUSTOMER_DIM.AGENCYID,CUSTOMER_DIM.AGENCYSTATUS
,CUSTOMER_DIM.INSPIRECUSTOMERTYPE,CUSTOMER_DIM.MDMID,CUSTOMER_DIM.ISMDMIDDELETED,CUSTOMER_DIM.LOYALTYCARDNUMBER,CUSTOMER_DIM.FIRSTNAME,CUSTOMER_DIM.LASTNAME
,CUSTOMER_DIM.MIDDLEINITIAL,CUSTOMER_DIM.DOB,CUSTOMER_DIM.BIRTHMONTH,CUSTOMER_DIM.BIRTHYEAR,CUSTOMER_DIM.ISBIRTHDATEIMPLIED
,CUSTOMER_DIM.GENDER,CUSTOMER_DIM.ENROLLMENTCHANNEL
,CUSTOMER_DIM.CUSTOMERSTATUS
,CUSTOMER_DIM.POINTBALANCE
,CUSTOMER_DIM.MEMBERSHIPSTATUS
,CUSTOMER_DIM.PROFILECOMPLETEDSTATUS
,CUSTOMER_DIM.DELIVERABILITYSTATUS
,CUSTOMER_DIM.MOBILENUMBER
,CUSTOMER_DIM.ADDRESSLINE1
,CUSTOMER_DIM.ADDRESSLINE2
,CUSTOMER_DIM.CITY
,CUSTOMER_DIM.STATE
,CUSTOMER_DIM.ZIPCODE
,CUSTOMER_DIM.COUNTRYCODE
,CUSTOMER_DIM.EMAILOPTOUTFLAG 
,CUSTOMER_DIM.PUSHNOTIFICATIONOPTIN
,CUSTOMER_DIM.SMSOPTIN
,CUSTOMER_DIM.PRIVACY
,CUSTOMER_DIM.UNSUBSCRIBEDATE
,CUSTOMER_DIM.POINTSEXPIREDATE
,CUSTOMER_DIM.ENROLLSTARTDATE
,CUSTOMER_DIM.LASTLOGINDATE
,CUSTOMER_DIM.LASTSTATUSCHANGEDATE
,CUSTOMER_DIM.PROFILECOMPLETIONDATE
,CUSTOMER_DIM.SOURCE
,CUSTOMER_DIM.CDMLOADDATE
,CUSTOMER_DIM.LOADID
,CUSTOMER_DIM.LOADDTTM
,CUSTOMER_DIM.SUBSCRIBERKEY
,CUSTOMER_DIM.SUBSCRIBERSOURCENAME
,CUSTOMER_DIM.LOYALTY_TIER_CHANGE_DTTM
,CUSTOMER_DIM.LOYALTY_TIER_NM
,CUSTOMER_DIM.LOYALTY_TIER_EXPIRATION_DT
,CUSTOMER_DIM.LOYALTY_ELITE_VISIT_CNT
FROM POLARIS_DEV.CUDM.CUSTOMER_DIM;
create view IF NOT EXISTS CUSTOMER_V_TEST_20210517(
	MEMBER_ID,
	LOYALTY_PROGRAM_ID,
	CLOSEST_STORE_ID,
	HOUSEHOLD_ID,
	EMAIL_ID MASKING POLICY #unknown_policy,
	MOBILE_DEVICE_ID,
	BRAND_ID,
	MEMBER_INSPIRE_ID,
	EXPERIAN_ID,
	EXPERIAN_STATUS_TXT,
	INSPIRE_CUSTOMER_TYP,
	MDM_ID,
	MDM_ID_DELETED_IND,
	LOYALTY_CARD_NBR,
	FIRST_NM,
	LAST_NM,
	MIDDLE_INITIAL_TXT,
	DOB_DT,
	BIRTH_MONTH_NBR,
	BIRTH_YEAR_NBR,
	IS_BIRTH_DATE_IMPLIED_IND,
	GENDER_TYP,
	ENROLLMENT_CHANNEL_TYP,
	MEMBER_STATUS_CD,
	POINT_BALANCE_NBR,
	MEMBERSHIP_STATUS_CD,
	PROFILE_COMPLETED_STATUS_IND,
	DELIVER_ABILITY_STATUS_IND,
	MOBILE_NBR,
	ADDRESS_LINE_1_TXT,
	ADDRESS_LINE_2_TXT,
	CITY_NM,
	STATE_CD,
	ZIP_CD,
	COUNTRY_CD,
	EMAIL_OPT_OUT_IND,
	PUSH_NOTIFICATION_OPT_IN_IND,
	SMS_OPT_IN_IND,
	PRIVACY_IND,
	UNSUBSCRIBE_DT,
	POINTS_EXPIRE_DT,
	ENROLL_START_DT,
	LAST_LOGIN_DT,
	LAST_STATUS_CHANGE_DT,
	PROFILE_COMPLETION_DT,
	SOURCE_SYSTEM_NM,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM,
	SUBSCRIBER_KEY,
	SUBSCRIBER_SOURCE_NM
) as SELECT 
CUSTOMER_DIM.CUSTOMERID,CUSTOMER_DIM.LOYALTYPROGRAMID,CUSTOMER_DIM.CLOSESTSTOREID,CUSTOMER_DIM.HOUSEHOLDID,CUSTOMER_DIM.EMAILID
,CUSTOMER_DIM.MOBILEDEVICEID,CUSTOMER_DIM.BRANDID,CUSTOMER_DIM.INSPIREID,CUSTOMER_DIM.AGENCYID,CUSTOMER_DIM.AGENCYSTATUS
,CUSTOMER_DIM.INSPIRECUSTOMERTYPE,CUSTOMER_DIM.MDMID,CUSTOMER_DIM.ISMDMIDDELETED,CUSTOMER_DIM.LOYALTYCARDNUMBER,CUSTOMER_DIM.FIRSTNAME,CUSTOMER_DIM.LASTNAME
,CUSTOMER_DIM.MIDDLEINITIAL,CUSTOMER_DIM.DOB,CUSTOMER_DIM.BIRTHMONTH,CUSTOMER_DIM.BIRTHYEAR,CUSTOMER_DIM.ISBIRTHDATEIMPLIED
,CUSTOMER_DIM.GENDER,CUSTOMER_DIM.ENROLLMENTCHANNEL
,CUSTOMER_DIM.CUSTOMERSTATUS
,CUSTOMER_DIM.POINTBALANCE
,CUSTOMER_DIM.MEMBERSHIPSTATUS
,CUSTOMER_DIM.PROFILECOMPLETEDSTATUS
,CUSTOMER_DIM.DELIVERABILITYSTATUS
,CUSTOMER_DIM.MOBILENUMBER
,CUSTOMER_DIM.ADDRESSLINE1
,CUSTOMER_DIM.ADDRESSLINE2
,CUSTOMER_DIM.CITY
,CUSTOMER_DIM.STATE
,CUSTOMER_DIM.ZIPCODE
,CUSTOMER_DIM.COUNTRYCODE
,CUSTOMER_DIM.EMAILOPTOUTFLAG 
,CUSTOMER_DIM.PUSHNOTIFICATIONOPTIN
,CUSTOMER_DIM.SMSOPTIN
,CUSTOMER_DIM.PRIVACY
,CUSTOMER_DIM.UNSUBSCRIBEDATE
,CUSTOMER_DIM.POINTSEXPIREDATE
,CUSTOMER_DIM.ENROLLSTARTDATE
,CUSTOMER_DIM.LASTLOGINDATE
,CUSTOMER_DIM.LASTSTATUSCHANGEDATE
,CUSTOMER_DIM.PROFILECOMPLETIONDATE
,CUSTOMER_DIM.SOURCE
,CUSTOMER_DIM.CDMLOADDATE
,CUSTOMER_DIM.LOADID
,CUSTOMER_DIM.LOADDTTM
,CUSTOMER_DIM.SUBSCRIBERKEY
,CUSTOMER_DIM.SUBSCRIBERSOURCENAME
FROM POLARIS_DEV.CUDM.CUSTOMER_DIM;
create view IF NOT EXISTS GUEST_EXPERIENCE_DAY_PART_OSAT_V(
	BRAND_ID,
	STORE_ID,
	BUSINESS_DT,
	DAY_PART_NM,
	DAY_OF_WEEK,
	FISC_WEEK_NBR,
	FISC_WEEK_START_DT,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DT,
	FISC_QUARTER_NBR,
	FISC_YEAR_NBR,
	OSAT_RESPONSE_CNT,
	OSAT_5_CNT,
	OSAT_5_PCT,
	OSAT_4_CNT,
	OSAT_4_PCT,
	OSAT_3_CNT,
	OSAT_3_PCT,
	OSAT_2_CNT,
	OSAT_2_PCT,
	OSAT_1_CNT,
	OSAT_1_PCT,
	OSAT_CLEANLINESS_RESPONSE_CNT,
	OSAT_CLEANLINESS_5_CNT,
	OSAT_CLEANLINESS_5_PCT,
	OSAT_CLEANLINESS_4_CNT,
	OSAT_CLEANLINESS_4_PCT,
	OSAT_CLEANLINESS_3_CNT,
	OSAT_CLEANLINESS_3_PCT,
	OSAT_CLEANLINESS_2_CNT,
	OSAT_CLEANLINESS_2_PCT,
	OSAT_CLEANLINESS_1_CNT,
	OSAT_CLEANLINESS_1_PCT,
	OSAT_ACCURACY_RESPONSE_CNT,
	OSAT_ACCURACY_5_CNT,
	OSAT_ACCURACY_5_PCT,
	OSAT_ACCURACY_4_CNT,
	OSAT_ACCURACY_4_PCT,
	OSAT_ACCURACY_3_CNT,
	OSAT_ACCURACY_3_PCT,
	OSAT_ACCURACY_2_CNT,
	OSAT_ACCURACY_2_PCT,
	OSAT_ACCURACY_1_CNT,
	OSAT_ACCURACY_1_PCT,
	OSAT_FOOD_TASTE_RESPONSE_CNT,
	OSAT_FOOD_TASTE_5_CNT,
	OSAT_FOOD_TASTE_5_PCT,
	OSAT_FOOD_TASTE_4_CNT,
	OSAT_FOOD_TASTE_4_PCT,
	OSAT_FOOD_TASTE_3_CNT,
	OSAT_FOOD_TASTE_3_PCT,
	OSAT_FOOD_TASTE_2_CNT,
	OSAT_FOOD_TASTE_2_PCT,
	OSAT_FOOD_TASTE_1_CNT,
	OSAT_FOOD_TASTE_1_PCT,
	OSAT_FRIENDLINESS_RESPONSE_CNT,
	OSAT_FRIENDLINESS_5_CNT,
	OSAT_FRIENDLINESS_5_PCT,
	OSAT_FRIENDLINESS_4_CNT,
	OSAT_FRIENDLINESS_4_PCT,
	OSAT_FRIENDLINESS_3_CNT,
	OSAT_FRIENDLINESS_3_PCT,
	OSAT_FRIENDLINESS_2_CNT,
	OSAT_FRIENDLINESS_2_PCT,
	OSAT_FRIENDLINESS_1_CNT,
	OSAT_FRIENDLINESS_1_PCT,
	OSAT_SERVICE_SPEED_RESPONSE_CNT,
	OSAT_SERVICE_SPEED_5_CNT,
	OSAT_SERVICE_SPEED_5_PCT,
	OSAT_SERVICE_SPEED_4_CNT,
	OSAT_SERVICE_SPEED_4_PCT,
	OSAT_SERVICE_SPEED_3_CNT,
	OSAT_SERVICE_SPEED_3_PCT,
	OSAT_SERVICE_SPEED_2_CNT,
	OSAT_SERVICE_SPEED_2_PCT,
	OSAT_SERVICE_SPEED_1_CNT,
	OSAT_SERVICE_SPEED_1_PCT,
	LIKE_TO_RETURN_RESPONSE_CNT,
	LIKE_TO_RETURN_SCORE_5_CNT,
	LIKE_TO_RETURN_SCORE_5_PCT,
	LIKE_TO_RETURN_SCORE_4_CNT,
	LIKE_TO_RETURN_SCORE_4_PCT,
	LIKE_TO_RETURN_SCORE_3_CNT,
	LIKE_TO_RETURN_SCORE_3_PCT,
	LIKE_TO_RETURN_SCORE_2_CNT,
	LIKE_TO_RETURN_SCORE_2_PCT,
	LIKE_TO_RETURN_SCORE_1_CNT,
	LIKE_TO_RETURN_SCORE_1_PCT,
	DMA_CD,
	DMA_NM,
	OWNERSHIP_TYP,
	LOC_LEVEL1_NM,
	LOC_LEVEL1_MANAGER_NM,
	LOC_LEVEL2_NM,
	LOC_LEVEL2_MANAGER_NM,
	LOC_LEVEL3_NM,
	LOC_LEVEL3_MANAGER_NM,
	LOC_LEVEL4_NM,
	LOC_LEVEL4_MANAGER_NM,
	LOC_LEVEL5_NM,
	LOC_LEVEL5_MANAGER_NM,
	STORE_CONFIG
) as
SELECT 
BRAND_ID , STORE_ID , BUSINESS_DT, DAY_PART_NM 
  , DAY_OF_WEEK 
  , FISC_WEEK_NBR , FISC_WEEK_START_DT 
  , FISC_PERIOD_NBR , FISC_PERIOD_START_DT 
  , FISC_QUARTER_NBR , FISC_YEAR_NBR 
  , OSAT_RESPONSE_CNT 
  , OSAT_5_CNT , OSAT_5_PCT 
  , OSAT_4_CNT , OSAT_4_PCT 
  , OSAT_3_CNT , OSAT_3_PCT 
  , OSAT_2_CNT , OSAT_2_PCT 
  , OSAT_1_CNT , OSAT_1_PCT 
  , OSAT_CLEANLINESS_RESPONSE_CNT 
  , OSAT_CLEANLINESS_5_CNT , OSAT_CLEANLINESS_5_PCT
  , OSAT_CLEANLINESS_4_CNT , OSAT_CLEANLINESS_4_PCT 
  , OSAT_CLEANLINESS_3_CNT , OSAT_CLEANLINESS_3_PCT 
  , OSAT_CLEANLINESS_2_CNT , OSAT_CLEANLINESS_2_PCT 
  , OSAT_CLEANLINESS_1_CNT , OSAT_CLEANLINESS_1_PCT 
  , OSAT_ACCURACY_RESPONSE_CNT 
  , OSAT_ACCURACY_5_CNT , OSAT_ACCURACY_5_PCT 
  , OSAT_ACCURACY_4_CNT , OSAT_ACCURACY_4_PCT 
  , OSAT_ACCURACY_3_CNT , OSAT_ACCURACY_3_PCT 
  , OSAT_ACCURACY_2_CNT , OSAT_ACCURACY_2_PCT 
  , OSAT_ACCURACY_1_CNT , OSAT_ACCURACY_1_PCT 
  , OSAT_FOOD_TASTE_RESPONSE_CNT 
  , OSAT_FOOD_TASTE_5_CNT , OSAT_FOOD_TASTE_5_PCT 
  , OSAT_FOOD_TASTE_4_CNT , OSAT_FOOD_TASTE_4_PCT
  , OSAT_FOOD_TASTE_3_CNT , OSAT_FOOD_TASTE_3_PCT 
  , OSAT_FOOD_TASTE_2_CNT , OSAT_FOOD_TASTE_2_PCT 
  , OSAT_FOOD_TASTE_1_CNT , OSAT_FOOD_TASTE_1_PCT 
  , OSAT_FRIENDLINESS_RESPONSE_CNT 
  , OSAT_FRIENDLINESS_5_CNT , OSAT_FRIENDLINESS_5_PCT 
  , OSAT_FRIENDLINESS_4_CNT , OSAT_FRIENDLINESS_4_PCT 
  , OSAT_FRIENDLINESS_3_CNT , OSAT_FRIENDLINESS_3_PCT 
  , OSAT_FRIENDLINESS_2_CNT , OSAT_FRIENDLINESS_2_PCT 
  , OSAT_FRIENDLINESS_1_CNT , OSAT_FRIENDLINESS_1_PCT 
  , OSAT_SERVICE_SPEED_RESPONSE_CNT 
  , OSAT_SERVICE_SPEED_5_CNT , OSAT_SERVICE_SPEED_5_PCT 
  , OSAT_SERVICE_SPEED_4_CNT , OSAT_SERVICE_SPEED_4_PCT 
  , OSAT_SERVICE_SPEED_3_CNT , OSAT_SERVICE_SPEED_3_PCT 
  , OSAT_SERVICE_SPEED_2_CNT , OSAT_SERVICE_SPEED_2_PCT 
  , OSAT_SERVICE_SPEED_1_CNT , OSAT_SERVICE_SPEED_1_PCT 
  , LIKE_TO_RETURN_RESPONSE_CNT 
  , LIKE_TO_RETURN_SCORE_5_CNT , LIKE_TO_RETURN_SCORE_5_PCT
  , LIKE_TO_RETURN_SCORE_4_CNT , LIKE_TO_RETURN_SCORE_4_PCT
  , LIKE_TO_RETURN_SCORE_3_CNT , LIKE_TO_RETURN_SCORE_3_PCT
  , LIKE_TO_RETURN_SCORE_2_CNT , LIKE_TO_RETURN_SCORE_2_PCT
  , LIKE_TO_RETURN_SCORE_1_CNT , LIKE_TO_RETURN_SCORE_1_PCT
  , DMA_CD , DMA_NM --moved cols per ODI-7558
  , OWNERSHIP_TYP --moved cols per ODI-7558
  , LOC_LEVEL1_NM , LOC_LEVEL1_MANAGER_NM 
  , LOC_LEVEL2_NM , LOC_LEVEL2_MANAGER_NM 
  , LOC_LEVEL3_NM , LOC_LEVEL3_MANAGER_NM 
  , LOC_LEVEL4_NM , LOC_LEVEL4_MANAGER_NM 
  , LOC_LEVEL5_NM , LOC_LEVEL5_MANAGER_NM 
  , STORE_CONFIG 
FROM TABLE( IDS_DEV.CUST.GUEST_EXPERIENCE_OSAT_INIT('DAY_PART') )
;
create view IF NOT EXISTS GUEST_EXPERIENCE_ORDER_FULFILLMENT_OSAT_V(
	BRAND_ID,
	STORE_ID,
	BUSINESS_DT,
	SURVEY_ORDER_FULFILLMENT_NM,
	DAY_OF_WEEK,
	FISC_WEEK_NBR,
	FISC_WEEK_START_DT,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DT,
	FISC_QUARTER_NBR,
	FISC_YEAR_NBR,
	OSAT_RESPONSE_CNT,
	OSAT_5_CNT,
	OSAT_5_PCT,
	OSAT_4_CNT,
	OSAT_4_PCT,
	OSAT_3_CNT,
	OSAT_3_PCT,
	OSAT_2_CNT,
	OSAT_2_PCT,
	OSAT_1_CNT,
	OSAT_1_PCT,
	OSAT_CLEANLINESS_RESPONSE_CNT,
	OSAT_CLEANLINESS_5_CNT,
	OSAT_CLEANLINESS_5_PCT,
	OSAT_CLEANLINESS_4_CNT,
	OSAT_CLEANLINESS_4_PCT,
	OSAT_CLEANLINESS_3_CNT,
	OSAT_CLEANLINESS_3_PCT,
	OSAT_CLEANLINESS_2_CNT,
	OSAT_CLEANLINESS_2_PCT,
	OSAT_CLEANLINESS_1_CNT,
	OSAT_CLEANLINESS_1_PCT,
	OSAT_ACCURACY_RESPONSE_CNT,
	OSAT_ACCURACY_5_CNT,
	OSAT_ACCURACY_5_PCT,
	OSAT_ACCURACY_4_CNT,
	OSAT_ACCURACY_4_PCT,
	OSAT_ACCURACY_3_CNT,
	OSAT_ACCURACY_3_PCT,
	OSAT_ACCURACY_2_CNT,
	OSAT_ACCURACY_2_PCT,
	OSAT_ACCURACY_1_CNT,
	OSAT_ACCURACY_1_PCT,
	OSAT_FOOD_TASTE_RESPONSE_CNT,
	OSAT_FOOD_TASTE_5_CNT,
	OSAT_FOOD_TASTE_5_PCT,
	OSAT_FOOD_TASTE_4_CNT,
	OSAT_FOOD_TASTE_4_PCT,
	OSAT_FOOD_TASTE_3_CNT,
	OSAT_FOOD_TASTE_3_PCT,
	OSAT_FOOD_TASTE_2_CNT,
	OSAT_FOOD_TASTE_2_PCT,
	OSAT_FOOD_TASTE_1_CNT,
	OSAT_FOOD_TASTE_1_PCT,
	OSAT_FRIENDLINESS_RESPONSE_CNT,
	OSAT_FRIENDLINESS_5_CNT,
	OSAT_FRIENDLINESS_5_PCT,
	OSAT_FRIENDLINESS_4_CNT,
	OSAT_FRIENDLINESS_4_PCT,
	OSAT_FRIENDLINESS_3_CNT,
	OSAT_FRIENDLINESS_3_PCT,
	OSAT_FRIENDLINESS_2_CNT,
	OSAT_FRIENDLINESS_2_PCT,
	OSAT_FRIENDLINESS_1_CNT,
	OSAT_FRIENDLINESS_1_PCT,
	OSAT_SERVICE_SPEED_RESPONSE_CNT,
	OSAT_SERVICE_SPEED_5_CNT,
	OSAT_SERVICE_SPEED_5_PCT,
	OSAT_SERVICE_SPEED_4_CNT,
	OSAT_SERVICE_SPEED_4_PCT,
	OSAT_SERVICE_SPEED_3_CNT,
	OSAT_SERVICE_SPEED_3_PCT,
	OSAT_SERVICE_SPEED_2_CNT,
	OSAT_SERVICE_SPEED_2_PCT,
	OSAT_SERVICE_SPEED_1_CNT,
	OSAT_SERVICE_SPEED_1_PCT,
	LIKE_TO_RETURN_RESPONSE_CNT,
	LIKE_TO_RETURN_SCORE_5_CNT,
	LIKE_TO_RETURN_SCORE_5_PCT,
	LIKE_TO_RETURN_SCORE_4_CNT,
	LIKE_TO_RETURN_SCORE_4_PCT,
	LIKE_TO_RETURN_SCORE_3_CNT,
	LIKE_TO_RETURN_SCORE_3_PCT,
	LIKE_TO_RETURN_SCORE_2_CNT,
	LIKE_TO_RETURN_SCORE_2_PCT,
	LIKE_TO_RETURN_SCORE_1_CNT,
	LIKE_TO_RETURN_SCORE_1_PCT,
	DMA_CD,
	DMA_NM,
	OWNERSHIP_TYP,
	LOC_LEVEL1_NM,
	LOC_LEVEL1_MANAGER_NM,
	LOC_LEVEL2_NM,
	LOC_LEVEL2_MANAGER_NM,
	LOC_LEVEL3_NM,
	LOC_LEVEL3_MANAGER_NM,
	LOC_LEVEL4_NM,
	LOC_LEVEL4_MANAGER_NM,
	LOC_LEVEL5_NM,
	LOC_LEVEL5_MANAGER_NM,
	STORE_CONFIG
) as
SELECT 
f.BRAND_ID , f.STORE_ID , f.BUSINESS_DT, sof.SURVEY_ORDER_FULFILLMENT_NM 
  , f.DAY_OF_WEEK 
  , f.FISC_WEEK_NBR , f.FISC_WEEK_START_DT 
  , f.FISC_PERIOD_NBR , f.FISC_PERIOD_START_DT 
  , f.FISC_QUARTER_NBR , f.FISC_YEAR_NBR 
  , f.OSAT_RESPONSE_CNT 
  , f.OSAT_5_CNT , f.OSAT_5_PCT 
  , f.OSAT_4_CNT , f.OSAT_4_PCT 
  , f.OSAT_3_CNT , f.OSAT_3_PCT 
  , f.OSAT_2_CNT , f.OSAT_2_PCT 
  , f.OSAT_1_CNT , f.OSAT_1_PCT 
  , f.OSAT_CLEANLINESS_RESPONSE_CNT 
  , f.OSAT_CLEANLINESS_5_CNT , f.OSAT_CLEANLINESS_5_PCT
  , f.OSAT_CLEANLINESS_4_CNT , f.OSAT_CLEANLINESS_4_PCT 
  , f.OSAT_CLEANLINESS_3_CNT , f.OSAT_CLEANLINESS_3_PCT 
  , f.OSAT_CLEANLINESS_2_CNT , f.OSAT_CLEANLINESS_2_PCT 
  , f.OSAT_CLEANLINESS_1_CNT , f.OSAT_CLEANLINESS_1_PCT 
  , f.OSAT_ACCURACY_RESPONSE_CNT 
  , f.OSAT_ACCURACY_5_CNT , f.OSAT_ACCURACY_5_PCT 
  , f.OSAT_ACCURACY_4_CNT , f.OSAT_ACCURACY_4_PCT 
  , f.OSAT_ACCURACY_3_CNT , f.OSAT_ACCURACY_3_PCT 
  , f.OSAT_ACCURACY_2_CNT , f.OSAT_ACCURACY_2_PCT 
  , f.OSAT_ACCURACY_1_CNT , f.OSAT_ACCURACY_1_PCT 
  , f.OSAT_FOOD_TASTE_RESPONSE_CNT 
  , f.OSAT_FOOD_TASTE_5_CNT , f.OSAT_FOOD_TASTE_5_PCT 
  , f.OSAT_FOOD_TASTE_4_CNT , f.OSAT_FOOD_TASTE_4_PCT
  , f.OSAT_FOOD_TASTE_3_CNT , f.OSAT_FOOD_TASTE_3_PCT 
  , f.OSAT_FOOD_TASTE_2_CNT , f.OSAT_FOOD_TASTE_2_PCT 
  , f.OSAT_FOOD_TASTE_1_CNT , f.OSAT_FOOD_TASTE_1_PCT 
  , f.OSAT_FRIENDLINESS_RESPONSE_CNT 
  , f.OSAT_FRIENDLINESS_5_CNT , f.OSAT_FRIENDLINESS_5_PCT 
  , f.OSAT_FRIENDLINESS_4_CNT , f.OSAT_FRIENDLINESS_4_PCT 
  , f.OSAT_FRIENDLINESS_3_CNT , f.OSAT_FRIENDLINESS_3_PCT 
  , f.OSAT_FRIENDLINESS_2_CNT , f.OSAT_FRIENDLINESS_2_PCT 
  , f.OSAT_FRIENDLINESS_1_CNT , f.OSAT_FRIENDLINESS_1_PCT 
  , f.OSAT_SERVICE_SPEED_RESPONSE_CNT 
  , f.OSAT_SERVICE_SPEED_5_CNT , f.OSAT_SERVICE_SPEED_5_PCT 
  , f.OSAT_SERVICE_SPEED_4_CNT , f.OSAT_SERVICE_SPEED_4_PCT 
  , f.OSAT_SERVICE_SPEED_3_CNT , f.OSAT_SERVICE_SPEED_3_PCT 
  , f.OSAT_SERVICE_SPEED_2_CNT , f.OSAT_SERVICE_SPEED_2_PCT 
  , f.OSAT_SERVICE_SPEED_1_CNT , f.OSAT_SERVICE_SPEED_1_PCT 
  , f.LIKE_TO_RETURN_RESPONSE_CNT 
  , f.LIKE_TO_RETURN_SCORE_5_CNT , f.LIKE_TO_RETURN_SCORE_5_PCT
  , f.LIKE_TO_RETURN_SCORE_4_CNT , f.LIKE_TO_RETURN_SCORE_4_PCT
  , f.LIKE_TO_RETURN_SCORE_3_CNT , f.LIKE_TO_RETURN_SCORE_3_PCT
  , f.LIKE_TO_RETURN_SCORE_2_CNT , f.LIKE_TO_RETURN_SCORE_2_PCT
  , f.LIKE_TO_RETURN_SCORE_1_CNT , f.LIKE_TO_RETURN_SCORE_1_PCT
  , f.DMA_CD , f.DMA_NM --columns moved per ODI-7559
  , f.OWNERSHIP_TYP --columns moved per ODI-7559
  , f.LOC_LEVEL1_NM , f.LOC_LEVEL1_MANAGER_NM 
  , f.LOC_LEVEL2_NM , f.LOC_LEVEL2_MANAGER_NM 
  , f.LOC_LEVEL3_NM , f.LOC_LEVEL3_MANAGER_NM 
  , f.LOC_LEVEL4_NM , f.LOC_LEVEL4_MANAGER_NM 
  , f.LOC_LEVEL5_NM , f.LOC_LEVEL5_MANAGER_NM 
  , f.STORE_CONFIG 
FROM 
TABLE( IDS_DEV.CUST.GUEST_EXPERIENCE_OSAT_INIT('ORDER_FULFILLMENT') ) AS f
LEFT OUTER JOIN
IDS_DEV.INT_REF.SURVEY_ORDER_FULFILLMENT AS sof ON f.SURVEY_ORDER_FULFILLMENT_ID = sof.SURVEY_ORDER_FULFILLMENT_ID
;
create view IF NOT EXISTS GUEST_EXPERIENCE_ORDER_PLACEMENT_OSAT_V(
	BRAND_ID,
	STORE_ID,
	BUSINESS_DT,
	SURVEY_ORDER_PLACEMENT_NM,
	DAY_OF_WEEK,
	FISC_WEEK_NBR,
	FISC_WEEK_START_DT,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DT,
	FISC_QUARTER_NBR,
	FISC_YEAR_NBR,
	OSAT_RESPONSE_CNT,
	OSAT_5_CNT,
	OSAT_5_PCT,
	OSAT_4_CNT,
	OSAT_4_PCT,
	OSAT_3_CNT,
	OSAT_3_PCT,
	OSAT_2_CNT,
	OSAT_2_PCT,
	OSAT_1_CNT,
	OSAT_1_PCT,
	OSAT_CLEANLINESS_RESPONSE_CNT,
	OSAT_CLEANLINESS_5_CNT,
	OSAT_CLEANLINESS_5_PCT,
	OSAT_CLEANLINESS_4_CNT,
	OSAT_CLEANLINESS_4_PCT,
	OSAT_CLEANLINESS_3_CNT,
	OSAT_CLEANLINESS_3_PCT,
	OSAT_CLEANLINESS_2_CNT,
	OSAT_CLEANLINESS_2_PCT,
	OSAT_CLEANLINESS_1_CNT,
	OSAT_CLEANLINESS_1_PCT,
	OSAT_ACCURACY_RESPONSE_CNT,
	OSAT_ACCURACY_5_CNT,
	OSAT_ACCURACY_5_PCT,
	OSAT_ACCURACY_4_CNT,
	OSAT_ACCURACY_4_PCT,
	OSAT_ACCURACY_3_CNT,
	OSAT_ACCURACY_3_PCT,
	OSAT_ACCURACY_2_CNT,
	OSAT_ACCURACY_2_PCT,
	OSAT_ACCURACY_1_CNT,
	OSAT_ACCURACY_1_PCT,
	OSAT_FOOD_TASTE_RESPONSE_CNT,
	OSAT_FOOD_TASTE_5_CNT,
	OSAT_FOOD_TASTE_5_PCT,
	OSAT_FOOD_TASTE_4_CNT,
	OSAT_FOOD_TASTE_4_PCT,
	OSAT_FOOD_TASTE_3_CNT,
	OSAT_FOOD_TASTE_3_PCT,
	OSAT_FOOD_TASTE_2_CNT,
	OSAT_FOOD_TASTE_2_PCT,
	OSAT_FOOD_TASTE_1_CNT,
	OSAT_FOOD_TASTE_1_PCT,
	OSAT_FRIENDLINESS_RESPONSE_CNT,
	OSAT_FRIENDLINESS_5_CNT,
	OSAT_FRIENDLINESS_5_PCT,
	OSAT_FRIENDLINESS_4_CNT,
	OSAT_FRIENDLINESS_4_PCT,
	OSAT_FRIENDLINESS_3_CNT,
	OSAT_FRIENDLINESS_3_PCT,
	OSAT_FRIENDLINESS_2_CNT,
	OSAT_FRIENDLINESS_2_PCT,
	OSAT_FRIENDLINESS_1_CNT,
	OSAT_FRIENDLINESS_1_PCT,
	OSAT_SERVICE_SPEED_RESPONSE_CNT,
	OSAT_SERVICE_SPEED_5_CNT,
	OSAT_SERVICE_SPEED_5_PCT,
	OSAT_SERVICE_SPEED_4_CNT,
	OSAT_SERVICE_SPEED_4_PCT,
	OSAT_SERVICE_SPEED_3_CNT,
	OSAT_SERVICE_SPEED_3_PCT,
	OSAT_SERVICE_SPEED_2_CNT,
	OSAT_SERVICE_SPEED_2_PCT,
	OSAT_SERVICE_SPEED_1_CNT,
	OSAT_SERVICE_SPEED_1_PCT,
	LIKE_TO_RETURN_RESPONSE_CNT,
	LIKE_TO_RETURN_SCORE_5_CNT,
	LIKE_TO_RETURN_SCORE_5_PCT,
	LIKE_TO_RETURN_SCORE_4_CNT,
	LIKE_TO_RETURN_SCORE_4_PCT,
	LIKE_TO_RETURN_SCORE_3_CNT,
	LIKE_TO_RETURN_SCORE_3_PCT,
	LIKE_TO_RETURN_SCORE_2_CNT,
	LIKE_TO_RETURN_SCORE_2_PCT,
	LIKE_TO_RETURN_SCORE_1_CNT,
	LIKE_TO_RETURN_SCORE_1_PCT,
	DMA_CD,
	DMA_NM,
	OWNERSHIP_TYP,
	LOC_LEVEL1_NM,
	LOC_LEVEL1_MANAGER_NM,
	LOC_LEVEL2_NM,
	LOC_LEVEL2_MANAGER_NM,
	LOC_LEVEL3_NM,
	LOC_LEVEL3_MANAGER_NM,
	LOC_LEVEL4_NM,
	LOC_LEVEL4_MANAGER_NM,
	LOC_LEVEL5_NM,
	LOC_LEVEL5_MANAGER_NM,
	STORE_CONFIG
) as
SELECT 
f.BRAND_ID , f.STORE_ID , f.BUSINESS_DT, sop.SURVEY_ORDER_PLACEMENT_NM 
  , f.DAY_OF_WEEK 
  , f.FISC_WEEK_NBR , f.FISC_WEEK_START_DT 
  , f.FISC_PERIOD_NBR , f.FISC_PERIOD_START_DT 
  , f.FISC_QUARTER_NBR , f.FISC_YEAR_NBR 
  , f.OSAT_RESPONSE_CNT 
  , f.OSAT_5_CNT , f.OSAT_5_PCT 
  , f.OSAT_4_CNT , f.OSAT_4_PCT 
  , f.OSAT_3_CNT , f.OSAT_3_PCT 
  , f.OSAT_2_CNT , f.OSAT_2_PCT 
  , f.OSAT_1_CNT , f.OSAT_1_PCT 
  , f.OSAT_CLEANLINESS_RESPONSE_CNT 
  , f.OSAT_CLEANLINESS_5_CNT , f.OSAT_CLEANLINESS_5_PCT
  , f.OSAT_CLEANLINESS_4_CNT , f.OSAT_CLEANLINESS_4_PCT 
  , f.OSAT_CLEANLINESS_3_CNT , f.OSAT_CLEANLINESS_3_PCT 
  , f.OSAT_CLEANLINESS_2_CNT , f.OSAT_CLEANLINESS_2_PCT 
  , f.OSAT_CLEANLINESS_1_CNT , f.OSAT_CLEANLINESS_1_PCT 
  , f.OSAT_ACCURACY_RESPONSE_CNT 
  , f.OSAT_ACCURACY_5_CNT , f.OSAT_ACCURACY_5_PCT 
  , f.OSAT_ACCURACY_4_CNT , f.OSAT_ACCURACY_4_PCT 
  , f.OSAT_ACCURACY_3_CNT , f.OSAT_ACCURACY_3_PCT 
  , f.OSAT_ACCURACY_2_CNT , f.OSAT_ACCURACY_2_PCT 
  , f.OSAT_ACCURACY_1_CNT , f.OSAT_ACCURACY_1_PCT 
  , f.OSAT_FOOD_TASTE_RESPONSE_CNT 
  , f.OSAT_FOOD_TASTE_5_CNT , f.OSAT_FOOD_TASTE_5_PCT 
  , f.OSAT_FOOD_TASTE_4_CNT , f.OSAT_FOOD_TASTE_4_PCT
  , f.OSAT_FOOD_TASTE_3_CNT , f.OSAT_FOOD_TASTE_3_PCT 
  , f.OSAT_FOOD_TASTE_2_CNT , f.OSAT_FOOD_TASTE_2_PCT 
  , f.OSAT_FOOD_TASTE_1_CNT , f.OSAT_FOOD_TASTE_1_PCT 
  , f.OSAT_FRIENDLINESS_RESPONSE_CNT 
  , f.OSAT_FRIENDLINESS_5_CNT , f.OSAT_FRIENDLINESS_5_PCT 
  , f.OSAT_FRIENDLINESS_4_CNT , f.OSAT_FRIENDLINESS_4_PCT 
  , f.OSAT_FRIENDLINESS_3_CNT , f.OSAT_FRIENDLINESS_3_PCT 
  , f.OSAT_FRIENDLINESS_2_CNT , f.OSAT_FRIENDLINESS_2_PCT 
  , f.OSAT_FRIENDLINESS_1_CNT , f.OSAT_FRIENDLINESS_1_PCT 
  , f.OSAT_SERVICE_SPEED_RESPONSE_CNT 
  , f.OSAT_SERVICE_SPEED_5_CNT , f.OSAT_SERVICE_SPEED_5_PCT 
  , f.OSAT_SERVICE_SPEED_4_CNT , f.OSAT_SERVICE_SPEED_4_PCT 
  , f.OSAT_SERVICE_SPEED_3_CNT , f.OSAT_SERVICE_SPEED_3_PCT 
  , f.OSAT_SERVICE_SPEED_2_CNT , f.OSAT_SERVICE_SPEED_2_PCT 
  , f.OSAT_SERVICE_SPEED_1_CNT , f.OSAT_SERVICE_SPEED_1_PCT 
  , f.LIKE_TO_RETURN_RESPONSE_CNT 
  , f.LIKE_TO_RETURN_SCORE_5_CNT , f.LIKE_TO_RETURN_SCORE_5_PCT
  , f.LIKE_TO_RETURN_SCORE_4_CNT , f.LIKE_TO_RETURN_SCORE_4_PCT
  , f.LIKE_TO_RETURN_SCORE_3_CNT , f.LIKE_TO_RETURN_SCORE_3_PCT
  , f.LIKE_TO_RETURN_SCORE_2_CNT , f.LIKE_TO_RETURN_SCORE_2_PCT
  , f.LIKE_TO_RETURN_SCORE_1_CNT , f.LIKE_TO_RETURN_SCORE_1_PCT
  , f.DMA_CD , f.DMA_NM --moved columns per ODI-7556
  , f.OWNERSHIP_TYP --moved columns per ODI-7556
  , f.LOC_LEVEL1_NM , f.LOC_LEVEL1_MANAGER_NM 
  , f.LOC_LEVEL2_NM , f.LOC_LEVEL2_MANAGER_NM 
  , f.LOC_LEVEL3_NM , f.LOC_LEVEL3_MANAGER_NM 
  , f.LOC_LEVEL4_NM , f.LOC_LEVEL4_MANAGER_NM 
  , f.LOC_LEVEL5_NM , f.LOC_LEVEL5_MANAGER_NM 
  , f.STORE_CONFIG 
FROM 
TABLE( IDS_DEV.CUST.GUEST_EXPERIENCE_OSAT_INIT('ORDER_PLACEMENT') ) AS f
LEFT OUTER JOIN
IDS_DEV.INT_REF.SURVEY_ORDER_PLACEMENT AS sop ON f.SURVEY_ORDER_PLACEMENT_ID = sop.SURVEY_ORDER_PLACEMENT_ID
;
create view IF NOT EXISTS GUEST_EXPERIENCE_OSAT_V(
	BRAND_ID,
	STORE_ID,
	BUSINESS_DT,
	DAY_OF_WEEK,
	FISC_WEEK_NBR,
	FISC_WEEK_START_DT,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DT,
	FISC_QUARTER_NBR,
	FISC_YEAR_NBR,
	OSAT_RESPONSE_CNT,
	OSAT_5_CNT,
	OSAT_5_PCT,
	OSAT_4_CNT,
	OSAT_4_PCT,
	OSAT_3_CNT,
	OSAT_3_PCT,
	OSAT_2_CNT,
	OSAT_2_PCT,
	OSAT_1_CNT,
	OSAT_1_PCT,
	OSAT_CLEANLINESS_RESPONSE_CNT,
	OSAT_CLEANLINESS_5_CNT,
	OSAT_CLEANLINESS_5_PCT,
	OSAT_CLEANLINESS_4_CNT,
	OSAT_CLEANLINESS_4_PCT,
	OSAT_CLEANLINESS_3_CNT,
	OSAT_CLEANLINESS_3_PCT,
	OSAT_CLEANLINESS_2_CNT,
	OSAT_CLEANLINESS_2_PCT,
	OSAT_CLEANLINESS_1_CNT,
	OSAT_CLEANLINESS_1_PCT,
	OSAT_ACCURACY_RESPONSE_CNT,
	OSAT_ACCURACY_5_CNT,
	OSAT_ACCURACY_5_PCT,
	OSAT_ACCURACY_4_CNT,
	OSAT_ACCURACY_4_PCT,
	OSAT_ACCURACY_3_CNT,
	OSAT_ACCURACY_3_PCT,
	OSAT_ACCURACY_2_CNT,
	OSAT_ACCURACY_2_PCT,
	OSAT_ACCURACY_1_CNT,
	OSAT_ACCURACY_1_PCT,
	OSAT_FOOD_TASTE_RESPONSE_CNT,
	OSAT_FOOD_TASTE_5_CNT,
	OSAT_FOOD_TASTE_5_PCT,
	OSAT_FOOD_TASTE_4_CNT,
	OSAT_FOOD_TASTE_4_PCT,
	OSAT_FOOD_TASTE_3_CNT,
	OSAT_FOOD_TASTE_3_PCT,
	OSAT_FOOD_TASTE_2_CNT,
	OSAT_FOOD_TASTE_2_PCT,
	OSAT_FOOD_TASTE_1_CNT,
	OSAT_FOOD_TASTE_1_PCT,
	OSAT_FRIENDLINESS_RESPONSE_CNT,
	OSAT_FRIENDLINESS_5_CNT,
	OSAT_FRIENDLINESS_5_PCT,
	OSAT_FRIENDLINESS_4_CNT,
	OSAT_FRIENDLINESS_4_PCT,
	OSAT_FRIENDLINESS_3_CNT,
	OSAT_FRIENDLINESS_3_PCT,
	OSAT_FRIENDLINESS_2_CNT,
	OSAT_FRIENDLINESS_2_PCT,
	OSAT_FRIENDLINESS_1_CNT,
	OSAT_FRIENDLINESS_1_PCT,
	OSAT_SERVICE_SPEED_RESPONSE_CNT,
	OSAT_SERVICE_SPEED_5_CNT,
	OSAT_SERVICE_SPEED_5_PCT,
	OSAT_SERVICE_SPEED_4_CNT,
	OSAT_SERVICE_SPEED_4_PCT,
	OSAT_SERVICE_SPEED_3_CNT,
	OSAT_SERVICE_SPEED_3_PCT,
	OSAT_SERVICE_SPEED_2_CNT,
	OSAT_SERVICE_SPEED_2_PCT,
	OSAT_SERVICE_SPEED_1_CNT,
	OSAT_SERVICE_SPEED_1_PCT,
	LIKE_TO_RETURN_RESPONSE_CNT,
	LIKE_TO_RETURN_SCORE_5_CNT,
	LIKE_TO_RETURN_SCORE_5_PCT,
	LIKE_TO_RETURN_SCORE_4_CNT,
	LIKE_TO_RETURN_SCORE_4_PCT,
	LIKE_TO_RETURN_SCORE_3_CNT,
	LIKE_TO_RETURN_SCORE_3_PCT,
	LIKE_TO_RETURN_SCORE_2_CNT,
	LIKE_TO_RETURN_SCORE_2_PCT,
	LIKE_TO_RETURN_SCORE_1_CNT,
	LIKE_TO_RETURN_SCORE_1_PCT,
	DMA_CD,
	DMA_NM,
	OWNERSHIP_TYP,
	LOC_LEVEL1_NM,
	LOC_LEVEL1_MANAGER_NM,
	LOC_LEVEL2_NM,
	LOC_LEVEL2_MANAGER_NM,
	LOC_LEVEL3_NM,
	LOC_LEVEL3_MANAGER_NM,
	LOC_LEVEL4_NM,
	LOC_LEVEL4_MANAGER_NM,
	LOC_LEVEL5_NM,
	LOC_LEVEL5_MANAGER_NM,
	STORE_CONFIG
) as
SELECT 
BRAND_ID , STORE_ID , BUSINESS_DT 
  , DAY_OF_WEEK 
  , FISC_WEEK_NBR , FISC_WEEK_START_DT 
  , FISC_PERIOD_NBR , FISC_PERIOD_START_DT 
  , FISC_QUARTER_NBR , FISC_YEAR_NBR 
  , OSAT_RESPONSE_CNT 
  , OSAT_5_CNT , OSAT_5_PCT 
  , OSAT_4_CNT , OSAT_4_PCT 
  , OSAT_3_CNT , OSAT_3_PCT 
  , OSAT_2_CNT , OSAT_2_PCT 
  , OSAT_1_CNT , OSAT_1_PCT 
  , OSAT_CLEANLINESS_RESPONSE_CNT 
  , OSAT_CLEANLINESS_5_CNT , OSAT_CLEANLINESS_5_PCT
  , OSAT_CLEANLINESS_4_CNT , OSAT_CLEANLINESS_4_PCT 
  , OSAT_CLEANLINESS_3_CNT , OSAT_CLEANLINESS_3_PCT 
  , OSAT_CLEANLINESS_2_CNT , OSAT_CLEANLINESS_2_PCT 
  , OSAT_CLEANLINESS_1_CNT , OSAT_CLEANLINESS_1_PCT 
  , OSAT_ACCURACY_RESPONSE_CNT 
  , OSAT_ACCURACY_5_CNT , OSAT_ACCURACY_5_PCT 
  , OSAT_ACCURACY_4_CNT , OSAT_ACCURACY_4_PCT 
  , OSAT_ACCURACY_3_CNT , OSAT_ACCURACY_3_PCT 
  , OSAT_ACCURACY_2_CNT , OSAT_ACCURACY_2_PCT 
  , OSAT_ACCURACY_1_CNT , OSAT_ACCURACY_1_PCT 
  , OSAT_FOOD_TASTE_RESPONSE_CNT 
  , OSAT_FOOD_TASTE_5_CNT , OSAT_FOOD_TASTE_5_PCT 
  , OSAT_FOOD_TASTE_4_CNT , OSAT_FOOD_TASTE_4_PCT
  , OSAT_FOOD_TASTE_3_CNT , OSAT_FOOD_TASTE_3_PCT 
  , OSAT_FOOD_TASTE_2_CNT , OSAT_FOOD_TASTE_2_PCT 
  , OSAT_FOOD_TASTE_1_CNT , OSAT_FOOD_TASTE_1_PCT 
  , OSAT_FRIENDLINESS_RESPONSE_CNT 
  , OSAT_FRIENDLINESS_5_CNT , OSAT_FRIENDLINESS_5_PCT 
  , OSAT_FRIENDLINESS_4_CNT , OSAT_FRIENDLINESS_4_PCT 
  , OSAT_FRIENDLINESS_3_CNT , OSAT_FRIENDLINESS_3_PCT 
  , OSAT_FRIENDLINESS_2_CNT , OSAT_FRIENDLINESS_2_PCT 
  , OSAT_FRIENDLINESS_1_CNT , OSAT_FRIENDLINESS_1_PCT 
  , OSAT_SERVICE_SPEED_RESPONSE_CNT 
  , OSAT_SERVICE_SPEED_5_CNT , OSAT_SERVICE_SPEED_5_PCT 
  , OSAT_SERVICE_SPEED_4_CNT , OSAT_SERVICE_SPEED_4_PCT 
  , OSAT_SERVICE_SPEED_3_CNT , OSAT_SERVICE_SPEED_3_PCT 
  , OSAT_SERVICE_SPEED_2_CNT , OSAT_SERVICE_SPEED_2_PCT 
  , OSAT_SERVICE_SPEED_1_CNT , OSAT_SERVICE_SPEED_1_PCT 
  , LIKE_TO_RETURN_RESPONSE_CNT 
  , LIKE_TO_RETURN_SCORE_5_CNT , LIKE_TO_RETURN_SCORE_5_PCT
  , LIKE_TO_RETURN_SCORE_4_CNT , LIKE_TO_RETURN_SCORE_4_PCT
  , LIKE_TO_RETURN_SCORE_3_CNT , LIKE_TO_RETURN_SCORE_3_PCT
  , LIKE_TO_RETURN_SCORE_2_CNT , LIKE_TO_RETURN_SCORE_2_PCT
  , LIKE_TO_RETURN_SCORE_1_CNT , LIKE_TO_RETURN_SCORE_1_PCT
  , DMA_CD , DMA_NM --moved cols per ODI-7577
  , OWNERSHIP_TYP --moved cols per ODI-7577
  , LOC_LEVEL1_NM , LOC_LEVEL1_MANAGER_NM 
  , LOC_LEVEL2_NM , LOC_LEVEL2_MANAGER_NM 
  , LOC_LEVEL3_NM , LOC_LEVEL3_MANAGER_NM 
  , LOC_LEVEL4_NM , LOC_LEVEL4_MANAGER_NM 
  , LOC_LEVEL5_NM , LOC_LEVEL5_MANAGER_NM 
  , STORE_CONFIG 
FROM TABLE( IDS_DEV.CUST.GUEST_EXPERIENCE_OSAT_INIT('BUSINESS_DT') )
;
create view IF NOT EXISTS GUEST_EXPERIENCE_SURVEY_CHANNEL_OSAT_V(
	BRAND_ID,
	STORE_ID,
	BUSINESS_DT,
	SURVEY_CHANNEL_NM,
	SURVEY_SUB_CHANNEL_NM,
	DAY_OF_WEEK,
	FISC_WEEK_NBR,
	FISC_WEEK_START_DT,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DT,
	FISC_QUARTER_NBR,
	FISC_YEAR_NBR,
	OSAT_RESPONSE_CNT,
	OSAT_5_CNT,
	OSAT_5_PCT,
	OSAT_4_CNT,
	OSAT_4_PCT,
	OSAT_3_CNT,
	OSAT_3_PCT,
	OSAT_2_CNT,
	OSAT_2_PCT,
	OSAT_1_CNT,
	OSAT_1_PCT,
	OSAT_CLEANLINESS_RESPONSE_CNT,
	OSAT_CLEANLINESS_5_CNT,
	OSAT_CLEANLINESS_5_PCT,
	OSAT_CLEANLINESS_4_CNT,
	OSAT_CLEANLINESS_4_PCT,
	OSAT_CLEANLINESS_3_CNT,
	OSAT_CLEANLINESS_3_PCT,
	OSAT_CLEANLINESS_2_CNT,
	OSAT_CLEANLINESS_2_PCT,
	OSAT_CLEANLINESS_1_CNT,
	OSAT_CLEANLINESS_1_PCT,
	OSAT_ACCURACY_RESPONSE_CNT,
	OSAT_ACCURACY_5_CNT,
	OSAT_ACCURACY_5_PCT,
	OSAT_ACCURACY_4_CNT,
	OSAT_ACCURACY_4_PCT,
	OSAT_ACCURACY_3_CNT,
	OSAT_ACCURACY_3_PCT,
	OSAT_ACCURACY_2_CNT,
	OSAT_ACCURACY_2_PCT,
	OSAT_ACCURACY_1_CNT,
	OSAT_ACCURACY_1_PCT,
	OSAT_FOOD_TASTE_RESPONSE_CNT,
	OSAT_FOOD_TASTE_5_CNT,
	OSAT_FOOD_TASTE_5_PCT,
	OSAT_FOOD_TASTE_4_CNT,
	OSAT_FOOD_TASTE_4_PCT,
	OSAT_FOOD_TASTE_3_CNT,
	OSAT_FOOD_TASTE_3_PCT,
	OSAT_FOOD_TASTE_2_CNT,
	OSAT_FOOD_TASTE_2_PCT,
	OSAT_FOOD_TASTE_1_CNT,
	OSAT_FOOD_TASTE_1_PCT,
	OSAT_FRIENDLINESS_RESPONSE_CNT,
	OSAT_FRIENDLINESS_5_CNT,
	OSAT_FRIENDLINESS_5_PCT,
	OSAT_FRIENDLINESS_4_CNT,
	OSAT_FRIENDLINESS_4_PCT,
	OSAT_FRIENDLINESS_3_CNT,
	OSAT_FRIENDLINESS_3_PCT,
	OSAT_FRIENDLINESS_2_CNT,
	OSAT_FRIENDLINESS_2_PCT,
	OSAT_FRIENDLINESS_1_CNT,
	OSAT_FRIENDLINESS_1_PCT,
	OSAT_SERVICE_SPEED_RESPONSE_CNT,
	OSAT_SERVICE_SPEED_5_CNT,
	OSAT_SERVICE_SPEED_5_PCT,
	OSAT_SERVICE_SPEED_4_CNT,
	OSAT_SERVICE_SPEED_4_PCT,
	OSAT_SERVICE_SPEED_3_CNT,
	OSAT_SERVICE_SPEED_3_PCT,
	OSAT_SERVICE_SPEED_2_CNT,
	OSAT_SERVICE_SPEED_2_PCT,
	OSAT_SERVICE_SPEED_1_CNT,
	OSAT_SERVICE_SPEED_1_PCT,
	LIKE_TO_RETURN_RESPONSE_CNT,
	LIKE_TO_RETURN_SCORE_5_CNT,
	LIKE_TO_RETURN_SCORE_5_PCT,
	LIKE_TO_RETURN_SCORE_4_CNT,
	LIKE_TO_RETURN_SCORE_4_PCT,
	LIKE_TO_RETURN_SCORE_3_CNT,
	LIKE_TO_RETURN_SCORE_3_PCT,
	LIKE_TO_RETURN_SCORE_2_CNT,
	LIKE_TO_RETURN_SCORE_2_PCT,
	LIKE_TO_RETURN_SCORE_1_CNT,
	LIKE_TO_RETURN_SCORE_1_PCT,
	DMA_CD,
	DMA_NM,
	OWNERSHIP_TYP,
	LOC_LEVEL1_NM,
	LOC_LEVEL1_MANAGER_NM,
	LOC_LEVEL2_NM,
	LOC_LEVEL2_MANAGER_NM,
	LOC_LEVEL3_NM,
	LOC_LEVEL3_MANAGER_NM,
	LOC_LEVEL4_NM,
	LOC_LEVEL4_MANAGER_NM,
	LOC_LEVEL5_NM,
	LOC_LEVEL5_MANAGER_NM,
	STORE_CONFIG
) as
SELECT 
f.BRAND_ID , f.STORE_ID , f.BUSINESS_DT, sc.SURVEY_CHANNEL_NM, sc.SURVEY_SUB_CHANNEL_NM
  , f.DAY_OF_WEEK 
  , f.FISC_WEEK_NBR , f.FISC_WEEK_START_DT 
  , f.FISC_PERIOD_NBR , f.FISC_PERIOD_START_DT 
  , f.FISC_QUARTER_NBR , f.FISC_YEAR_NBR 
  , f.OSAT_RESPONSE_CNT 
  , f.OSAT_5_CNT , f.OSAT_5_PCT 
  , f.OSAT_4_CNT , f.OSAT_4_PCT 
  , f.OSAT_3_CNT , f.OSAT_3_PCT 
  , f.OSAT_2_CNT , f.OSAT_2_PCT 
  , f.OSAT_1_CNT , f.OSAT_1_PCT 
  , f.OSAT_CLEANLINESS_RESPONSE_CNT 
  , f.OSAT_CLEANLINESS_5_CNT , f.OSAT_CLEANLINESS_5_PCT
  , f.OSAT_CLEANLINESS_4_CNT , f.OSAT_CLEANLINESS_4_PCT 
  , f.OSAT_CLEANLINESS_3_CNT , f.OSAT_CLEANLINESS_3_PCT 
  , f.OSAT_CLEANLINESS_2_CNT , f.OSAT_CLEANLINESS_2_PCT 
  , f.OSAT_CLEANLINESS_1_CNT , f.OSAT_CLEANLINESS_1_PCT 
  , f.OSAT_ACCURACY_RESPONSE_CNT 
  , f.OSAT_ACCURACY_5_CNT , f.OSAT_ACCURACY_5_PCT 
  , f.OSAT_ACCURACY_4_CNT , f.OSAT_ACCURACY_4_PCT 
  , f.OSAT_ACCURACY_3_CNT , f.OSAT_ACCURACY_3_PCT 
  , f.OSAT_ACCURACY_2_CNT , f.OSAT_ACCURACY_2_PCT 
  , f.OSAT_ACCURACY_1_CNT , f.OSAT_ACCURACY_1_PCT 
  , f.OSAT_FOOD_TASTE_RESPONSE_CNT 
  , f.OSAT_FOOD_TASTE_5_CNT , f.OSAT_FOOD_TASTE_5_PCT 
  , f.OSAT_FOOD_TASTE_4_CNT , f.OSAT_FOOD_TASTE_4_PCT
  , f.OSAT_FOOD_TASTE_3_CNT , f.OSAT_FOOD_TASTE_3_PCT 
  , f.OSAT_FOOD_TASTE_2_CNT , f.OSAT_FOOD_TASTE_2_PCT 
  , f.OSAT_FOOD_TASTE_1_CNT , f.OSAT_FOOD_TASTE_1_PCT 
  , f.OSAT_FRIENDLINESS_RESPONSE_CNT 
  , f.OSAT_FRIENDLINESS_5_CNT , f.OSAT_FRIENDLINESS_5_PCT 
  , f.OSAT_FRIENDLINESS_4_CNT , f.OSAT_FRIENDLINESS_4_PCT 
  , f.OSAT_FRIENDLINESS_3_CNT , f.OSAT_FRIENDLINESS_3_PCT 
  , f.OSAT_FRIENDLINESS_2_CNT , f.OSAT_FRIENDLINESS_2_PCT 
  , f.OSAT_FRIENDLINESS_1_CNT , f.OSAT_FRIENDLINESS_1_PCT 
  , f.OSAT_SERVICE_SPEED_RESPONSE_CNT 
  , f.OSAT_SERVICE_SPEED_5_CNT , f.OSAT_SERVICE_SPEED_5_PCT 
  , f.OSAT_SERVICE_SPEED_4_CNT , f.OSAT_SERVICE_SPEED_4_PCT 
  , f.OSAT_SERVICE_SPEED_3_CNT , f.OSAT_SERVICE_SPEED_3_PCT 
  , f.OSAT_SERVICE_SPEED_2_CNT , f.OSAT_SERVICE_SPEED_2_PCT 
  , f.OSAT_SERVICE_SPEED_1_CNT , f.OSAT_SERVICE_SPEED_1_PCT 
  , f.LIKE_TO_RETURN_RESPONSE_CNT 
  , f.LIKE_TO_RETURN_SCORE_5_CNT , f.LIKE_TO_RETURN_SCORE_5_PCT
  , f.LIKE_TO_RETURN_SCORE_4_CNT , f.LIKE_TO_RETURN_SCORE_4_PCT
  , f.LIKE_TO_RETURN_SCORE_3_CNT , f.LIKE_TO_RETURN_SCORE_3_PCT
  , f.LIKE_TO_RETURN_SCORE_2_CNT , f.LIKE_TO_RETURN_SCORE_2_PCT
  , f.LIKE_TO_RETURN_SCORE_1_CNT , f.LIKE_TO_RETURN_SCORE_1_PCT
  , f.DMA_CD , f.DMA_NM --moved cols per ODI-7557
  , f.OWNERSHIP_TYP --moved cols per ODI-7557
  , f.LOC_LEVEL1_NM , f.LOC_LEVEL1_MANAGER_NM 
  , f.LOC_LEVEL2_NM , f.LOC_LEVEL2_MANAGER_NM 
  , f.LOC_LEVEL3_NM , f.LOC_LEVEL3_MANAGER_NM 
  , f.LOC_LEVEL4_NM , f.LOC_LEVEL4_MANAGER_NM 
  , f.LOC_LEVEL5_NM , f.LOC_LEVEL5_MANAGER_NM 
  , f.STORE_CONFIG 
FROM 
TABLE( IDS_DEV.CUST.GUEST_EXPERIENCE_OSAT_INIT('SURVEY_CHANNEL') ) AS f
LEFT OUTER JOIN
IDS_DEV.INT_REF.SURVEY_CHANNEL AS sc ON f.SURVEY_CHANNEL_ID = sc.SURVEY_CHANNEL_ID
;
create view IF NOT EXISTS MEMBER_OFFER_V(
	MEMBER_OFFER_ID,
	OFFER_CD,
	MEMBER_ID,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	PRIVACY_IND,
	OPT_IN_DT,
	CREATE_DT,
	EXPIRATION_DT,
	LOADED_DT,
	MODIFIED_DT,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM
) as 

SELECT MEMBER_OFFER_DIM.PROFILEOFFERID,MEMBER_OFFER_DIM.OFFERCODE,MEMBER_OFFER_DIM.CUSTOMERID,MEMBER_OFFER_DIM.BRANDID,
MEMBER_OFFER_DIM.SOURCE,MEMBER_OFFER_DIM.PRIVACY,MEMBER_OFFER_DIM.OPTINDATE,MEMBER_OFFER_DIM.CREATEDATE,MEMBER_OFFER_DIM.EXPIRATIONDATE
,MEMBER_OFFER_DIM.LOADEDDATE,MEMBER_OFFER_DIM.MODIFIEDDATE,MEMBER_OFFER_DIM.CDMLOADDATE
,MEMBER_OFFER_DIM.LOADID,MEMBER_OFFER_DIM.LOADDTTM
FROM POLARIS_DEV.CUDM.MEMBER_OFFER_DIM;
create view IF NOT EXISTS OFFER_V(
	OFFER_CD,
	BRAND_ID,
	UNQ_OFFER_ID,
	VARIANT_ID,
	OFFER_SEQ_KEY,
	OFFER_NM,
	OFFER_REPORT_DESC,
	OFFER_SHORT_DESC,
	OFFER_DESC,
	OPTIN_REQUIRED_IND,
	LOCATIONS_NM,
	EXCLUSIONS_TXT,
	OFFER_IMAGE_NM,
	TERMS_AND_CONDITIONS_TXT,
	MOMENT_ELIGIBILITY_TXT,
	STANDARD_EXCLUSIONS_TXT,
	OTHER_EXCLUSIONS_TXT,
	POINTS_AWARDED_NBR,
	LIMITS_NBR,
	STORE_LOCATION_NBR,
	DAY_PART_NBR,
	MEMBER_LIST_NEEDED_IND,
	STORE_TYP,
	ONE_TIME_OFFER_IND,
	TRIVIA_NEEDED_IND,
	CHECKIN_IND,
	OFFER_PRIORITY_NBR,
	OFFER_TYP,
	AWARD_NM,
	PRODUCT_PLUS_DESC,
	TRIVIA_QUESTION_IND,
	TRIVIA_ANSWERS_IND,
	TARGET_OFFER_IND,
	PRIVACY_IND,
	VALIDITY_PERIOD_NBR,
	VALIDITY_UNIT_DATE_NM,
	DNA_INCENTIVE_TYP,
	DNA_CAMPAIGN_TYP,
	DNA_DISCOUNT_PRODUCT_NM,
	DNA_PRODUCT_CAT,
	DNA_ACTION_REQUIRED_TXT,
	DNA_MINIMUM_SPEND_NBR,
	DNA_OFFER_WEEK_IN_CAMPAIGN_NBR,
	OFFER_VISIBLE_DT,
	OFFER_START_DT,
	OFFER_END_DT,
	DNA_INCENTIVE_VAL,
	SYSTEM_OFFER_ID,
	DNA_DISCOUNT_PRODUCT_LEVEL_CD,
	DNA_OFFER_REDEMPTION_CHANNEL_TYP,
	STATUS_CD,
	OFFER_IMAGE2_NM,
	SOURCE_APPLICATION_TYP,
	SOURCE_SYSTEM_NM,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM
) as 
SELECT OFFER_DIM.OFFERCODE
,OFFER_DIM.BRANDID
,OFFER_DIM.UNQOFFERID
,OFFER_DIM.VARIANTID
,OFFER_DIM.OFFERSEQKEY
,OFFER_DIM.OFFERNAME
,OFFER_DIM.OFFERREPORTDESC
,OFFER_DIM.OFFERSHORTDESC
,OFFER_DIM.OFFERDESC
,OFFER_DIM.OPTINREQUIRED
,OFFER_DIM.LOCATIONS
,OFFER_DIM.EXCLUSIONS
,OFFER_DIM.OFFERIMAGENAME
,OFFER_DIM.TERMSANDCONDITIONS
,OFFER_DIM.MOMENTELIGIBILITY
,OFFER_DIM.STANDARDEXCLUSIONS
,OFFER_DIM.OTHEREXCLUSIONS
,OFFER_DIM.NUMBEROFPOINTSAWARDED
,OFFER_DIM.LIMITS
,OFFER_DIM.STORELOCATION
,OFFER_DIM.DAYPART
,OFFER_DIM.MEMBERLISTNEEDED
,OFFER_DIM.STORETYPE
,OFFER_DIM.ONETIMEOFFER
,OFFER_DIM.TRIVIANEEDED
,OFFER_DIM.CHECKINFLAG
,OFFER_DIM.OFFERPRIORITY
,OFFER_DIM.OFFERTYPE
,OFFER_DIM.AWARD
,OFFER_DIM.PRODUCTPLUS
,OFFER_DIM.TRIVIAQUESTION
,OFFER_DIM.TRIVIAANSWERS
,OFFER_DIM.TARGETOFFER
,OFFER_DIM.PRIVACY
,OFFER_DIM.VALIDITYPERIOD
,OFFER_DIM.VALIDITYUNIT
,OFFER_DIM.DNAINCENTIVETYPE
,OFFER_DIM.DNACAMPAIGNTYPE
,OFFER_DIM.DNADISCOUNTPRODUCT
,OFFER_DIM.DNAPRODUCTCATEGORY
,OFFER_DIM.DNAACTIONREQUIRED
,OFFER_DIM.DNAMINIMUMSPEND
,OFFER_DIM.DNAOFFERWEEKINCAMPAIGN
,OFFER_DIM.DATEOFFERVISIBLE
,OFFER_DIM.OFFERSTARTDATE
,OFFER_DIM.OFFERENDDATE
,OFFER_DIM.DNAINCENTIVEVALUE
,OFFER_DIM.SYSTEM_OFFER_ID
,OFFER_DIM.DNA_DISCOUNT_PRODUCT_LEVEL_CD
,OFFER_DIM.DNA_OFFER_REDEMPTION_CHANNEL_TYP
,OFFER_DIM.STATUS_CD
,OFFER_DIM.OFFER_IMAGE2_NM
,OFFER_DIM.SOURCE_APPLICATION_TYP
,OFFER_DIM.SOURCE
,OFFER_DIM.CDMLOADDATE
,OFFER_DIM.LOADID
,OFFER_DIM.LOADDTTM

FROM POLARIS_DEV.CUDM.OFFER_DIM;
create view IF NOT EXISTS TEST(
	UNIQORDERLINEID,
	ORDERID,
	ORDERLINEID,
	CHANNELID,
	MDMITEMID,
	MDMPARENTITEMID,
	BUSINESSDATE,
	TIMEKEY,
	STOREID,
	EMPLOYEEID,
	REVENUECENTER,
	REGISTERID,
	TAXID,
	SEATNUMBER,
	GROSSQUANTITY,
	PRICE,
	DISCOUNTPRICE,
	GROSSAMOUNT,
	NETAMOUNT,
	TAXAMOUNT,
	INCLUSIVETAX,
	ISCLEARED,
	ISDELETED,
	ISVOIDED,
	ISINVENTORY,
	ISDISCOUNTED,
	MODIFIERID,
	TAXEXEMPTID,
	MANAGERID,
	BRANDID,
	SOURCE,
	CDMLOADDATE,
	LOADTYPE,
	LOADID,
	LOADDTTM
) as
  select * from Polaris_dev.sadm.orderline_fact;