create VIEW IF NOT EXISTS CRM_PUSH_MESSAGE_DETAIL_BV (
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
	CONTACT_KEY COMMENT 'Contact Key is ID of the contact that owns the device that received the message but not the ID of the device; one contact could have multiple devices. The ContactKey matches theÂ Contacts and DevicesÂ ID. The ContactKey indicates the subscriber associated with the device when the report is run',
	REQUEST_ID COMMENT 'Request Id is Marketing Clouds unique identifier for every request',
	SERVICE_TRANSACTION_ID COMMENT 'Service Transaction Identifier per each activity event for a message. The initial population of this field is for data being integrated from Epsilon Agility Harmony. There can be multiple service transactions per message.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	LOAD_FILENAME COMMENT 'Load Filename is the name of the file that was used to create the data.'
) COMMENT='CRM Push Message Detail_BV contains information about push notification interactions.'
 as 
SELECT CRM_PUSH_MESSAGE_DETAIL.BRAND_ID,CRM_PUSH_MESSAGE_DETAIL.SOURCE_SYSTEM_NM,CRM_PUSH_MESSAGE_DETAIL.ACCOUNT_ID,CRM_PUSH_MESSAGE_DETAIL.EID,CRM_PUSH_MESSAGE_DETAIL.APP_NAME,CRM_PUSH_MESSAGE_DETAIL.MESSAGE_NAME,CRM_PUSH_MESSAGE_DETAIL.MESSAGE_ID,CRM_PUSH_MESSAGE_DETAIL.CAMPAIGNS,CRM_PUSH_MESSAGE_DETAIL.DEVICE_ID,CRM_PUSH_MESSAGE_DETAIL.DATE_TIME_SEND,CRM_PUSH_MESSAGE_DETAIL.MESSAGE_CONTENT,CRM_PUSH_MESSAGE_DETAIL.MESSAGE_OPENED,CRM_PUSH_MESSAGE_DETAIL.OPEN_DATE,CRM_PUSH_MESSAGE_DETAIL.TIME_IN_APP,CRM_PUSH_MESSAGE_DETAIL.PLATFORM,CRM_PUSH_MESSAGE_DETAIL.PLATFORM_VERSION,CRM_PUSH_MESSAGE_DETAIL.STATUS,CRM_PUSH_MESSAGE_DETAIL.SERVICE_RESPONSE,CRM_PUSH_MESSAGE_DETAIL.GEOFENCE_NAME,CRM_PUSH_MESSAGE_DETAIL.TEMPLATE,CRM_PUSH_MESSAGE_DETAIL.FORMAT,CRM_PUSH_MESSAGE_DETAIL.PAGE_NAME,CRM_PUSH_MESSAGE_DETAIL.PUSH_JOB_ID,CRM_PUSH_MESSAGE_DETAIL.SYSTEM_TOKEN,CRM_PUSH_MESSAGE_DETAIL.INBOX_DOWNLOAD,CRM_PUSH_MESSAGE_DETAIL.INBOX_OPEN,CRM_PUSH_MESSAGE_DETAIL.IOS_MEDIA_URL,CRM_PUSH_MESSAGE_DETAIL.ANDROID_MEDIA_URL,CRM_PUSH_MESSAGE_DETAIL.MEDIA_ALT,CRM_PUSH_MESSAGE_DETAIL.CONTACT_KEY,CRM_PUSH_MESSAGE_DETAIL.REQUEST_ID,CRM_PUSH_MESSAGE_DETAIL.SERVICE_TRANSACTION_ID,CRM_PUSH_MESSAGE_DETAIL.LOAD_ID,CRM_PUSH_MESSAGE_DETAIL.LOAD_DTTM,CRM_PUSH_MESSAGE_DETAIL.UPDATE_ID,CRM_PUSH_MESSAGE_DETAIL.UPDATE_DTTM,CRM_PUSH_MESSAGE_DETAIL.LOAD_FILENAME
FROM IDS_%%envname%%.CUST.CRM_PUSH_MESSAGE_DETAIL ;

create VIEW IF NOT EXISTS CRM_PUSH_SEND_LOG_BV(
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
FROM IDS_%%envname%%.CUST.CRM_PUSH_SEND_LOG ;