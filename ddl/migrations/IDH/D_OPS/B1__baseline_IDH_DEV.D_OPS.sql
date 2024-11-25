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
create view IF NOT EXISTS CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	CMX_ID COMMENT 'CMX Identifier appears to uniquely identify the attribute combinations of group_name, report_tag, core_audit_service_service_name, and parent_item_id from the raw table. Sample values a7d51eab-f91e-4c98-91b3-510abf4b8bb0 and 7dbbab8a-484a-45a1-9fab-e8e4d070a202.',
	CMX_AUDIT_ID COMMENT 'CMX Audit Identifier  Sample values str-65f32bfe45b148cd9b541450fa2349a7 and str-ccf32239a76247a48231c223a00c9382.',
	REST_ID COMMENT 'Store Identifier is an integer that uniquely identifies the store.',
	AUDIT_DATE COMMENT 'Audit Date is the day of the audit.',
	CMX_AUDIT_SERVICE_NAME COMMENT 'CMX Audit Service Name is a label for a type of audit. Sample values CTR PRE-Certification Audit, EcoSure (Self Assessment) 2021, and NCTR Certification Audit.',
	CMX_AUDIT_SERVICE_VERSION_NBR COMMENT 'CMX Audit Service Version Number is the version of an audit.',
	CMX_GROUP_NAME COMMENT 'CMX Group Name is a high level categorization of questions. Sample values ARBY''S STANDARDS, FOOD SAFETY, and GUEST EXPERIENCE.',
	CMX_GROUP_POINT_QTY COMMENT 'CMX Group Point Quantity is the number of points for an audit that a restaraunt recieved at the Group Level.',
	CMX_GROUP_MAXIMUM_POINT_QTY COMMENT 'CMX Group Maximum Point Quantity is the maximum number of points for an audit that a restaraunt may receive at the Group Level.',
	CMX_GROUP_VAL COMMENT 'CMX Group Value  is a calculated value fromGroup Point Quantity and Group Maximum Point Quantity and store the percentage calculation. Examples of values are 93.32 and 87.65.',
	CMX_REPORT_TAG_NAME COMMENT 'CMX Report Tag is a further categorization of audit question within a Group. NameSample values Building/Equipment/OSM Standards, Menu/Marketing, Equipment, Team Practices, Accuracy, and Cleanliness- Guest View.',
	CMX_REPORT_TAG_POINT_QTY COMMENT 'CMX Report Tag Point Quantity is the number of points for an audit that a restaraunt recieved at the Group/Report Tag Level.',
	CMX_REPORT_TAG_MAXIMUM_POINT_QTY COMMENT 'CMX Report Tag Maximum Point Quantity is the maximum number of points for an audit that a restaraunt may receive at the Group/Report Tag  Level.',
	CMX_REPORT_TAG_VAL COMMENT 'CMX Report Tag Value is a calculated value from Report Tag Point Quantity and Report Tag Maximum Point Quantity and store the percentage calculation. Examples of values are 93.32 and 87.65.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='CMX Restaurant Audit Report Tag Agg  is CMX audit aggregated data at the Group/Report Tag level.'
 as 
SELECT CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.BRAND_ID,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_ID,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_AUDIT_ID,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.STORE_ID,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.AUDIT_DT,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_AUDIT_SERVICE_NM,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_AUDIT_SERVICE_VERSION_NBR,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_GROUP_NM,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_GROUP_POINT_QTY,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_GROUP_MAXIMUM_POINT_QTY,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_GROUP_VAL,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_REPORT_TAG_NM,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_REPORT_TAG_POINT_QTY,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_REPORT_TAG_MAXIMUM_POINT_QTY,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.CMX_REPORT_TAG_VAL,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.SOURCE_SYSTEM_NM,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.LOAD_ID,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.LOAD_DTTM,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.UPDATE_ID,CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV.UPDATE_DTTM
FROM IDS_DEV.LOCN_BV.CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG_BV ;
create view IF NOT EXISTS ECOSURE_RESTAURANT_AUDIT(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	ECOSURE_AUDIT_ID COMMENT 'Ecosure Audit Identifier is a unique integer value to identify an audit.',
	AUDIT_DATE COMMENT 'Audit Date is the day of the audit.',
	ECOSURE_CONCEPT_NBR COMMENT 'ECOSURE CONCEPT NUMBER is a location attribute. Sample value ARB.',
	REST_ID COMMENT 'Store Identifier is an integer that uniquely identifies the store.',
	ECOSURE_AUDITOR_ID COMMENT 'ECOSURE AUDITOR IDENTIFIER uniquely identifies an indvidual auditor.',
	ECOSURE_CYCLE_QTY COMMENT 'ECOSURE CYCLE QUANTITY is the number of rounds of an audit.',
	ECOSURE_VISIT_QTY COMMENT 'ECOSURE VISIT QUANTITY is the count of visits within a cycle.',
	ECOSURE_REFERENCE_NBR COMMENT 'ECOSURE REFERENCE NUMBER is the Internal EcoSure program number. Sample value AY2018.',
	ECOSURE_NOTE_TXT COMMENT 'ECOSURE NOTE TEXT are audit level notes.',
	ECOSURE_REST_ID COMMENT 'ECOSURE STORE IDENTIFIER is the unique EcoSure location id.',
	ECOSURE_FORM_NAME COMMENT 'ECOSURE FORM NAME is the program audit form name. Sample value Arby''s Safety First (S).',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='ECOSURE_RESTAURANT_AUDIT identifies the audit level information for a particular store that is sourced from ECOSURE.'
 as 
SELECT ECOSURE_RESTAURANT_AUDIT_BV.BRAND_ID,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_AUDIT_ID,ECOSURE_RESTAURANT_AUDIT_BV.AUDIT_DT,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_CONCEPT_NBR,ECOSURE_RESTAURANT_AUDIT_BV.STORE_ID,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_AUDITOR_ID,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_CYCLE_QTY,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_VISIT_QTY,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_REFERENCE_NBR,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_NOTE_TXT,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_STORE_ID,ECOSURE_RESTAURANT_AUDIT_BV.ECOSURE_FORM_NM,ECOSURE_RESTAURANT_AUDIT_BV.SOURCE_SYSTEM_NM,ECOSURE_RESTAURANT_AUDIT_BV.LOAD_ID,ECOSURE_RESTAURANT_AUDIT_BV.LOAD_DTTM,ECOSURE_RESTAURANT_AUDIT_BV.UPDATE_ID,ECOSURE_RESTAURANT_AUDIT_BV.UPDATE_DTTM
FROM IDS_DEV.LOCN_BV.ECOSURE_RESTAURANT_AUDIT_BV ;
create view IF NOT EXISTS ECOSURE_RESTAURANT_AUDIT_SCORE(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	ECOSURE_AUDIT_ID COMMENT 'Ecosure Audit Identifier is a unique integer value to identify an audit.',
	ECOSURE_SCORE_NAME COMMENT 'ECOSURE SCORE NAME is the label for the score. Sample value: Overall Score.',
	ECOSURE_SCORE_AMT COMMENT 'ECOSURE SCORE AMOUNT is a decimal value from 0 to 100. Sample Values 93.4, 82, and 62.8.',
	ECOSURE_RATING_VAL COMMENT 'ECOSURE RATING VALUE a single letter value denoting the score. Sample Values A, B, C, and F.',
	ECOSURE_IN_COMPLIANCE_QTY COMMENT 'ECOSURE IN COMPLIANCE QUANTITY is the count of items in compliance.',
	ECOSURE_OUT_OF_COMPLIANCE_QTY COMMENT 'ECOSURE OUT OF COMPLIANCE QUANTITY  is the count of items in out of compliance.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='ECOSURE_RESTAURANT_AUDIT SCORE contains the scores at an audit aggregate level for a store that is sourced from ECOSURE.'
 as 
SELECT ECOSURE_RESTAURANT_AUDIT_SCORE_BV.BRAND_ID,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.ECOSURE_AUDIT_ID,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.ECOSURE_SCORE_NM,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.ECOSURE_SCORE_AMT,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.ECOSURE_RATING_VAL,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.ECOSURE_IN_COMPLIANCE_QTY,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.ECOSURE_OUT_OF_COMPLIANCE_QTY,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.SOURCE_SYSTEM_NM,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.LOAD_ID,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.LOAD_DTTM,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.UPDATE_ID,ECOSURE_RESTAURANT_AUDIT_SCORE_BV.UPDATE_DTTM
FROM IDS_DEV.LOCN_BV.ECOSURE_RESTAURANT_AUDIT_SCORE_BV ;
create view IF NOT EXISTS REST_HRS(
	REST_ID,
	DAY_OF_WK,
	OPEN_TIME,
	CLOSE_TIME,
	BRAND_ID,
	CDM_LOAD_DATE,
	SOURCE_SYSTEM_NAME,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as 
SELECT c.STORE_ID AS REST_ID
, c.DAY_OF_WEEK_NM AS DAY_OF_WK
, c.OPEN_TM_TXT AS OPEN_TIME
, c.CLOSE_TM_TXT AS CLOSE_TIME
, c.BRAND_ID AS BRAND_ID
, c.CDM_LOAD_DT AS CDM_LOAD_DATE
, c.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NAME
, c.LOAD_ID AS LOAD_ID
, c.LOAD_DTTM AS LOAD_DTTM
, c.UPDATE_ID AS UPDATE_ID
, c.UPDATE_DTTM AS UPDATE_DTTM
FROM IDM_DEV.COREDIM_BV.STORE_HRS_BV c;
create view IF NOT EXISTS SCHEDULED_AND_ACTUAL_HRS(
	EMPLOYEE_PUNCHES_KEY,
	REST_ID,
	DATE,
	CLOCK_IN_DATE,
	CLOCK_IN_TIME,
	CLOCK_OUT_DATE,
	CLOCK_OUT_TIME,
	PUNCH_TYPE,
	ACTIVITY_TYPE,
	HOURS,
	JOB_CODE,
	BRAND_ID,
	LAST_NAME,
	FIRST_NAME,
	LOAD_TYPE,
	SOURCE_SYSTEM_NAME,
	CDM_LOAD_DATE,
	LOAD_ID,
	LOAD_DTTM
) as 
SELECT
c.EMPLOYEE_PUNCHES_KEY AS EMPLOYEE_PUNCHES_KEY
,c.REST_ID AS REST_ID
,c.DATE AS DATE
,c.CLOCK_IN_DATE AS CLOCK_IN_DATE
,c.CLOCK_IN_TIME AS CLOCK_IN_TIME
,c.CLOCK_OUT_DATE AS CLOCK_OUT_DATE
,c.CLOCK_OUT_TIME AS CLOCK_OUT_TIME
,c.PUNCH_TYPE AS PUNCH_TYPE
,c.ACTIVITY_TYPE AS ACTIVITY_TYPE
,c.HOURS AS HOURS
,c.JOB_CODE AS JOB_CODE
,c.BRAND_ID AS BRAND_ID
,c.LAST_NAME AS LAST_NAME
,c.FIRST_NAME AS FIRST_NAME
,c.LOAD_TYPE AS LOAD_TYPE
,c.SOURCE_SYSTEM_NAME AS SOURCE_SYSTEM_NAME
,c.CDM_LOAD_DATE AS CDM_LOAD_DATE
,c.LOAD_ID AS LOAD_ID
,c.LOAD_DTTM AS LOAD_DTTM
FROM IDS_DEV.LOCN_BV.SCHEDULED_AND_ACTUAL_HRS_BV C;
create view IF NOT EXISTS SPEED_OF_SERVICE(
	REST_ID,
	BUSINESS_DATE,
	CAR_ID,
	DAYPART,
	ARRIVAL_DTTM,
	DEPARTURE_DTTM,
	GOAL_NAME,
	REPORTING_NAME,
	GOAL_SECONDS_NBR,
	SERVICE_POINT_NBR,
	DISCARD_IND,
	DISCARD_CODE,
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	CDM_LOAD_DATE,
	LOAD_TYPE,
	LOAD_ID,
	LOAD_DTTM
) as 
SELECT
c.REST_ID AS REST_ID
,c.BUSINESS_DATE AS BUSINESS_DATE
,c.CAR_ID AS CAR_ID
,c.DAYPART AS DAYPART
,c.ARRIVAL_DTTM AS ARRIVAL_DTTM
,c.DEPARTURE_DTTM AS DEPARTURE_DTTM
,c.GOAL_NAME AS GOAL_NAME
,c.REPORTING_NAME AS REPORTING_NAME
,c.GOAL_SECONDS_NBR AS GOAL_SECONDS_NBR
,c.SERVICE_POINT_NBR AS SERVICE_POINT_NBR
,c.DISCARD_IND AS DISCARD_IND
,c.DISCARD_CODE AS DISCARD_CODE
,c.BRAND_ID AS BRAND_ID
,c.SOURCE_SYSTEM_NAME AS SOURCE_SYSTEM_NAME
,c.CDM_LOAD_DATE AS CDM_LOAD_DATE
,c.LOAD_TYPE AS LOAD_TYPE
,c.LOAD_ID AS LOAD_ID
,c.LOAD_DTTM AS LOAD_DTTM
FROM IDS_DEV.LOCN_BV.SPEED_OF_SERVICE_BV C;
create view IF NOT EXISTS TRANS_LINE_ITEM_PREP(
	REST_ID,
	BUMP_DTTM,
	SEND_DTTM,
	FIRE_DTTM,
	ORDER_LINE_PREP_LOC_ID,
	ORDER_LINE_ID,
	ORDER_ID,
	CDM_LOAD_DATE,
	SOURCE_SYSTEM_NAME,
	BRAND_ID,
	LOAD_ID,
	LOAD_DTTM
) as
SELECT
c.REST_ID AS REST_ID
,c.BUMP_DTTM AS BUMP_DTTM
,c.SEND_DTTM AS SEND_DTTM
,c.FIRE_DTTM AS FIRE_DTTM
,c.ORDER_LINE_PREP_LOC_ID AS ORDER_LINE_PREP_LOC_ID
,c.ORDER_LINE_ID AS ORDER_LINE_ID
,c.ORDER_ID AS ORDER_ID
,c.CDM_LOAD_DATE AS CDM_LOAD_DATE
,c.SOURCE_SYSTEM_NAME AS SOURCE_SYSTEM_NAME
,c.BRAND_ID AS BRAND_ID
,c.LOAD_ID AS LOAD_ID
,c.LOAD_DTTM AS LOAD_DTTM
FROM IDS_DEV.TXN_BV.TRANS_LINE_ITEM_PREP_BV C;
create view IF NOT EXISTS VISIT_SURVEY(
	SURVEY_ID,
	REST_ID,
	SURVEY_DATE,
	QUESTION_ID,
	QUESTION_LABEL_TXT,
	QUESTION_TXT,
	RESPONSE_TXT,
	POINTS_POSSIBLE_NBR,
	BRAND_ID,
	CDM_LOAD_DATE,
	LOAD_ID,
	LOAD_DTTM
) as 
SELECT
c.SURVEY_ID AS SURVEY_ID
,c.REST_ID AS REST_ID
,c.SURVEY_DATE AS SURVEY_DATE
,c.QUESTION_ID AS QUESTION_ID
,c.QUESTION_LABEL_TXT AS QUESTION_LABEL_TXT
,c.QUESTION_TXT AS QUESTION_TXT
,c.RESPONSE_TXT AS RESPONSE_TXT
,c.POINTS_POSSIBLE_NBR AS POINTS_POSSIBLE_NBR
,c.BRAND_ID AS BRAND_ID
,c.CDM_LOAD_DATE AS CDM_LOAD_DATE
,c.LOAD_ID AS LOAD_ID
,c.LOAD_DTTM AS LOAD_DTTM
FROM IDS_DEV.LOCN_BV.VISIT_SURVEY_BV C;