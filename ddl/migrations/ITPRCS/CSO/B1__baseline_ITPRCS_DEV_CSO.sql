create or replace materialized view FLATTEN(
	FULL_TABLE_NAME,
	QUERIES,
	CHILD
) as
                select distinct 
                    lns.FULL_TABLE_NAME, 
                    lns.QUERIES,
                    lns_child.VALUE::string as CHILD
                from ITPRCS_DEV.CSO.LINEAGE_NODES lns,
                lateral flatten (input => lns.LINEAGE) as lns_child;
create TABLE IF NOT EXISTS IDS_TABLES_MONITORING (
	TABLE_NAME VARCHAR(16777216),
	TABLE_TYPE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS IDS_TABLES_MONITORING_BUSINESSDATE (
	BUSINESS_DT VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_COUNT NUMBER(38,0),
	TABLE_NAME VARCHAR(16777216),
	TABLE_TYPE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS IDS_TABLES_MONITORING_LOADID (
	LOAD_ID VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_COUNT NUMBER(38,0),
	TABLE_NAME VARCHAR(16777216),
	TABLE_TYPE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS IDS_TABLES_MONITORING_SOURCE (
	TABLE_NAME VARCHAR(16777216),
	TABLE_TYPE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS LINEAGE_NODES (
	TABLE_CATALOG VARCHAR(16777216),
	TABLE_SCHEMA VARCHAR(16777216),
	TABLE_NAME VARCHAR(16777216),
	FULL_TABLE_NAME VARCHAR(16777216),
	COLUMNS OBJECT,
	LINEAGE ARRAY NOT NULL,
	QUERIES ARRAY NOT NULL
);
create or replace TRANSIENT TABLE LINEAGE_NODES_TMP (
	TABLE_CATALOG VARCHAR(16777216),
	TABLE_SCHEMA VARCHAR(16777216),
	TABLE_NAME VARCHAR(16777216),
	FULL_TABLE_NAME VARCHAR(16777216),
	COLUMNS OBJECT,
	LINEAGE ARRAY NOT NULL,
	QUERIES ARRAY NOT NULL
);
create TABLE IF NOT EXISTS PIPE_MONITORING_LOG (
	PIPE_DATABASE VARCHAR(16777216),
	PIPE_SCHEMA VARCHAR(16777216),
	PIPE_NAME VARCHAR(16777216),
	TABLE_DATABASE VARCHAR(16777216),
	TABLE_SCHEMA VARCHAR(16777216),
	TABLE_NAME VARCHAR(16777216),
	PIPE_OFFSET TIMESTAMP_LTZ(9)
);
create TABLE IF NOT EXISTS STAGE_MONITORING_LOG (
	DATABASE VARCHAR(16777216),
	SCHEMA VARCHAR(16777216),
	STAGE_NAME VARCHAR(16777216),
	FILE_COUNT NUMBER(38,0),
	STAGE_SIZE NUMBER(38,0),
	LOG_TIMESTAMP TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS STORED_PROCEDURE_LOG (
	SPNAME VARCHAR(16777216),
	LEVEL VARCHAR(16777216),
	TIMESTAMP TIMESTAMP_NTZ(9),
	TEXT VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TASK_MONITORING_LOG (
	DATABASE VARCHAR(16777216),
	SCHEMA VARCHAR(16777216),
	TASK_NAME VARCHAR(16777216),
	TASK_OFFSET TIMESTAMP_NTZ(9)
);
create or replace view TEMP_COUNT_LOADID_CHANNEL_DIM(
	SOURCE_SYSTEM_NM,
	BRAND_ID,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "SOURCE" AS "SOURCE_SYSTEM_NM", "BRANDID" AS "BRAND_ID", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.SADM.CHANNEL_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CHANNEL_HIERARCHY(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.DATASTORE.CHANNEL_HIERARCHY))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.LOCN.CMX_RESTAURANT_AUDIT_REPORT_TAG_AGG))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CRM_BOUNCE(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.CRM_BOUNCE))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CRM_CLICK(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.CRM_CLICK))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CRM_OPEN(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.CRM_OPEN))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CRM_SEND_JOB(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.CRM_SEND_JOB))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CRM_SEND_LOG(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.CRM_SEND_LOG))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CRM_SENT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.CRM_SENT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CRM_UNSUBSCRIBE(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.CRM_UNSUBSCRIBE))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CUSTOMER(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.CUSTOMER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CUSTOMER_CERTIFICATE_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.CUDM.CUSTOMER_CERTIFICATE_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CUSTOMER_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.CUDM.CUSTOMER_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_CUST_ENRICHMENT_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.CUST_ENRICHMENT_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DAILYFLASHSALES_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.SADM.DAILYFLASHSALES_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DAILY_DAY_PART_FLASH_SALES_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.DATASTORE.DAILY_DAY_PART_FLASH_SALES_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DAILY_FLASH_SALES_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.DATASTORE.DAILY_FLASH_SALES_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DATE_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.INT_REF.DATE_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_CHANNEL_MAPPING(
	SOURCE_SYSTEM_NM,
	BRAND_ID,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "SOURCE_SYSTEM_NM", "SOURCE_BRAND_ID" AS "BRAND_ID", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_CHANNEL_MAPPING))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_DISCOUNT(
	BRAND_ID,
	LOAD_ID,
	SOURCE_SYSTEM_NM
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "LOAD_ID", "SOURCE_SYSTEM_NM" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_DISCOUNT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM(
	BRAND_ID,
	LOAD_ID,
	SOURCE_SYSTEM_NM
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "LOAD_ID", "SOURCE_SYSTEM_NM" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_LINE(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_LINE))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_LINE_CHILD(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_LINE_CHILD))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_LINE_CHILD_MODIFIER(
	LOAD_ID,
	BRAND_ID,
	SOURCE_SYSTEM_NM
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "LOAD_ID", "BRAND_ID", "SOURCE_SYSTEM_NM" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_LINE_CHILD_MODIFIER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP(
	LOAD_ID,
	BRAND_ID,
	SOURCE_SYSTEM_NM
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "LOAD_ID", "BRAND_ID", "SOURCE_SYSTEM_NM" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_LINE_MODIFIER(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_LINE_MODIFIER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_LINE_MODIFIER_GROUP(
	BRAND_ID,
	LOAD_ID,
	SOURCE_SYSTEM_NM
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "LOAD_ID", "SOURCE_SYSTEM_NM" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_LINE_MODIFIER_GROUP))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_ORDER_PAYMENT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_ORDER_PAYMENT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_SUGGESTED_SELL(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_SUGGESTED_SELL))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_SUGGESTED_SELL_CART(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_SUGGESTED_SELL_CART))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_DIGITAL_SUGGESTED_SELL_RECOMMENDATION(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.TXN.DIGITAL_SUGGESTED_SELL_RECOMMENDATION))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_ECOSURE_RESTAURANT_AUDIT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.LOCN.ECOSURE_RESTAURANT_AUDIT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_ECOSURE_RESTAURANT_AUDIT_SCORE(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.LOCN.ECOSURE_RESTAURANT_AUDIT_SCORE))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_EMPLOYEEPAYDAY_FACT(
	BRAND_ID,
	LOAD_ID,
	SOURCE_SYSTEM_NM
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "CDMLOADDATE" AS "LOAD_ID", "SOURCE" AS "SOURCE_SYSTEM_NM" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.EMPLOYEEPAYDAY_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_GUEST_EXPERIENCE_SURVEY_RESPONSE_ANSWER(
	SOURCE_SYSTEM_NM,
	BRAND_ID,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "SOURCE_SYSTEM_NM", "BRAND_ID", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.GUEST_EXPERIENCE_SURVEY_RESPONSE_ANSWER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER(
	SOURCE_SYSTEM_NM,
	BRAND_ID,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "SOURCE_SYSTEM_NM", "BRAND_ID", "LOAD_ID" FROM ( SELECT  *  FROM (IDS_DEV.CUST.GUEST_EXPERIENCE_SURVEY_RESPONSE_HEADER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LABOR_QTRHR_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.LABOR_QTRHR_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LOCATION_HIERARCHY_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.LOCATION_HIERARCHY_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LOCATION_OWNERSHIP_STATUS_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.LOCATION_OWNERSHIP_STATUS_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LOYALTY_ACTVY_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.LOYALTY_ACTVY_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LOYALTY_CERTIFICATE_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.LOYALTY_CERTIFICATE_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LOYALTY_DISCOUNT_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.LOYALTY_DISCOUNT_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LOYALTY_MBR_OFFER(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.LOYALTY_MBR_OFFER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_LOYALTY_OFFER(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.LOYALTY_OFFER))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_MEMBER_OFFER_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.CUDM.MEMBER_OFFER_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_MENU_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.MENU_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_OFFER_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.CUDM.OFFER_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_OPERATION_MANAGEMENT_HIERARCHY(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.OPERATION_MANAGEMENT_HIERARCHY))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_ORDERLINEPREP_FACT(
	LOAD_ID,
	SOURCE_SYSTEM_NM,
	BRAND_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "CDMLOADDATE" AS "LOAD_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "BRANDID" AS "BRAND_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.ORDERLINEPREP_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_ORDERLINE_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.SADM.ORDERLINE_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_ORDER_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.SADM.ORDER_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_PAYMENT_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.SADM.PAYMENT_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_RESTAURANT_SCD_DIM(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.RESTAURANT_SCD_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_SALESFORECAST_DAY_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.SALESFORECAST_DAY_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_SALESFORECAST_QTRHR_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.SALESFORECAST_QTRHR_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_SCHEDULEDANDACTUALHOURS_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.SCHEDULEDANDACTUALHOURS_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_SPEEDOFSERVICE_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "CDMLOADDATE" AS "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.SPEEDOFSERVICE_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_STOREHOURS_DIM(
	BRAND_ID,
	LOAD_ID,
	SOURCE_SYSTEM_NM
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRANDID" AS "BRAND_ID", "CDMLOADDATE" AS "LOAD_ID", "SOURCE" AS "SOURCE_SYSTEM_NM" FROM ( SELECT  *  FROM (POLARIS_DEV.OPDM.STOREHOURS_DIM))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_STORE_HRS(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (IDM_DEV.COREDIM.STORE_HRS))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_WEATHER_FORECAST_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.SHDM.WEATHER_FORECAST_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
create or replace view TEMP_COUNT_LOADID_WEATHER_HOURLY_FACT(
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID
) as  SELECT  *  FROM ( SELECT  *  FROM ( SELECT "BRAND_ID", "SOURCE" AS "SOURCE_SYSTEM_NM", "LOAD_ID" FROM ( SELECT  *  FROM (POLARIS_DEV.SHDM.WEATHER_HOURLY_FACT))) WHERE ("LOAD_ID" = TO_CHAR(SYSDATE(), 'yyyyMMdd')));
CREATE OR REPLACE FUNCTION "CONVERT_TIMESTAMP_FROM_UTC_TO_TIMEZONE"("T" TIMESTAMP_NTZ(9), "TIME_ZONE" VARCHAR(16777216))
RETURNS TIMESTAMP_NTZ(9)
LANGUAGE JAVASCRIPT
AS '
    const time_zone_dict = {
        ''Central Standard Time'': ''America/Chicago'',
        ''Eastern Standard Time'': ''America/Detroit'',
        ''Pacific Standard Time'': ''America/Los_Angeles'',
        ''Mountain Standard Time'': ''America/Denver'',
        ''Atlantic Standard Time'': ''America/Barbados'',
        ''Alaskan Standard Time'': ''America/Anchorage'',
        ''Hawaiian Standard Time'': ''Pacific/Honolulu''
    };
    if(!TIME_ZONE) return null
    return new Date(new Date(T + "+00:00").toLocaleString("en-US", {timeZone: time_zone_dict[TIME_ZONE]})).getTime()
  ';
CREATE PROCEDURE IF NOT EXISTS "POPULATE_IDS_TABLES_MONITORING_BUSINESSDATE"()
RETURNS VARCHAR(16777216)
LANGUAGE SCALA
RUNTIME_VERSION = '2.12'
PACKAGES = ('com.snowflake:snowpark:1.6.0')
HANDLER = 'POPULATE_IDS_TABLES_MONITORING_BUSINESSDATE.run'
TARGET_PATH = '@ITPRCS_DEV.CSO.STORED_PROCEDURES/POPULATE_IDS_TABLES_MONITORING_BUSINESSDATE.jar'
EXECUTE AS CALLER
AS '
object POPULATE_IDS_TABLES_MONITORING_BUSINESSDATE {
    import com.snowflake.snowpark.functions._
    import com.snowflake.snowpark.{Column, DataFrame, Row, Session}
    import java.util.Objects

      private def parseBusinessDate(columnName: String): Column = {
            substring(
              replace(col(columnName), lit("-"), lit("")),
              lit(1),
              lit(8)
            )
      }

      def fetchThreeDaysBackBusinessDate(
          session:Session,
          tableNameTypeTuple: Seq[(String, String)],
          tempViewDbSchema: String
      ): DataFrame = {
        tableNameTypeTuple
          .map { case (tableName, tableType) =>
            (tableName.split("\\\\.").last, tableType, session.table(tableName))
          }
          .map { case (tableName, tableType, df) =>
            val fieldsName = df.schema.fields.map(_.name)
            val columns = fieldsName match {
              case x: Array[String]
                  if Seq("BUSINESS_DT", "SOURCE_SYSTEM_NM", "BRAND_ID").forall(
                    col => x.contains(col)
                  ) =>
                Array(
                  col("BRAND_ID"),
                  col("SOURCE_SYSTEM_NM"),
                  parseBusinessDate("BUSINESS_DT").alias("BUSINESS_DT")
                )
              case x: Array[String]
                  if Seq("EVENT_DATE", "SOURCE_SYSTEM_NM", "BRAND_ID")
                    .forall(col => x.contains(col)) =>
                Array(
                  col("BRAND_ID"),
                  col("SOURCE_SYSTEM_NM"),
                  parseBusinessDate("EVENT_DATE").alias("BUSINESS_DT")
                )
              case x: Array[String]
                  if Seq("BUSINESSDATE", "SOURCE", "BRANDID")
                    .forall(col => x.contains(col)) =>
                Array(
                  col("BRANDID").alias("BRAND_ID"),
                  col("SOURCE").as("SOURCE_SYSTEM_NM"),
                  parseBusinessDate("BUSINESSDATE").alias("BUSINESS_DT")
                )
              case x: Array[String]
                  if Seq("BUSINESS_DT", "BRAND_ID")
                    .forall(col => x.contains(col)) =>
                Array(
                  col("BRAND_ID"),
                  lit("N/A").as("SOURCE_SYSTEM_NM"),
                  parseBusinessDate("BUSINESS_DT").alias("BUSINESS_DT")
                )

              case _ => Array.empty[Column]
            }
            if (columns.length == 3) {
              df.select(columns)
                .where(
                  sqlExpr("TO_DATE(BUSINESS_DT, ''yyyyMMdd'')")
                    .geq(
                      dateadd(
                        "day",
                        lit(-3),
                        sysdate()
                      )
                    )
                )
                .createOrReplaceTempView(
                  s"$tempViewDbSchema.TEMP_COUNT_BT_$tableName"
                )
              val dfView =
                session.table(s"$tempViewDbSchema.TEMP_COUNT_BT_$tableName")
              val dfNew = dfView
                .groupBy(dfView.schema.fields.map(_.name))
                .agg(Array(count(lit("*")).alias("LOAD_COUNT")))
                .withColumn("TABLE_NAME", lit(tableName))
                .withColumn("TABLE_TYPE", lit(tableType))
              dfNew.select(dfNew.schema.fields.map(_.name))
            } else null
          }.filter(Objects.nonNull).reduce(_ unionAll _)
      }


      def run(session: com.snowflake.snowpark.Session): String = {
        val tempViewDbSchema = "ITPRCS_DEV.CSO"
        val tableDetails: Seq[(String, String)] = session
          .table(s"$tempViewDbSchema.IDS_TABLES_MONITORING_SOURCE")
          .collect()
          .map { row: Row =>
            (row.getString(0), row.getString(1))
          }
        val businessDateTableDestination =
          session.table(s"$tempViewDbSchema.IDS_TABLES_MONITORING_BUSINESSDATE")
        val toMergeDF =
          fetchThreeDaysBackBusinessDate(session, tableDetails, tempViewDbSchema)
        val toMergeFields = toMergeDF.schema.fields.map(_.name)
        val result = businessDateTableDestination
          .merge(
            toMergeDF,
            toMergeFields
              .filterNot(_.equalsIgnoreCase("LOAD_COUNT"))
              .map(colName =>
                toMergeDF(colName).equal_to(businessDateTableDestination(colName))
              )
              .reduce(_ and _)
          )
          .whenMatched
          .update(Map("LOAD_COUNT" -> toMergeDF("LOAD_COUNT")))
          .whenNotMatched
          .insert(toMergeFields.map(name => (name, toMergeDF(name))).toMap)
          .collect()
        s"Rows Inserted ${result.rowsInserted} - Rows Updated ${result.rowsUpdated}"
      }
  }
   ';
CREATE PROCEDURE IF NOT EXISTS "POPULATE_IDS_TABLES_MONITORING_LOADID"()
RETURNS VARCHAR(16777216)
LANGUAGE SCALA
RUNTIME_VERSION = '2.12'
PACKAGES = ('com.snowflake:snowpark:1.6.0')
HANDLER = 'POPULATE_IDS_TABLES_MONITORING_LOADID.run'
TARGET_PATH = '@ITPRCS_DEV.CSO.STORED_PROCEDURES/POPULATE_IDS_TABLES_MONITORING_LOADID.jar'
EXECUTE AS CALLER
AS '
object POPULATE_IDS_TABLES_MONITORING_LOADID {
    import com.snowflake.snowpark.functions._
    import com.snowflake.snowpark.{Column, DataFrame, Row, Session}
    import java.util.Objects

    def fetchByLoadId(session:Session, tableNameTypeTuple: Seq[(String, String)], tempViewDbSchema:String): DataFrame = {
      val firstDayOfPreviousMonth = dateadd(
        "day",
        lit(1),
        dateadd(
          "month",
          lit(-2),
          last_day(sysdate())
        )
      )
      val sysdateNoDashes = sqlExpr("TO_CHAR(SYSDATE(), ''yyyyMMdd'')")

      tableNameTypeTuple
        .map { case (tableName, tableType) =>
          (tableName.split("\\\\.").last, tableType, session.table(tableName))
        }
        .map { case (tableName, tableType, df) =>
          val fieldsName = df.schema.fields.map(_.name)
          val columns = fieldsName match {
            case x: Array[String]
                if Seq("LOAD_ID", "SOURCE_SYSTEM_NM", "BRAND_ID")
                  .forall(col => x.contains(col)) =>
              Array(
                col("BRAND_ID"),
                col("SOURCE_SYSTEM_NM"),
                col("LOAD_ID")
              )
            case x: Array[String]
                if Seq("LOAD_ID", "SOURCE_SYSTEM_NM", "SOURCE_BRAND_ID")
                  .forall(col => x.contains(col)) =>
              Array(
                col("SOURCE_BRAND_ID").as("BRAND_ID"),
                col("SOURCE_SYSTEM_NM"),
                col("LOAD_ID")
              )
            case x: Array[String]
                if Seq("CDMLOADDATE", "SOURCE", "BRANDID")
                  .forall(col => x.contains(col)) =>
              Array(
                col("BRANDID").alias("BRAND_ID"),
                col("SOURCE").as("SOURCE_SYSTEM_NM"),
                col("CDMLOADDATE").alias("LOAD_ID")
              )
            case x: Array[String]
                if Seq("LOAD_ID", "SOURCE", "BRAND_ID")
                  .forall(col => x.contains(col)) =>
              Array(
                col("BRAND_ID"),
                col("SOURCE").as("SOURCE_SYSTEM_NM"),
                col("LOAD_ID")
              )
            case x: Array[String]
                if Seq("LOAD_ID", "BRAND_ID")
                  .forall(col => x.contains(col)) =>
              Array(
                col("BRAND_ID"),
                lit("N/A").as("SOURCE_SYSTEM_NM"),
                col("LOAD_ID")
              )
            case x: Array[String]
                if Seq("CDMLOADDATE", "BRAND_ID")
                  .forall(col => x.contains(col)) =>
              Array(
                col("BRAND_ID"),
                lit("N/A").as("SOURCE_SYSTEM_NM"),
                col("CDMLOADDATE").alias("LOAD_ID")
              )

            case _ => Array.empty[Column]
          }

          if (columns.length == 3) {
            val onlyLastMonthBrandSSN = df
              .select(columns)
              .where(
                sqlExpr("try_to_date(load_id::string, ''yyyyMMdd'')")
                  .geq(firstDayOfPreviousMonth)
              )
              .distinct()
            val todayLoadIdsCountPerBrandSSN = df
              .select(columns)
              .where(col("LOAD_ID").equal_to(sysdateNoDashes))

            val outDF = onlyLastMonthBrandSSN.where(
              not(
                col("BRAND_ID")
                  .in(todayLoadIdsCountPerBrandSSN.select("BRAND_ID"))
              ).and(
                not(
                  col("SOURCE_SYSTEM_NM")
                    .in(todayLoadIdsCountPerBrandSSN.select("SOURCE_SYSTEM_NM"))
                )
              )
            )

            todayLoadIdsCountPerBrandSSN
              .createOrReplaceTempView(s"$tempViewDbSchema.TEMP_COUNT_LOADID_$tableName")
            if (outDF.count() == 0) {
              val dfView = session.table(s"$tempViewDbSchema.TEMP_COUNT_LOADID_$tableName")
              val dfNew = dfView
                .groupBy(dfView.schema.fields.map(_.name))
                .agg(Array(count(lit("*")).alias("LOAD_COUNT")))
                .withColumn("TABLE_NAME", lit(tableName))
                .withColumn("TABLE_TYPE", lit(tableType))
              dfNew.select(dfNew.schema.fields.map(_.name))
            } else {
              val dfView = session.table(s"$tempViewDbSchema.TEMP_COUNT_LOADID_$tableName")

              dfView
                .select(todayLoadIdsCountPerBrandSSN.schema.fields.map(_.name))
                .withColumn("TABLE_NAME", lit(tableName))
                .withColumn("TABLE_TYPE", lit(tableType))
                .withColumn("LOAD_COUNT", lit(0))
            }
          } else null
        }.filter(Objects.nonNull).reduce(_ unionAll _)
    }


  def run(session: com.snowflake.snowpark.Session): String = {
    val tempViewDbSchema = "ITPRCS_DEV.CSO"
    val tableDetails: Seq[(String, String)] = session
      .table(s"$tempViewDbSchema.IDS_TABLES_MONITORING_SOURCE")
      .collect()
      .map { row: Row =>
        (row.getString(0), row.getString(1))
      }

    val loadIdTableDestination =
      session.table(s"$tempViewDbSchema.IDS_TABLES_MONITORING_LOADID")
    val toMergeDF = fetchByLoadId(session, tableDetails, tempViewDbSchema)
    val toMergeFields = toMergeDF.schema.fields.map(_.name)
    val result = loadIdTableDestination
      .merge(
        toMergeDF,
        toMergeFields
          .filterNot(_.equalsIgnoreCase("LOAD_COUNT"))
          .map(colName =>
            toMergeDF(colName).equal_to(loadIdTableDestination(colName))
          )
          .reduce(_ and _)
      )
      .whenMatched
      .update(Map("LOAD_COUNT" -> toMergeDF("LOAD_COUNT")))
      .whenNotMatched
      .insert(toMergeFields.map(name => (name, toMergeDF(name))).toMap)
      .collect()
    s"Rows Inserted ${result.rowsInserted} - Rows Updated ${result.rowsUpdated}"
  }
}
';
CREATE PROCEDURE IF NOT EXISTS "POPULATE_STAGE_MONITOR_LOG"("STAGE_DB" VARCHAR(16777216), "STAGE_SCHEMA" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "MONITOR_TBL_DB" VARCHAR(16777216), "MONITOR_TBL_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
def run(session, stage_db, stage_schema, stage_name, monitor_tbl_db, monitor_tbl_schema):
  from snowflake.snowpark.functions import col,lit, sysdate, count

  sql_list = session.sql(f"list @{stage_db}.{stage_schema}.{stage_name}")

  sql_select = sql_list.where(col("size")>lit(0)).select(
    lit(stage_db).alias(''database''),
    lit(stage_schema).alias(''schema''),
    lit(stage_name).alias(''stage_name''),
    count(col("size")).alias("file_count"),
    sysdate().alias("log_timestamp"))

  sql_select.write.mode("append").saveAsTable(f"{monitor_tbl_db}.{monitor_tbl_schema}.STAGE_MONITORING_TEMP")
  return "successful"
';
CREATE PROCEDURE IF NOT EXISTS "POPULATE_STAGE_MONITOR_LOG_TABLE"("STAGE_DB" VARCHAR(16777216), "STAGE_SCHEMA" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "MONITOR_TBL_DB" VARCHAR(16777216), "MONITOR_TBL_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
def run(session, stage_db, stage_schema, stage_name, monitor_tbl_db, monitor_tbl_schema):
  from snowflake.snowpark.functions import col,lit, sysdate, count

  sql_list = session.sql("list @" + stage_db + "." + stage_schema + "." + stage_name)

  sql_select = sql_list.where(col("size")>lit(0)).select(
    lit(stage_db).alias(''database''),
    lit(stage_schema).alias(''schema''),
    lit(stage_name).alias(''stage_name''),
    count(col("size")).alias("file_count"),
    sysdate().alias("log_timestamp"))

  sql_select.write.mode("append").saveAsTable(monitor_tbl_db + "." + monitor_tbl_schema + ".STAGE_MONITORING_LOG")
  return "successful"
';
CREATE PROCEDURE IF NOT EXISTS "POPULATE_STAGE_MONITOR_LOG_TEMP"("STAGE_DB" VARCHAR(16777216), "STAGE_SCHEMA" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "MONITOR_TBL_DB" VARCHAR(16777216), "MONITOR_TBL_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SCALA
RUNTIME_VERSION = '2.12'
PACKAGES = ('com.snowflake:snowpark:1.4.0')
HANDLER = 'POPULATE_STAGE_MONITOR_LOG_TEMP.run'
EXECUTE AS CALLER
AS '
object POPULATE_STAGE_MONITOR_LOG_TEMP {
  def run(session: com.snowflake.snowpark.Session, stage_db: String, stage_schema: String, stage_name: String, monitor_tbl_db: String, monitor_tbl_schema: String): String = {
    import com.snowflake.snowpark.functions._
    val  sql_list = session.sql(f"list @${stage_db}.${stage_schema}.${stage_name}")
    val sql_select = sql_list.where(col("size")>lit(0)).select(lit(stage_db).alias("database"),
    lit(stage_schema).alias("schema"),
    lit(stage_name).alias("stage_name"),
    count(col("size")).alias("file_count"),
    sum(col("size")).alias("stage_size"),
    sysdate().alias("log_timestamp"))

    sql_select.write.mode("append").saveAsTable(f"${monitor_tbl_db}.${monitor_tbl_schema}.STAGE_MONITORING_TEMP")
    return "successful"
  }
}
  ';
CREATE PROCEDURE IF NOT EXISTS "SP_BASE_VIEW_CREATION"("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216), "SRC_TB_PREFIXES_PARAM" VARCHAR(16777216), "VIEW_SUFFIX" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
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
      return `SELECT table_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = :1 AND Table_schema = :2  ${where_table_name_stmt};`;
    }

    var select_table_names = prepareSelectTableNames(SRC_TB_PREFIXES_PARAM);

    var select_column_names = `SELECT column_name, comment FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_NAME = :3 order by ordinal_position;`

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
        var colum_name_obj = {};

        while(column_names.next())
        {
            var column_name = column_names.getColumnValue(1);
            var comment = column_names.getColumnValue(2);
            colum_name_obj[column_name] = comment ? ` comment ''${comment}''`: "";
        }

        var source_tb_columns = Object.entries(colum_name_obj).map(x => `"${x[0]}"`).join('', '');
        var column_name_with_comment = Object.entries(colum_name_obj).map(x => `"${x[0]}"${x[1]}`).join('', '');


        var view_template = `CREATE OR REPLACE VIEW ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}` + VIEW_SUFFIX +` COPY GRANTS (${column_name_with_comment})
                            AS SELECT
                               ${source_tb_columns}
                            FROM ` + SRC_DB_PARAM + `.` + SRC_SCHEMA_PARAM + `."${table_name}";`
        var execute_base_view_stmt = snowflake.createStatement(
        {
            sqlText: view_template
        });
        execute_base_view_stmt.execute();
    }

    var view_names = table_name_array.map(x => `${x}` + VIEW_SUFFIX).join('', '')
    try {

        return `The following base views are created: ${view_names} in the database: ${DST_DB_PARAM} and schema: ${DST_SCHEMA_PARAM}`;
        }
    catch (err)  {
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS "SP_BASE_VIEW_CREATION_TEST"("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216), "SRC_TB_PREFIXES_PARAM" VARCHAR(16777216), "VIEW_SUFFIX" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
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
      return `SELECT table_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = :1 AND Table_schema = :2  ${where_table_name_stmt};`;
    }

    var select_table_names = prepareSelectTableNames(SRC_TB_PREFIXES_PARAM);

    var select_column_names = `SELECT column_name, comment FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_NAME = :3 order by ordinal_position;`

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
        var colum_name_obj = {};

        while(column_names.next())
        {
            var column_name = column_names.getColumnValue(1);
            var comment = column_names.getColumnValue(2);
            colum_name_obj[column_name] = comment ? ` comment ''${comment}''`: "";
        }

        var source_tb_columns = Object.keys(colum_name_obj).join('', '');
        var column_name_with_comment = Object.entries(colum_name_obj).map(x => `${x[0]}${x[1]}`).join('', '');


        var view_template = `CREATE OR REPLACE VIEW ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}` + VIEW_SUFFIX +` COPY GRANTS (${column_name_with_comment})
                            AS SELECT
                               ${source_tb_columns}
                            FROM ` + SRC_DB_PARAM + `.` + SRC_SCHEMA_PARAM + `."${table_name}";`
        var execute_base_view_stmt = snowflake.createStatement(
        {
            sqlText: view_template
        });
        
        return view_template
//        execute_base_view_stmt.execute();
    }

    var view_names = table_name_array.map(x => `${x}` + VIEW_SUFFIX).join('', '')
    try {

        return `The following base views are created: ${view_names} in the database: ${DST_DB_PARAM} and schema: ${DST_SCHEMA_PARAM}`;
        }
    catch (err)  {
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS "SP_BASE_VIEW_CREATION_TEST_IRB"("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216), "SRC_TB_PREFIXES_PARAM" VARCHAR(16777216), "VIEW_SUFFIX" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
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
      return `SELECT table_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = :1 AND Table_schema = :2  ${where_table_name_stmt};`;
    }

    var select_table_names = prepareSelectTableNames(SRC_TB_PREFIXES_PARAM);

    var select_column_names = `SELECT column_name, comment FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_NAME = :3 order by ordinal_position;`

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
        var colum_name_obj = {};

        while(column_names.next())
        {
            var column_name = column_names.getColumnValue(1);
            var comment = column_names.getColumnValue(2);
            colum_name_obj[column_name] = comment ? ` comment ''${comment}''`: "";
        }
        
        var source_tb_columns = Object.entries(colum_name_obj).map(x => `"${x[0]}"`).join('', '');
        var column_name_with_comment = Object.entries(colum_name_obj).map(x => `"${x[0]}"${x[1]}`).join('', '');

        var view_template = `CREATE OR REPLACE VIEW ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}` + VIEW_SUFFIX +` COPY GRANTS (${column_name_with_comment})
                            AS SELECT
                               ${source_tb_columns}
                            FROM ` + SRC_DB_PARAM + `.` + SRC_SCHEMA_PARAM + `."${table_name}";`
        var execute_base_view_stmt = snowflake.createStatement(
        {
            sqlText: view_template
        });
        
        return view_template
//        execute_base_view_stmt.execute();
    }

    var view_names = table_name_array.map(x => `${x}` + VIEW_SUFFIX).join('', '')
    try {

        return `The following base views are created: ${view_names} in the database: ${DST_DB_PARAM} and schema: ${DST_SCHEMA_PARAM}`;
        }
    catch (err)  {
        throw err;
        }
    ';
CREATE PROCEDURE IF NOT EXISTS "STAGE_MONITORING_SCALA_V1"()
RETURNS VARCHAR(16777216)
LANGUAGE SCALA
RUNTIME_VERSION = '2.12'
PACKAGES = ('com.snowflake:snowpark:1.4.0')
HANDLER = 'STAGE_MONITORING_SCALA_V1.run'
EXECUTE AS CALLER
AS '
object STAGE_MONITORING_SCALA_V1 {
  def run(session: com.snowflake.snowpark.Session): String = {
    import com.snowflake.snowpark.functions._
    val  sql_list = session.sql("list @RDS_DEV.ARB.IDP_ORDER_STAGE")
    val sql_select_files = sql_list.where(col("size")>lit(0)).select(lit("RDS_DEV").alias("database"),
    lit("ARB").alias("schema"),
    lit("IDP_ORDER_STAGE").alias("stage_name"),
    count(col("size")).alias("file_count"),
    sum(col("size")).alias("stage_size"),
    sysdate().alias("log_timestamp"))
    sql_select_files.write.mode("append").saveAsTable("ITPRCS_DEV.CSO.STAGE_MONITORING_LOG")
    return "successful"
  }
}
  ';
CREATE PROCEDURE IF NOT EXISTS "TEST_IRB_BASE_VIEW_CREATION"("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216), "SRC_TB_PREFIXES_PARAM" VARCHAR(16777216))
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
        
        var view_template = `CREATE OR REPLACE VIEW ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}_BV COPY GRANTS
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