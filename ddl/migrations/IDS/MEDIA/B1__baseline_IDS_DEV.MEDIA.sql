create sequence IF NOT EXISTS DAYNUMBER start with 1 increment by 1;
create sequence IF NOT EXISTS SEQ1 start with 1 increment by 1;
create sequence IF NOT EXISTS SEQ_01 start with 1 increment by 1;
create sequence IF NOT EXISTS TEST start with 1 increment by 1;
create TABLE IF NOT EXISTS COMPETITOR_MEDIA_SPEND_WEEK (
	COMPETITOR_MEDIA_SPEND_ID NUMBER(38,0) NOT NULL autoincrement COMMENT 'Primary Key of the table.',
	WEEK_START_DT DATE COMMENT 'Week start date. This should always be Monday''s date.',
	ADVERTISER_NM VARCHAR(16777216) COMMENT 'Inspire brands competitor advertiser. Ex: Burger King, Wendys.',
	PRODUCT_NM VARCHAR(16777216) COMMENT 'Name of the product.',
	MEDIA_TYP VARCHAR(16777216) COMMENT 'Type of media. Ex: TV, Radio, Print etc.',
	DMA_CD VARCHAR(16777216) COMMENT 'Market Area Code',
	SPEND_AMT NUMBER(18,2) COMMENT 'Amount spent on media for a specific week',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand of the advertiser.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Data collected from source',
	LOAD_ID VARCHAR(16777216) COMMENT 'Job Id.',
	LOAD_DTTM DATE COMMENT 'Date when data is loaded into the table.',
	UPDATE_ID VARCHAR(16777216) COMMENT 'Updated By.',
	UPDATE_DTTM DATE COMMENT 'Date when data is updated in the table.',
	constraint XPKCOMPETITOR_MEDIA_SPEND_WEEK primary key (COMPETITOR_MEDIA_SPEND_ID)
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
create TABLE IF NOT EXISTS MEDIA_CAMPAIGN (
	CAMPAIGN_ID NUMBER(38,0) NOT NULL autoincrement,
	CAMPAIGN_NM VARCHAR(16777216),
	CAMPAIGN_DESC VARCHAR(16777216),
	STANDARD_CAMPAIGN_NM VARCHAR(16777216),
	STANDARD_CAMPAIGN_DESC VARCHAR(16777216),
	STANDARD_OBJECTIVE_TYP VARCHAR(16777216),
	MEDIA_TYP VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_CAMPAIGN primary key (CAMPAIGN_ID)
);
create TABLE IF NOT EXISTS MEDIA_CAMPAIGN_DETAIL (
	CAMPAIGN_DETAIL_ID NUMBER(38,0) NOT NULL autoincrement,
	PARTNER_ID NUMBER(38,0),
	STRATEGY_ID NUMBER(38,0),
	CHANNEL_ID NUMBER(38,0),
	CAMPAIGN_ID NUMBER(38,0),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_CAMPAIGN_DETAIL primary key (CAMPAIGN_DETAIL_ID),
	constraint MC_MCD foreign key (CHANNEL_ID) references MEDIA_CHANNEL(CHANNEL_ID),
	constraint MCP_MCD foreign key (CAMPAIGN_ID) references MEDIA_CAMPAIGN(CAMPAIGN_ID)
);
create TABLE IF NOT EXISTS MEDIA_CHANNEL (
	CHANNEL_ID NUMBER(38,0) NOT NULL autoincrement,
	CHANNEL_TYPE_ID NUMBER(38,0),
	CHANNEL_NM VARCHAR(16777216),
	CHANNEL_DESC VARCHAR(16777216),
	PARENT_CHANNEL_ID NUMBER(38,0),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_CHANNEL primary key (CHANNEL_ID),
	constraint MC_MC foreign key (PARENT_CHANNEL_ID) references MEDIA_CHANNEL(CHANNEL_ID),
	constraint MCT_MC foreign key (CHANNEL_TYPE_ID) references MEDIA_CHANNEL_TYPE(CHANNEL_TYPE_ID)
);
create TABLE IF NOT EXISTS MEDIA_CHANNEL_HIERARCHY_REF (
	CHANNEL_HIERARCHY_ID NUMBER(38,0) NOT NULL autoincrement,
	SOURCE_CHANNEL_NM VARCHAR(16777216),
	SOURCE_SUBCHANNEL_NM VARCHAR(16777216),
	SOURCE_SUBCHANNEL2_NM VARCHAR(16777216),
	SOURCE_PARTNER_NM VARCHAR(16777216),
	INSPIRE_CHANNEL_NM VARCHAR(16777216),
	INSPIRE_SUBCHANNEL_NM VARCHAR(16777216),
	INSPIRESUBCHANNEL2_NM VARCHAR(16777216),
	INSPIRE_PARTNER_NM VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_CHANNEL_HIERARCHY_REF primary key (CHANNEL_HIERARCHY_ID)
);
create TABLE IF NOT EXISTS MEDIA_CHANNEL_HIERARCHY_REF_BKP (
	CHANNEL_HIERARCHY_ID NUMBER(38,0) NOT NULL autoincrement,
	SOURCE_CHANNEL_NM VARCHAR(16777216),
	SOURCE_SUBCHANNEL_NM VARCHAR(16777216),
	SOURCE_SUBCHANNEL2_NM VARCHAR(16777216),
	SOURCE_PARTNER_NM VARCHAR(16777216),
	INSPIRE_CHANNEL_NM VARCHAR(16777216),
	INSPIRE_SUBCHANNEL_NM VARCHAR(16777216),
	INSPIRESUBCHANNEL2_NM VARCHAR(16777216),
	INSPIRE_PARTNER_NM VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_CHANNEL_HIERARCHY_REF primary key (CHANNEL_HIERARCHY_ID)
);
create TABLE IF NOT EXISTS MEDIA_CHANNEL_TYPE (
	CHANNEL_TYPE_ID NUMBER(38,0) NOT NULL autoincrement,
	CHANNEL_TYPE_NM VARCHAR(16777216),
	CHANNEL_TYPE_DESC VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_CHANNEL_TYPE primary key (CHANNEL_TYPE_ID)
);
create TABLE IF NOT EXISTS MEDIA_DMA_REF (
	DMA_CROSSWALK_ID NUMBER(38,0) NOT NULL autoincrement,
	SOURCE_DMA_CD VARCHAR(16777216),
	SOURCE_DMA_NM VARCHAR(16777216),
	INSPIRE_DMA_CD VARCHAR(16777216),
	INSPIRE_DMA_NM VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_DMA_REF primary key (DMA_CROSSWALK_ID)
);
create TABLE IF NOT EXISTS MEDIA_DMA_REF_BKP (
	DMA_CROSSWALK_ID NUMBER(38,0) NOT NULL autoincrement,
	SOURCE_DMA_CD VARCHAR(16777216),
	SOURCE_DMA_NM VARCHAR(16777216),
	INSPIRE_DMA_CD VARCHAR(16777216),
	INSPIRE_DMA_NM VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_DMA_REF primary key (DMA_CROSSWALK_ID)
);
create TABLE IF NOT EXISTS MEDIA_PARTNER (
	PARTNER_ID NUMBER(38,0) NOT NULL autoincrement,
	CHANNEL_TYPE_ID NUMBER(38,0),
	PARTNER_NM VARCHAR(16777216),
	PARTNER_DESC VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_PARTNER primary key (PARTNER_ID),
	constraint MCT_MP foreign key (CHANNEL_TYPE_ID) references MEDIA_CHANNEL_TYPE(CHANNEL_TYPE_ID)
);
create TABLE IF NOT EXISTS MEDIA_SPEND_TYPE (
	SPEND_TYPE_ID NUMBER(38,0) NOT NULL autoincrement,
	SPEND_TYPE_NM VARCHAR(16777216),
	SPEND_TYPE_DESC VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_SPEND_TYPE primary key (SPEND_TYPE_ID)
);
create TABLE IF NOT EXISTS MEDIA_SPEND_WEEK (
	SPEND_ID NUMBER(38,0) NOT NULL autoincrement,
	WEEK_START_DT DATE NOT NULL,
	DMA_CD VARCHAR(16777216),
	CAMPAIGN_DETAIL_ID NUMBER(38,0),
	SPEND_TYPE_ID NUMBER(38,0),
	SPEND_AMT FLOAT,
	CURRENCY_CD VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_SPEND_WEEK primary key (SPEND_ID, WEEK_START_DT),
	constraint MCD_MSW foreign key (CAMPAIGN_DETAIL_ID) references MEDIA_CAMPAIGN_DETAIL(CAMPAIGN_DETAIL_ID),
	constraint MST_MSW foreign key (SPEND_TYPE_ID) references MEDIA_SPEND_TYPE(SPEND_TYPE_ID)
);
create TABLE IF NOT EXISTS MEDIA_SPEND_WEEK_06202022 (
	SPEND_ID NUMBER(38,0),
	WEEK_START_DT DATE,
	DMA_CD VARCHAR(16777216),
	CAMPAIGN_DETAIL_ID NUMBER(38,0),
	SPEND_TYPE_ID NUMBER(38,0),
	SPEND_AMT FLOAT,
	CURRENCY_CD VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS MEDIA_SPEND_WEEK_TEST (
	SPEND_ID NUMBER(38,0) NOT NULL autoincrement,
	WEEK_START_DT DATE NOT NULL,
	DMA_CD VARCHAR(16777216),
	CAMPAIGN_DETAIL_ID NUMBER(38,0),
	SPEND_TYPE_ID NUMBER(38,0),
	SPEND_AMT FLOAT,
	CURRENCY_CD VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint MST_MSW foreign key (SPEND_TYPE_ID) references MEDIA_SPEND_TYPE(SPEND_TYPE_ID),
	constraint XPKMEDIA_SPEND_WEEK primary key (SPEND_ID, WEEK_START_DT)
);
create TABLE IF NOT EXISTS MEDIA_STRATEGY (
	STRATEGY_ID NUMBER(38,0) NOT NULL autoincrement,
	STRATEGY_TYPE_ID NUMBER(38,0),
	STRATEGY_NM VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_STRATEGY primary key (STRATEGY_ID),
	constraint MST_MS foreign key (STRATEGY_TYPE_ID) references MEDIA_STRATEGY_TYPE(STRATEGY_TYPE_ID)
);
create TABLE IF NOT EXISTS MEDIA_STRATEGY_TEST (
	STRATEGY_ID NUMBER(38,0) NOT NULL autoincrement,
	STRATEGY_TYPE_ID NUMBER(38,0),
	STRATEGY_NM VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint MST_MS foreign key (STRATEGY_TYPE_ID) references MEDIA_STRATEGY_TYPE(STRATEGY_TYPE_ID),
	constraint XPKMEDIA_STRATEGY primary key (STRATEGY_ID)
);
create TABLE IF NOT EXISTS MEDIA_STRATEGY_TYPE (
	STRATEGY_TYPE_ID NUMBER(38,0) NOT NULL autoincrement,
	STRATEGY_TYPE_NM VARCHAR(16777216),
	STRATEGY_TYPE_DESC VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	constraint XPKMEDIA_STRATEGY_TYPE primary key (STRATEGY_TYPE_ID)
);
create TABLE IF NOT EXISTS MEDIA_TRACKER_WEEK (
	TRACKER_ID NUMBER(38,0) NOT NULL autoincrement,
	WEEK_START_DT DATE NOT NULL,
	DMA_CD VARCHAR(16777216),
	CAMPAIGN_DETAIL_ID NUMBER(38,0),
	IMPRESSIONS_CNT NUMBER(38,6),
	RATINGS_CNT NUMBER(38,6),
	CLICKS_CNT NUMBER(38,6),
	GRP_CNT NUMBER(38,6),
	TRP_CNT NUMBER(38,6),
	IMPRESSIONS_TYP VARCHAR(16777216) COMMENT 'Define the type of Impressions.the values P or (A or null) to indicate whether it’s planned / actual.',
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS MEDIA_TRACKER_WEEK_06202022 (
	TRACKER_ID NUMBER(38,0),
	WEEK_START_DT DATE,
	DMA_CD VARCHAR(16777216),
	CAMPAIGN_DETAIL_ID NUMBER(38,0),
	IMPRESSIONS_CNT NUMBER(38,6),
	RATINGS_CNT NUMBER(38,6),
	CLICKS_CNT NUMBER(38,6),
	GRP_CNT NUMBER(38,6),
	TRP_CNT NUMBER(38,6),
	IMPRESSIONS_TYP VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID VARCHAR(16777216),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS TOTAL_MEDIA_UPLIFT (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	WK_START_DATE TIMESTAMP_NTZ(9) NOT NULL,
	DMA_CODE VARCHAR(16777216) NOT NULL,
	CHANNEL VARCHAR(16777216) NOT NULL,
	SUBCHANNEL VARCHAR(16777216) NOT NULL,
	SUBCHANNEL2 VARCHAR(16777216) NOT NULL,
	PARTNER VARCHAR(16777216) NOT NULL,
	SPEND_AMOUNT FLOAT,
	ACTIVITY_VALUE FLOAT,
	TRANS_UPLIFT FLOAT,
	TRANS_EFFICIENCY FLOAT,
	TRANS_MARGINAL FLOAT,
	WEEKLY_TRANS_AMOUNT FLOAT,
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	constraint XPKTOTAL_MEDIA_UPLIFT primary key (BRAND_ID, WK_START_DATE, DMA_CODE, CHANNEL, SUBCHANNEL, SUBCHANNEL2, PARTNER)
)COMMENT='Total Media Uplift. It is souced from Data Science periodically. Old data is deleted and new data reloaded.'
;
create TABLE IF NOT EXISTS TOTAL_MODEL_UPLIFT (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	WK_START_DATE TIMESTAMP_NTZ(9) NOT NULL,
	DMA_CODE VARCHAR(16777216) NOT NULL,
	GROUP_1_TEXT VARCHAR(16777216) NOT NULL,
	GROUP_2_TEXT VARCHAR(16777216) NOT NULL,
	GROUP_3_TEXT VARCHAR(16777216) NOT NULL,
	GROUP_4_TEXT VARCHAR(16777216) NOT NULL,
	GROUP_5_TEXT VARCHAR(16777216) NOT NULL,
	TRANS_UPLIFT FLOAT,
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	constraint XPKTOTAL_MODEL_UPLIFT primary key (BRAND_ID, WK_START_DATE, DMA_CODE, GROUP_1_TEXT, GROUP_2_TEXT, GROUP_3_TEXT, GROUP_4_TEXT, GROUP_5_TEXT)
)COMMENT='Total Model Uplift. It is souced from Data Science periodically. Old data is deleted and new data reloaded.'
;
CREATE FILE FORMAT IF NOT EXISTS ENTERPRISES_FORMAT
	TYPE = csv
	FIELD_DELIMITER = '|'
;
CREATE FILE FORMAT IF NOT EXISTS MEDIA_CSV_FORMAT
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	ENCODING = 'iso-8859-1'
;
CREATE FILE FORMAT IF NOT EXISTS REF_TABLES_CSV_FORMAT
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
;
CREATE PROCEDURE IF NOT EXISTS CHECK_RULES("ENV" VARCHAR(16777216), "BRAND" VARCHAR(16777216), "SOURCE_SYSTEM" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
        var sql_rules_text = "SELECT * FROM IDS_" + ENV + ".INT_REF.DQ_VALIDATION_RULE WHERE RULE_ACTIVE_IND = TRUE AND BRAND_ID = ''" + BRAND + "'' AND SOURCE_SYSTEM_NAME = ''" + SOURCE_SYSTEM + "''";
        //var sql_rules_text = "SELECT * FROM IDS_DEV.INT_REF.DQ_VALIDATION_RULE WHERE RULE_ACTIVE_IND = TRUE AND DQ_RULE_ID IN (1,2,3,4)";
        var sql_rules_statement = snowflake.createStatement( {sqlText:sql_rules_text} );
        var sql_rules_execution = sql_rules_statement.execute();
        var sql_rules_final_result = [];
        
        while (sql_rules_execution.next()){
            var rule_text = sql_rules_execution.getColumnValue(''RULE_SQL_TEXT'');
            var rule_id = sql_rules_execution.getColumnValue(''DQ_RULE_ID'');
            var rule_brand = sql_rules_execution.getColumnValue(''BRAND_ID'');
            var rule_business_key = sql_rules_execution.getColumnValue(''COLUMN_NAME'');
            var rule_source = sql_rules_execution.getColumnValue(''SOURCE_SYSTEM_NAME'');
            var rule_status_type = sql_rules_execution.getColumnValue(''RULE_ACTIVE_IND'');
            var rule_business_key_value = "";
            var rule_output_text = "";
                        
            var rule_statement = snowflake.createStatement({sqlText:rule_text});
            var rule_execution = rule_statement.execute();
            while (rule_execution.next()) {
            
                var rule_result = rule_execution.getColumnValue(1);
                if (rule_business_key == "N/A") {
                    rule_business_key_value = "N/A";
                    rule_output_text = rule_result;
                } else {
                    rule_business_key_value = rule_result;
                    rule_output_text = "N/A";
                }
                
                var insert_sql_text = "\\
                    INSERT INTO IDS_DEV.INT_REF.DQ_VALIDATION_RULE_RESULT (DQ_RULE_ID, BRAND_ID, TABLE_BUSINESS_KEY, BUSINESS_KEY_VALUE, RULE_SQL_OUTPUT_TEXT, DATA_BUSINESS_DATE, RESULT_STATUS_TYPE, SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM) \\
                    VALUES (\\
                        " + rule_id + ", \\
                        ''" + rule_brand + "'', \\
                        ''" + rule_business_key + "'', \\
                        ''" + rule_business_key_value + "'', \\
                        ''" + rule_output_text + "'', \\
                        CURRENT_DATE, \\
                        ''" + rule_status_type + "'', \\
                        ''" + rule_source + "'', \\
                        TO_CHAR(CURRENT_DATE, ''YYYYMMDD''), \\
                        TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)), \\
                        TO_CHAR(CURRENT_DATE, ''YYYYMMDD''), \\
                        TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) \\
                    )"
                var insert_statement = snowflake.createStatement({sqlText:insert_sql_text});
                var insert_execution = insert_statement.execute();
                //insert_execution.next();
                
                final_result_json = {rule_id, rule_brand, rule_source, rule_business_key, rule_business_key_value, rule_output_text};
                sql_rules_final_result.push(final_result_json);
            }
    
      }
      return sql_rules_final_result; 
    ';
CREATE FUNCTION IF NOT EXISTS DATE_FORMAT("DATE_COLUMN" VARCHAR(16777216))
RETURNS DATE
LANGUAGE SQL
AS '
        SELECT
            CASE WHEN (TRY_TO_DATE(REGEXP_REPLACE(date_column,''/'',''-''))) IS NULL THEN to_date(to_char(date(to_timestamp(trim(TO_DATE(REGEXP_REPLACE(date_column,''/'',''-''),''mm-dd-yyyy'')))), ''yy-MM-dd''), ''yy-MM-dd'')
            ELSE to_date(to_char(date(to_timestamp(trim(REGEXP_REPLACE(date_column,''/'',''-'')))), ''yy-MM-dd''), ''yy-MM-dd'')
            END
     ';
CREATE PROCEDURE IF NOT EXISTS LOAD_DATA_INTO_IDS("ENV" VARCHAR(16777216), "SCHEMA" VARCHAR(16777216), "BRAND" VARCHAR(16777216), "SOURCE_SYSTEM" VARCHAR(16777216), "LOAD_TYPE" VARCHAR(16777216), "TEMP_TABLE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS CALLER
AS '  
    var merge_media_partner = "\\
      MERGE INTO IDS_"+ENV+"."+SCHEMA+".media_partner tgt \\
      USING ( \\
            SELECT DISTINCT \\
                df.CHANNEL_TYPE_ID \\
                ,df.Partner_NM \\
                ,df.Partner_Desc \\
                ,df.LOAD_TYPE AS LOAD_TYP \\
                ,df.BRAND_ID AS BRAND_ID \\
                ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM \\
                ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID \\
                ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM \\
                ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID \\
                ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM \\
             FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df \\
             LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_PARTNER mp \\
                ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mp.Partner_NM)) \\
                    AND mp.Brand_Id = df.BRAND_ID \\
                    AND mp.Source_System_NM = df.SOURCE_SYSTEM_NM WHERE mp.Partner_ID IS NULL \\
              ) src \\
              ON LOWER(TRIM(src.PARTNER_NM)) = LOWER(TRIM(tgt.PARTNER_NM)) \\
                  AND src.BRAND_ID = tgt.BRAND_ID \\
                  AND src.SOURCE_SYSTEM_NM = tgt.SOURCE_SYSTEM_NM \\
                 WHEN NOT MATCHED \\
                  THEN INSERT (Channel_type_id,Partner_NM,Partner_Desc,load_typ,brand_id,source_system_nm,load_id,load_dttm,update_id,update_dttm) \\
                       VAlUES (src.Channel_type_id,src.Partner_NM,src.Partner_Desc,src.load_typ,src.brand_id,src.source_system_nm,src.load_id,src.load_dttm,src.update_id,src.update_dttm) \\
    ";
    
    var merge_media_campaign = "\\
      MERGE INTO IDS_"+ENV+"."+SCHEMA+".media_campaign tgt\\
      USING (\\
          SELECT DISTINCT\\
            df.campaign_name\\
            ,df.campaign_desc\\
            ,null Standard_Campaign_Nm\\
            ,null Standard_Campaign_Desc\\
            ,null Standard_Objective_Typ\\
            ,null Media_Typ\\
            ,df.LOAD_TYPE AS LOAD_TYP\\
            ,df.BRAND_ID AS BRAND_ID\\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
          FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_CAMPAIGN mc\\
              ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mc.CAMPAIGN_NM)) AND\\
                 mc.Brand_Id = df.BRAND_ID AND\\
                 mc.Source_System_NM = df.SOURCE_SYSTEM_NM\\
          WHERE mc.CAMPAIGN_ID IS NULL AND LOWER(TRIM(df.CAMPAIGN_NAME)) != ''undefined''\\
        ) src\\
        ON LOWER(TRIM(src.CAMPAIGN_NAME)) = LOWER(TRIM(tgt.CAMPAIGN_NM)) AND\\
           src.BRAND_ID = tgt.BRAND_ID AND\\
           src.SOURCE_SYSTEM_NM = tgt.SOURCE_SYSTEM_NM\\
        WHEN NOT MATCHED\\
          THEN INSERT (CAMPAIGN_NM,CAMPAIGN_DESC,STANDARD_CAMPAIGN_NM,STANDARD_CAMPAIGN_DESC,STANDARD_OBJECTIVE_TYP,MEDIA_TYP,LOAD_TYP,BRAND_ID,SOURCE_SYSTEM_NM,LOAD_ID,LOAD_DTTM,UPDATE_ID,UPDATE_DTTM)\\
               VALUES (src.CAMPAIGN_NAME,src.CAMPAIGN_DESC,src.STANDARD_CAMPAIGN_NM,src.STANDARD_CAMPAIGN_DESC,src.STANDARD_OBJECTIVE_TYP,\\
                       src.MEDIA_TYP,src.LOAD_TYP,BRAND_ID,src.SOURCE_SYSTEM_NM,src.LOAD_ID,src.LOAD_DTTM,src.UPDATE_ID,src.UPDATE_DTTM)\\
    ";
    
    var merge_media_strategy ="\\
        MERGE INTO IDS_"+ENV+"."+SCHEMA+".media_strategy tgt\\
        USING (\\
          SELECT DISTINCT\\
            df.strategy_type_id\\
            ,df.strategy_nm\\
            ,df.LOAD_TYPE AS LOAD_TYP\\
            ,df.BRAND_ID AS BRAND_ID\\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
          FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_STRATEGY ms\\
               ON LOWER(TRIM(df.strategy_nm)) = LOWER(TRIM(ms.strategy_nm)) AND\\
                  ms.Brand_Id = df.BRAND_ID AND\\
                  ms.Source_System_NM = df.SOURCE_SYSTEM_NM\\
          WHERE ms.STRATEGY_ID IS NULL AND LOWER(TRIM(df.strategy_nm)) != ''undefined''\\
        ) src\\
        ON LOWER(TRIM(src.Strategy_NM)) = LOWER(TRIM(tgt.Strategy_NM)) AND\\
           src.BRAND_ID = tgt.BRAND_ID AND\\
           src.SOURCE_SYSTEM_NM = tgt.SOURCE_SYSTEM_NM\\
        WHEN NOT MATCHED\\
          THEN INSERT (STRATEGY_TYPE_ID,STRATEGY_NM,LOAD_TYP,BRAND_ID,SOURCE_SYSTEM_NM,LOAD_ID,LOAD_DTTM,UPDATE_ID,UPDATE_DTTM)\\
               VALUES (src.STRATEGY_TYPE_ID,src.STRATEGY_NM,src.LOAD_TYP,src.BRAND_ID,src.SOURCE_SYSTEM_NM,src.LOAD_ID,src.LOAD_DTTM,src.UPDATE_ID,src.UPDATE_DTTM)\\
    ";
    
    var merge_media_campaign_detail = "\\
        MERGE INTO IDS_"+ENV+"."+SCHEMA+".media_campaign_detail tgt\\
        USING \\
        (\\
          SELECT\\
            Partner_Id\\
            ,Strategy_Id\\
            ,Channel_Id\\
            ,Campaign_Id\\
            ,LOAD_TYP\\
            ,BRAND_ID\\
            ,SOURCE_SYSTEM_NM\\
            ,LOAD_ID\\
            ,LOAD_DTTM\\
            ,UPDATE_ID\\
            ,UPDATE_DTTM\\
          FROM\\
          (\\
            SELECT DISTINCT\\
              lower(trim(df.SUBCHANNEL2))\\
              ,lower(trim(mcp.SubChannel2))\\
              ,mcpp.Campaign_Id\\
              ,mcp.Channel_Id\\
              ,mpp.Partner_Id\\
              ,msp.Strategy_Id\\
              ,df.LOAD_TYPE AS LOAD_TYP\\
              ,''irb'' AS BRAND_ID\\
              ,''internal'' AS SOURCE_SYSTEM_NM\\
              ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
              ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
              ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
              ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
            FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
            LEFT JOIN (\\
                       SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt\\
                       WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined''\\
                       UNION \\
                       SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct \\
                       WHERE mct.CAMPAIGN_NM = ''Undefined'' AND mct.BRAND_ID = ''irb'' AND mct.SOURCE_SYSTEM_NM = ''internal''\\
                      ) mcpp\\
                      ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM))\\
            LEFT JOIN (\\
                        SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1\\
                        LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id\\
                        LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id\\
                        LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id\\
                        WHERE l3.Channel_Type_Id = ''3''\\
                      ) mcp \\
                      ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2))\\
            LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp \\
                      ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID AND mpp.Source_System_NM = df.SOURCE_SYSTEM_NM\\
            LEFT JOIN (\\
                       SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt\\
                       WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined''\\
                       UNION \\
                       SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                       WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal''\\
                      ) msp\\
                      ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM))\\
            LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON\\
                      mcpp.Campaign_Id = mcdp.Campaign_Id AND\\
                      mcp.Channel_Id = mcdp.Channel_Id AND\\
                      mpp.Partner_Id = mcdp.Partner_Id AND\\
                      msp.Strategy_Id = mcdp.Strategy_Id AND\\
                      mcdp.Brand_Id = df.GENERIC_BRAND AND mcdp.Source_System_NM = df.GENERIC_SOURCE\\
            WHERE mcdp.Campaign_Id IS NULL AND mcdp.Channel_Id IS NULL AND mcdp.Partner_Id IS NULL AND mcdp.Strategy_Id IS NULL\\
          ) s1\\
        ) src\\
        ON src.CAMPAIGN_ID = tgt.CAMPAIGN_ID AND\\
           src.CHANNEL_ID = tgt.CHANNEL_ID AND\\
           src.PARTNER_ID = tgt.PARTNER_ID AND\\
           src.STRATEGY_ID = tgt.STRATEGY_ID AND\\
           src.BRAND_ID = tgt.BRAND_ID AND\\
           src.SOURCE_SYSTEM_NM = tgt.SOURCE_SYSTEM_NM\\
        WHEN NOT MATCHED\\
          THEN INSERT (PARTNER_ID,STRATEGY_ID,CHANNEL_ID,CAMPAIGN_ID,LOAD_TYP,BRAND_ID,SOURCE_SYSTEM_NM,LOAD_ID,LOAD_DTTM,UPDATE_ID,UPDATE_DTTM)\\
               VALUES (src.PARTNER_ID,src.STRATEGY_ID,src.CHANNEL_ID,src.CAMPAIGN_ID,src.LOAD_TYP,src.BRAND_ID,src.SOURCE_SYSTEM_NM,src.LOAD_ID,src.LOAD_DTTM,src.UPDATE_ID,src.UPDATE_DTTM)\\
    ";
    
   var merge_media_tracker = "\\
        MERGE INTO IDS_"+ENV+"."+SCHEMA+".media_tracker_week tgt\\
        USING (\\
          SELECT\\
            week_start_dt\\
            ,DMA_CD\\
            ,CAMPAIGN_DETAIL_ID\\
            ,IMPRESSIONS_CNT\\
            ,RATINGS_CNT\\
            ,CLICKS_CNT\\
            ,GRP_CNT\\
            ,TRP_CNT\\
            ,LOAD_TYP\\
            ,IMPRESSIONS_TYP\\
            ,BRAND_ID\\
            ,SOURCE_SYSTEM_NM\\
            ,LOAD_ID\\
            ,LOAD_DTTM\\
            ,UPDATE_ID\\
            ,UPDATE_DTTM\\
           FROM\\
            (\\
              SELECT s1.* FROM\\
                (\\
                SELECT\\
                  df.week_start_dt\\
                  ,df.DMA_CD\\
                  ,df.grp as grp_cnt\\
                  ,df.ratings as ratings_cnt\\
                  ,df.clicks clicks_cnt\\
                  ,df.IMPRESSIONS_TYP \\
                  ,mcdp.Campaign_Detail_Id\\
                  ,df.Impressions_cnt Impressions_CNT\\
                  ,df.TRP trp_cnt\\
                  ,df.LOAD_TYPE AS LOAD_TYP\\
                  ,df.BRAND_ID AS BRAND_ID\\
                  ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
                  ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
                  ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
                  ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
                  ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
                FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
                LEFT JOIN (\\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt\\
                           WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined''\\
                           UNION \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct \\
                           WHERE mct.CAMPAIGN_NM = ''Undefined'' AND mct.BRAND_ID = ''irb'' AND mct.SOURCE_SYSTEM_NM = ''internal''\\
                          ) mcpp\\
                         ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM))    \\
                LEFT JOIN (\\
                          SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id\\
                          WHERE l3.Channel_Type_Id = ''3''\\
                          ) mcp \\
                          ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2))\\
                LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp\\
                         ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID and mpp.Source_System_NM = df.SOURCE_SYSTEM_NM\\
                LEFT JOIN (\\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt\\
                           WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined''\\
                           UNION \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                           WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal''\\
                          ) msp\\
                         ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM))\\
                LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON\\
                          mcpp.Campaign_Id = mcdp.Campaign_Id AND\\
                          mcp.Channel_Id = mcdp.Channel_Id AND\\
                          mpp.Partner_Id = mcdp.Partner_Id AND\\
                          msp.Strategy_Id = mcdp.Strategy_Id AND\\
                          mcdp.Brand_Id = df.GENERIC_BRAND AND mcdp.Source_System_NM = df.GENERIC_SOURCE\\
                ) s1\\
              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_tracker_week mtwp ON\\
                        s1.WEEK_START_DT = mtwp.WEEK_START_DT\\
                        AND s1.DMA_CD = mtwp.DMA_CD\\
                        AND s1.CAMPAIGN_DETAIL_ID = mtwp.CAMPAIGN_DETAIL_ID\\
                        AND s1.BRAND_ID = mtwp.Brand_Id\\
                        AND s1.Source_System_NM = mtwp.Source_System_NM\\
              WHERE mtwp.WEEK_START_DT IS NULL AND mtwp.DMA_CD IS NULL AND mtwp.CAMPAIGN_DETAIL_ID IS NULL\\
              ) s2\\
        ) src\\
        ON src.WEEK_START_DT = tgt.WEEK_START_DT AND\\
           src.DMA_CD = tgt.DMA_CD AND\\
           src.CAMPAIGN_DETAIL_ID = tgt.CAMPAIGN_DETAIL_ID AND\\
           src.BRAND_ID = tgt.BRAND_ID AND\\
           src.SOURCE_SYSTEM_NM = tgt.SOURCE_SYSTEM_NM\\
        WHEN NOT MATCHED\\
          THEN INSERT (WEEK_START_DT,DMA_CD,CAMPAIGN_DETAIL_ID,IMPRESSIONS_CNT,RATINGS_CNT,CLICKS_CNT,GRP_CNT,TRP_CNT,LOAD_TYP,IMPRESSIONS_TYP,BRAND_ID,SOURCE_SYSTEM_NM,LOAD_ID,LOAD_DTTM,UPDATE_ID,UPDATE_DTTM)\\
            VALUES (src.WEEK_START_DT,src.DMA_CD,src.CAMPAIGN_DETAIL_ID,src.IMPRESSIONS_CNT,src.RATINGS_CNT,src.CLICKS_CNT,src.GRP_CNT,src.TRP_CNT,src.LOAD_TYP,src.IMPRESSIONS_TYP,src.BRAND_ID,src.SOURCE_SYSTEM_NM,src.LOAD_ID,src.LOAD_DTTM,src.UPDATE_ID,src.UPDATE_DTTM)\\
    ";
                  
    var merge_media_spend="\\
        MERGE INTO IDS_"+ENV+"."+SCHEMA+".media_spend_week tgt\\
        USING (\\
              SELECT\\
                week_start_dt\\
                ,DMA_CD\\
                ,CAMPAIGN_DETAIL_ID\\
                ,SPEND_TYPE_ID\\
                ,SPEND_AMT\\
                ,CURRENCY_CD\\
                ,LOAD_TYP\\
                ,BRAND_ID\\
                ,SOURCE_SYSTEM_NM\\
                ,LOAD_ID\\
                ,LOAD_DTTM\\
                ,UPDATE_ID\\
                ,UPDATE_DTTM\\
              FROM\\
                (\\
                SELECT s1.* FROM\\
                  (\\
                    SELECT\\
                    df.week_start_dt\\
                    ,df.DMA_CD\\
                    ,mcdp.Campaign_Detail_Id\\
                    ,df.spend_type_id\\
                    ,df.spend_amt as spend_amt\\
                    ,currency_cd\\
                    ,df.LOAD_TYPE AS LOAD_TYP\\
                    ,df.BRAND_ID AS BRAND_ID\\
                    ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
                    ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
                    ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
                    ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
                    ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
                  FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
                  LEFT JOIN (\\
                             SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt\\
                             WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined''\\
                             UNION SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct where mct.CAMPAIGN_NM = ''Undefined'' and mct.BRAND_ID = ''irb'' and mct.SOURCE_SYSTEM_NM = ''internal''\\
                            ) mcpp\\
                            ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM))\\
                  LEFT JOIN\\
                            (\\
                              SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id\\
                              WHERE l3.Channel_Type_Id = ''3''\\
                            ) mcp \\
                           ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2))\\
                  LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp\\
                           ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID AND mpp.Source_System_NM = df.SOURCE_SYSTEM_NM\\
                  LEFT JOIN (\\
                            SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt\\
                            WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined''\\
                            UNION \\
                            SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                            WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal''\\
                            ) msp\\
                           ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM))\\
                  LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON\\
                            mcpp.Campaign_Id = mcdp.Campaign_Id \\
                            AND mcp.Channel_Id = mcdp.Channel_Id \\
                            AND mpp.Partner_Id = mcdp.Partner_Id \\
                            AND msp.Strategy_Id = mcdp.Strategy_Id \\
                            AND mcdp.Brand_Id = df.GENERIC_BRAND \\
                            AND mcdp.Source_System_NM = df.GENERIC_SOURCE\\
                  ) s1\\
                 LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_spend_week mtwp ON\\
                          s1.WEEK_START_DT = mtwp.WEEK_START_DT\\
                          AND s1.DMA_CD = mtwp.DMA_CD\\
                          AND s1.CAMPAIGN_DETAIL_ID = mtwp.CAMPAIGN_DETAIL_ID\\
                          AND s1.BRAND_ID = mtwp.Brand_Id\\
                          AND s1.Source_System_NM = mtwp.Source_System_NM\\
                WHERE mtwp.WEEK_START_DT IS NULL AND mtwp.DMA_CD IS NULL AND mtwp.CAMPAIGN_DETAIL_ID IS NULL\\
                ) s2\\
        ) src\\
        ON src.WEEK_START_DT = tgt.WEEK_START_DT AND\\
           src.DMA_CD = tgt.DMA_CD AND\\
           src.CAMPAIGN_DETAIL_ID = tgt.CAMPAIGN_DETAIL_ID AND\\
           src.BRAND_ID = tgt.BRAND_ID AND\\
           src.SOURCE_SYSTEM_NM = tgt.SOURCE_SYSTEM_NM\\
        WHEN NOT MATCHED\\
          THEN INSERT (WEEK_START_DT,DMA_CD,CAMPAIGN_DETAIL_ID,SPEND_TYPE_ID,SPEND_AMT,CURRENCY_CD,LOAD_TYP,BRAND_ID,SOURCE_SYSTEM_NM,LOAD_ID,LOAD_DTTM,UPDATE_ID,UPDATE_DTTM)\\
            VALUES (src.WEEK_START_DT,src.DMA_CD,src.CAMPAIGN_DETAIL_ID,src.SPEND_TYPE_ID,src.SPEND_AMT,src.CURRENCY_CD,src.LOAD_TYP,src.BRAND_ID,src.SOURCE_SYSTEM_NM,src.LOAD_ID,src.LOAD_DTTM,src.UPDATE_ID,src.UPDATE_DTTM)\\
    ";
    
    try {
        snowflake.execute ({sqlText: merge_media_partner});
        snowflake.execute ({sqlText: merge_media_campaign});
        snowflake.execute ({sqlText: merge_media_strategy});
        snowflake.execute ({sqlText: merge_media_campaign_detail});
        try{snowflake.execute ({sqlText: merge_media_tracker});} catch{ };
        try{snowflake.execute ({sqlText: merge_media_spend});} catch{ };
        return "Succeeded";
        }
    catch (err)  {
        return "Failed: " + err;
        };
        
    ';
CREATE PROCEDURE IF NOT EXISTS PREP_UNIT_TEST("ENV" VARCHAR(16777216), "SCHEMA" VARCHAR(16777216), "BRAND" VARCHAR(16777216), "SOURCE_SYSTEM" VARCHAR(16777216), "LOAD_TYPE" VARCHAR(16777216), "TEMP_TABLE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var media_partner = "SELECT COUNT(*) \\
        FROM \\
        (SELECT DISTINCT \\
            df.CHANNEL_TYPE_ID \\
            ,df.Partner_NM \\
            ,df.Partner_Desc \\
            ,df.LOAD_TYPE AS LOAD_TYP \\
            ,df.BRAND_ID AS BRAND_ID \\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM \\
         FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df \\
         LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_PARTNER mp \\
            ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mp.Partner_NM)) \\
                AND mp.Brand_Id = df.BRAND_ID \\
                AND mp.Source_System_NM = df.SOURCE_SYSTEM_NM \\
         WHERE mp.Partner_ID IS NULL \\
          ) src ";
          
      var media_partner_check_null = "SELECT COUNT(*) \\
        FROM \\
        (SELECT DISTINCT \\
            df.CHANNEL_TYPE_ID \\
            ,df.Partner_NM \\
            ,df.Partner_Desc \\
            ,df.LOAD_TYPE AS LOAD_TYP \\
            ,df.BRAND_ID AS BRAND_ID \\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM \\
         FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df \\
         LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_PARTNER mp \\
            ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mp.Partner_NM)) \\
                AND mp.Brand_Id = df.BRAND_ID \\
                AND mp.Source_System_NM = df.SOURCE_SYSTEM_NM \\
         WHERE ( df.CHANNEL_TYPE_ID IS Null \\
                OR df.PARTNER_NM IS Null \\
                OR df.PARTNER_DESC IS Null \\
                OR df.LOAD_TYPE IS Null \\
                OR df.BRAND_ID IS Null \\
                OR df.SOURCE_SYSTEM_NM IS Null \\
               ) \\
          ) src ";
          
     var media_campaign = "SELECT COUNT(*) \\
          FROM\\
          (SELECT DISTINCT\\
            df.campaign_name\\
            ,df.campaign_desc\\
            ,null Standard_Campaign_Nm\\
            ,null Standard_Campaign_Desc\\
            ,null Standard_Objective_Typ\\
            ,null Media_Typ\\
            ,df.LOAD_TYPE AS LOAD_TYP\\
            ,df.BRAND_ID AS BRAND_ID\\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
          FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_CAMPAIGN mc\\
              ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mc.CAMPAIGN_NM)) AND\\
                 mc.Brand_Id = df.BRAND_ID AND\\
                 mc.Source_System_NM = df.SOURCE_SYSTEM_NM\\
          WHERE mc.CAMPAIGN_ID IS NULL AND LOWER(TRIM(df.CAMPAIGN_NAME)) != ''undefined''\\
          ) src ";
          
     var media_campaign_check_null = "SELECT COUNT(*) \\
          FROM\\
          (SELECT DISTINCT\\
            df.campaign_name\\
            ,df.campaign_desc\\
            ,null Standard_Campaign_Nm\\
            ,null Standard_Campaign_Desc\\
            ,null Standard_Objective_Typ\\
            ,null Media_Typ\\
            ,df.LOAD_TYPE AS LOAD_TYP\\
            ,df.BRAND_ID AS BRAND_ID\\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
          FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_CAMPAIGN mc\\
              ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mc.CAMPAIGN_NM)) AND\\
                 mc.Brand_Id = df.BRAND_ID AND\\
                 mc.Source_System_NM = df.SOURCE_SYSTEM_NM\\
          WHERE (df.CAMPAIGN_NAME IS Null \\
                OR df.CAMPAIGN_DESC IS Null \\
                OR df.LOAD_TYPE IS Null \\
                OR df.BRAND_ID IS Null \\
                OR df.SOURCE_SYSTEM_NM IS Null \\
                )\\
          ) src "; 
          
     var media_strategy = "SELECT COUNT(*) FROM \\
           (SELECT DISTINCT \\
            df.strategy_type_id \\
            ,df.strategy_nm \\
            ,df.LOAD_TYPE AS LOAD_TYP \\
            ,df.BRAND_ID AS BRAND_ID \\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM \\
            FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df \\
            LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_STRATEGY ms \\
               ON LOWER(TRIM(df.strategy_nm)) = LOWER(TRIM(ms.strategy_nm)) AND \\
                  ms.Brand_Id = df.BRAND_ID AND \\
                  ms.Source_System_NM = df.SOURCE_SYSTEM_NM \\
          WHERE ms.STRATEGY_ID IS NULL AND LOWER(TRIM(df.strategy_nm)) != ''undefined'' \\
        )src ";
        
     var media_strategy_check_null = "SELECT COUNT(*) FROM \\
           (SELECT DISTINCT \\
            df.strategy_type_id \\
            ,df.strategy_nm \\
            ,df.LOAD_TYPE AS LOAD_TYP \\
            ,df.BRAND_ID AS BRAND_ID \\
            ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM \\
            ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID \\
            ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM \\
            FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df \\
            LEFT JOIN IDS_"+ENV+"."+SCHEMA+".MEDIA_STRATEGY ms \\
               ON LOWER(TRIM(df.strategy_nm)) = LOWER(TRIM(ms.strategy_nm)) AND \\
                  ms.Brand_Id = df.BRAND_ID AND \\
                  ms.Source_System_NM = df.SOURCE_SYSTEM_NM \\
          WHERE (df.strategy_type_id IS NULL \\
                 OR df.strategy_nm IS NULL \\
                 OR df.LOAD_TYPE IS Null \\
                 OR df.BRAND_ID IS Null \\
                 OR df.SOURCE_SYSTEM_NM IS Null \\
                ) \\
        )src ";
          
     var media_campaign_detail = "SELECT COUNT(*) FROM \\
           ( \\
            SELECT\\
              Partner_Id\\
              ,Strategy_Id\\
              ,Channel_Id\\
              ,Campaign_Id\\
              ,LOAD_TYP\\
              ,BRAND_ID\\
              ,SOURCE_SYSTEM_NM\\
              ,LOAD_ID\\
              ,LOAD_DTTM\\
              ,UPDATE_ID\\
              ,UPDATE_DTTM\\
            FROM\\
            (\\
              SELECT DISTINCT\\
                lower(trim(df.SUBCHANNEL2))\\
                ,lower(trim(mcp.SubChannel2))\\
                ,mcpp.Campaign_Id\\
                ,mcp.Channel_Id\\
                ,mpp.Partner_Id\\
                ,msp.Strategy_Id\\
                ,df.LOAD_TYPE AS LOAD_TYP\\
                ,''irb'' AS BRAND_ID\\
                ,''internal'' AS SOURCE_SYSTEM_NM\\
                ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
                ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
                ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
                ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
              FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
              LEFT JOIN (\\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt\\
                         WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined''\\
                         UNION \\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct \\
                         WHERE mct.CAMPAIGN_NM = ''Undefined'' AND mct.BRAND_ID = ''irb'' AND mct.SOURCE_SYSTEM_NM = ''internal''\\
                        ) mcpp\\
                        ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM))\\
              LEFT JOIN (\\
                          SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id\\
                          WHERE l3.Channel_Type_Id = ''3''\\
                        ) mcp \\
                        ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2))\\
              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp \\
                        ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID AND mpp.Source_System_NM = df.SOURCE_SYSTEM_NM\\
              LEFT JOIN (\\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt\\
                         WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined''\\
                         UNION \\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                         WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal''\\
                        ) msp\\
                        ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM))\\
              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON\\
                        mcpp.Campaign_Id = mcdp.Campaign_Id AND\\
                        mcp.Channel_Id = mcdp.Channel_Id AND\\
                        mpp.Partner_Id = mcdp.Partner_Id AND\\
                        msp.Strategy_Id = mcdp.Strategy_Id AND\\
                        mcdp.Brand_Id = df.GENERIC_BRAND AND mcdp.Source_System_NM = df.GENERIC_SOURCE\\
              WHERE mcdp.Campaign_Id IS NULL AND mcdp.Channel_Id IS NULL AND mcdp.Partner_Id IS NULL AND mcdp.Strategy_Id IS NULL\\
            ) s1\\
         ) src";
         
         
     var media_campaign_detail_check_null = "SELECT COUNT(*) FROM \\
           ( \\
            SELECT \\
              Partner_Id\\
              ,Strategy_Id\\
              ,Channel_Id\\
              ,Campaign_Id\\
              ,LOAD_TYP\\
              ,BRAND_ID\\
              ,SOURCE_SYSTEM_NM\\
              ,LOAD_ID\\
              ,LOAD_DTTM\\
              ,UPDATE_ID\\
              ,UPDATE_DTTM\\
            FROM\\
            (\\
              SELECT DISTINCT\\
                lower(trim(df.SUBCHANNEL2))\\
                ,lower(trim(mcp.SubChannel2))\\
                ,mcpp.Campaign_Id\\
                ,mcp.Channel_Id\\
                ,mpp.Partner_Id\\
                ,msp.Strategy_Id\\
                ,df.LOAD_TYPE AS LOAD_TYP\\
                ,''irb'' AS BRAND_ID\\
                ,''internal'' AS SOURCE_SYSTEM_NM\\
                ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
                ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
                ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
                ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
              FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
              LEFT JOIN (\\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt\\
                         WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined''\\
                         UNION \\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct \\
                         WHERE mct.CAMPAIGN_NM = ''Undefined'' AND mct.BRAND_ID = ''irb'' AND mct.SOURCE_SYSTEM_NM = ''internal''\\
                        ) mcpp\\
                        ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM))\\
              LEFT JOIN (\\
                          SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id\\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id\\
                          WHERE l3.Channel_Type_Id = ''3''\\
                        ) mcp \\
                        ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2))\\
              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp \\
                        ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID AND mpp.Source_System_NM = df.SOURCE_SYSTEM_NM\\
              LEFT JOIN (\\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt\\
                         WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined''\\
                         UNION \\
                         SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                         WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal''\\
                        ) msp\\
                        ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM))\\
              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON\\
                        mcpp.Campaign_Id = mcdp.Campaign_Id AND\\
                        mcp.Channel_Id = mcdp.Channel_Id AND\\
                        mpp.Partner_Id = mcdp.Partner_Id AND\\
                        msp.Strategy_Id = mcdp.Strategy_Id AND\\
                        mcdp.Brand_Id = df.GENERIC_BRAND AND mcdp.Source_System_NM = df.GENERIC_SOURCE\\
            ) s1\\
          WHERE (  s1.PARTNER_ID IS Null \\
                  OR s1.STRATEGY_ID IS Null \\
                  OR s1.CHANNEL_ID IS Null \\
                  OR s1.CAMPAIGN_ID IS Null \\
                  OR s1.LOAD_TYP IS Null \\
                  OR s1.BRAND_ID IS Null \\
                  OR s1.SOURCE_SYSTEM_NM IS Null \\
                 ) \\
         ) src";
     
     var media_tracker_week = "SELECT COUNT(*) FROM \\
           ( \\
            SELECT \\
            week_start_dt \\
            ,DMA_CD \\
            ,CAMPAIGN_DETAIL_ID \\
            ,IMPRESSIONS_CNT \\
            ,RATINGS_CNT \\
            ,CLICKS_CNT \\
            ,GRP_CNT \\
            ,TRP_CNT \\
            ,IMPRESSIONS_TYP \\
            ,LOAD_TYP \\
            ,BRAND_ID \\
            ,SOURCE_SYSTEM_NM \\
            ,LOAD_ID \\
            ,LOAD_DTTM \\
            ,UPDATE_ID \\
            ,UPDATE_DTTM \\
           FROM \\
            ( \\
              SELECT s1.* FROM \\
                ( \\
                SELECT \\
                  df.week_start_dt \\
                  ,df.DMA_CD \\
                  ,df.grp as grp_cnt \\
                  ,df.ratings as ratings_cnt \\
                  ,df.clicks clicks_cnt \\
                  ,df.IMPRESSIONS_TYP \\
                  ,mcdp.Campaign_Detail_Id \\
                  ,df.Impressions_cnt Impressions_CNT \\
                  ,df.TRP trp_cnt \\
                  ,df.LOAD_TYPE AS LOAD_TYP \\
                  ,df.BRAND_ID AS BRAND_ID \\
                  ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM \\
                  ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID \\
                  ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM \\
                  ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID \\
                  ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM \\
                FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df \\
                LEFT JOIN ( \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt \\
                           WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined'' \\
                           UNION \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct \\
                           WHERE mct.CAMPAIGN_NM = ''Undefined'' AND mct.BRAND_ID = ''irb'' AND mct.SOURCE_SYSTEM_NM = ''internal'' \\
                          ) mcpp \\
                         ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM)) \\
                LEFT JOIN ( \\
                          SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1 \\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id \\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id \\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id \\
                          WHERE l3.Channel_Type_Id = ''3'' \\
                          ) mcp \\
                          ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2)) \\
                LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp \\
                         ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID and mpp.Source_System_NM = df.SOURCE_SYSTEM_NM \\
                LEFT JOIN ( \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt \\
                           WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined'' \\
                           UNION \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                           WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal'' \\
                          ) msp \\
                         ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM)) \\
                LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON \\
                          mcpp.Campaign_Id = mcdp.Campaign_Id AND \\
                          mcp.Channel_Id = mcdp.Channel_Id AND \\
                          mpp.Partner_Id = mcdp.Partner_Id AND \\
                          msp.Strategy_Id = mcdp.Strategy_Id AND \\
                          mcdp.Brand_Id = df.GENERIC_BRAND AND mcdp.Source_System_NM = df.GENERIC_SOURCE \\
                ) s1 \\
              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_tracker_week mtwp ON \\
                        s1.WEEK_START_DT = mtwp.WEEK_START_DT \\
                        AND s1.DMA_CD = mtwp.DMA_CD \\
                        AND s1.CAMPAIGN_DETAIL_ID = mtwp.CAMPAIGN_DETAIL_ID \\
                        AND s1.BRAND_ID = mtwp.Brand_Id \\
                        AND s1.Source_System_NM = mtwp.Source_System_NM \\
              WHERE mtwp.WEEK_START_DT IS NULL AND mtwp.DMA_CD IS NULL AND mtwp.CAMPAIGN_DETAIL_ID IS NULL \\
              ) s2 \\
        ) src";
     
     var media_tracker_week_check_null = "SELECT COUNT(*) FROM \\
           ( \\
            SELECT \\
            week_start_dt \\
            ,DMA_CD \\
            ,CAMPAIGN_DETAIL_ID \\
            ,IMPRESSIONS_CNT \\
            ,RATINGS_CNT \\
            ,CLICKS_CNT \\
            ,GRP_CNT \\
            ,TRP_CNT \\
            ,IMPRESSIONS_TYP \\
            ,LOAD_TYP \\
            ,BRAND_ID \\
            ,SOURCE_SYSTEM_NM \\
            ,LOAD_ID \\
            ,LOAD_DTTM \\
            ,UPDATE_ID \\
            ,UPDATE_DTTM \\
           FROM \\
            ( \\
              SELECT s1.* FROM \\
                ( \\
                SELECT \\
                  df.week_start_dt \\
                  ,df.DMA_CD \\
                  ,df.grp as grp_cnt \\
                  ,df.ratings as ratings_cnt \\
                  ,df.clicks clicks_cnt \\
                  ,df.IMPRESSIONS_TYP \\
                  ,mcdp.Campaign_Detail_Id \\
                  ,df.Impressions_cnt Impressions_CNT \\
                  ,df.TRP trp_cnt \\
                  ,df.LOAD_TYPE AS LOAD_TYP \\
                  ,df.BRAND_ID AS BRAND_ID \\
                  ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM \\
                  ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID \\
                  ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM \\
                  ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID \\
                  ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM \\
                FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df \\
                LEFT JOIN ( \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt \\
                           WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined'' \\
                           UNION \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct \\
                           WHERE mct.CAMPAIGN_NM = ''Undefined'' AND mct.BRAND_ID = ''irb'' AND mct.SOURCE_SYSTEM_NM = ''internal'' \\
                          ) mcpp \\
                         ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM)) \\
                LEFT JOIN ( \\
                          SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1 \\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id \\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id \\
                          LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id \\
                          WHERE l3.Channel_Type_Id = ''3'' \\
                          ) mcp \\
                          ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2)) \\
                LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp \\
                         ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID and mpp.Source_System_NM = df.SOURCE_SYSTEM_NM \\
                LEFT JOIN ( \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt \\
                           WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined'' \\
                           UNION \\
                           SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                           WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal'' \\
                          ) msp \\
                         ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM)) \\
                LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON \\
                          mcpp.Campaign_Id = mcdp.Campaign_Id AND \\
                          mcp.Channel_Id = mcdp.Channel_Id AND \\
                          mpp.Partner_Id = mcdp.Partner_Id AND \\
                          msp.Strategy_Id = mcdp.Strategy_Id AND \\
                          mcdp.Brand_Id = df.GENERIC_BRAND AND mcdp.Source_System_NM = df.GENERIC_SOURCE \\
                ) s1 \\
              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_tracker_week mtwp ON \\
                        s1.WEEK_START_DT = mtwp.WEEK_START_DT \\
                        AND s1.DMA_CD = mtwp.DMA_CD \\
                        AND s1.CAMPAIGN_DETAIL_ID = mtwp.CAMPAIGN_DETAIL_ID \\
                        AND s1.BRAND_ID = mtwp.Brand_Id \\
                        AND s1.Source_System_NM = mtwp.Source_System_NM \\
              WHERE mtwp.WEEK_START_DT IS NULL AND mtwp.DMA_CD IS NULL AND mtwp.CAMPAIGN_DETAIL_ID IS NULL \\
              ) s2 \\
              WHERE (  s2.WEEK_START_DT IS Null \\
                      OR s2.DMA_CD IS Null \\
                      OR s2.CAMPAIGN_DETAIL_ID IS Null \\
                      OR s2.IMPRESSIONS_CNT IS Null \\
                      OR s2.LOAD_TYP IS Null \\
                      OR s2.BRAND_ID IS Null \\
                      OR s2.SOURCE_SYSTEM_NM IS Null \\
                      OR s2.CLICKS_CNT IS Null \\
                      ) \\
        ) src";
        
    var media_spend_week = "SELECT COUNT(*) FROM \\
           ( \\
            SELECT\\
                week_start_dt\\
                ,DMA_CD\\
                ,CAMPAIGN_DETAIL_ID\\
                ,SPEND_TYPE_ID\\
                ,SPEND_AMT\\
                ,CURRENCY_CD\\
                ,LOAD_TYP\\
                ,BRAND_ID\\
                ,SOURCE_SYSTEM_NM\\
                ,LOAD_ID\\
                ,LOAD_DTTM\\
                ,UPDATE_ID\\
                ,UPDATE_DTTM\\
              FROM\\
                (\\
                SELECT s1.* FROM\\
                  (\\
                    SELECT\\
                    df.week_start_dt\\
                    ,df.DMA_CD\\
                    ,mcdp.Campaign_Detail_Id\\
                    ,df.spend_type_id\\
                    ,df.spend_amt as spend_amt\\
                    ,currency_cd\\
                    ,df.LOAD_TYPE AS LOAD_TYP\\
                    ,df.BRAND_ID AS BRAND_ID\\
                    ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
                    ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
                    ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
                    ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
                    ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
                  FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
                  LEFT JOIN (\\
                             SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt\\
                             WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined''\\
                             UNION SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct where mct.CAMPAIGN_NM = ''Undefined'' and mct.BRAND_ID = ''irb'' and mct.SOURCE_SYSTEM_NM = ''internal''\\
                            ) mcpp\\
                            ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM))\\
                  LEFT JOIN\\
                            (\\
                              SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id\\
                              WHERE l3.Channel_Type_Id = ''3''\\
                            ) mcp \\
                           ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2))\\
                  LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp\\
                           ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID AND mpp.Source_System_NM = df.SOURCE_SYSTEM_NM\\
                  LEFT JOIN (\\
                            SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt\\
                            WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined''\\
                            UNION \\
                            SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                            WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal''\\
                            ) msp\\
                           ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM))\\
                  LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON\\
                            mcpp.Campaign_Id = mcdp.Campaign_Id \\
                            AND mcp.Channel_Id = mcdp.Channel_Id \\
                            AND mpp.Partner_Id = mcdp.Partner_Id \\
                            AND msp.Strategy_Id = mcdp.Strategy_Id \\
                            AND mcdp.Brand_Id = df.GENERIC_BRAND \\
                            AND mcdp.Source_System_NM = df.GENERIC_SOURCE\\
                  ) s1\\
                 LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_spend_week mtwp ON\\
                          s1.WEEK_START_DT = mtwp.WEEK_START_DT\\
                          AND s1.DMA_CD = mtwp.DMA_CD\\
                          AND s1.CAMPAIGN_DETAIL_ID = mtwp.CAMPAIGN_DETAIL_ID\\
                          AND s1.BRAND_ID = mtwp.Brand_Id\\
                          AND s1.Source_System_NM = mtwp.Source_System_NM\\
                WHERE mtwp.WEEK_START_DT IS NULL AND mtwp.DMA_CD IS NULL AND mtwp.CAMPAIGN_DETAIL_ID IS NULL\\
                ) s2\\
        ) src ";
        
    var media_spend_week_check_null = "SELECT COUNT(*) FROM \\
           ( \\
            SELECT\\
                week_start_dt\\
                ,DMA_CD\\
                ,CAMPAIGN_DETAIL_ID\\
                ,SPEND_TYPE_ID\\
                ,SPEND_AMT\\
                ,CURRENCY_CD\\
                ,LOAD_TYP\\
                ,BRAND_ID\\
                ,SOURCE_SYSTEM_NM\\
                ,LOAD_ID\\
                ,LOAD_DTTM\\
                ,UPDATE_ID\\
                ,UPDATE_DTTM\\
              FROM\\
                (\\
                SELECT s1.* FROM\\
                  (\\
                    SELECT\\
                    df.week_start_dt\\
                    ,df.DMA_CD\\
                    ,mcdp.Campaign_Detail_Id\\
                    ,df.spend_type_id\\
                    ,df.spend_amt as spend_amt\\
                    ,currency_cd\\
                    ,df.LOAD_TYPE AS LOAD_TYP\\
                    ,df.BRAND_ID AS BRAND_ID\\
                    ,df.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM\\
                    ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS LOAD_ID\\
                    ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS LOAD_DTTM\\
                    ,TO_NUMBER(TO_CHAR(CURRENT_DATE, ''YYYYMMDD'')) AS UPDATE_ID\\
                    ,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) AS UPDATE_DTTM\\
                  FROM IDS_"+ENV+"."+SCHEMA+"."+TEMP_TABLE+" df\\
                  LEFT JOIN (\\
                             SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mcppt\\
                             WHERE mcppt.Brand_Id = ''"+BRAND+"'' AND mcppt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mcppt.CAMPAIGN_NM != ''Undefined''\\
                             UNION SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_campaign mct where mct.CAMPAIGN_NM = ''Undefined'' and mct.BRAND_ID = ''irb'' and mct.SOURCE_SYSTEM_NM = ''internal''\\
                            ) mcpp\\
                            ON LOWER(TRIM(df.CAMPAIGN_NAME)) = LOWER(TRIM(mcpp.CAMPAIGN_NM))\\
                  LEFT JOIN\\
                            (\\
                              SELECT l1.Channel_NM, l2.Channel_NM SubChannel, l3.Channel_NM SubChannel2, l3.Channel_Id FROM IDS_"+ENV+"."+SCHEMA+".media_channel l1\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l2 on l1.Channel_Id = l2.Parent_Channel_Id\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l3 on l2.Channel_Id = l3.Parent_Channel_Id\\
                              LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_channel l4 on l3.Channel_Id = l4.Parent_Channel_Id\\
                              WHERE l3.Channel_Type_Id = ''3''\\
                            ) mcp \\
                           ON LOWER(TRIM(df.SUBCHANNEL)) = LOWER(TRIM(mcp.SubChannel)) AND LOWER(TRIM(df.SUBCHANNEL2)) = LOWER(TRIM(mcp.SubChannel2))\\
                  LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_partner mpp\\
                           ON LOWER(TRIM(df.Partner_NM)) = LOWER(TRIM(mpp.Partner_NM)) AND mpp.Brand_Id = df.BRAND_ID AND mpp.Source_System_NM = df.SOURCE_SYSTEM_NM\\
                  LEFT JOIN (\\
                            SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mspt\\
                            WHERE mspt.Brand_Id = ''"+BRAND+"'' AND mspt.SOURCE_SYSTEM_NM = ''"+SOURCE_SYSTEM+"'' AND mspt.Strategy_NM != ''Undefined''\\
                            UNION \\
                            SELECT * FROM IDS_"+ENV+"."+SCHEMA+".media_strategy mst \\
                            WHERE mst.Strategy_NM = ''Undefined'' AND mst.BRAND_ID = ''irb'' AND mst.SOURCE_SYSTEM_NM = ''internal''\\
                            ) msp\\
                           ON LOWER(TRIM(df.Strategy_NM)) = LOWER(TRIM(msp.strategy_NM))\\
                  LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_campaign_detail mcdp ON\\
                            mcpp.Campaign_Id = mcdp.Campaign_Id \\
                            AND mcp.Channel_Id = mcdp.Channel_Id \\
                            AND mpp.Partner_Id = mcdp.Partner_Id \\
                            AND msp.Strategy_Id = mcdp.Strategy_Id \\
                            AND mcdp.Brand_Id = df.GENERIC_BRAND \\
                            AND mcdp.Source_System_NM = df.GENERIC_SOURCE\\
                  ) s1\\
                 LEFT JOIN IDS_"+ENV+"."+SCHEMA+".media_spend_week mtwp ON\\
                          s1.WEEK_START_DT = mtwp.WEEK_START_DT\\
                          AND s1.DMA_CD = mtwp.DMA_CD\\
                          AND s1.CAMPAIGN_DETAIL_ID = mtwp.CAMPAIGN_DETAIL_ID\\
                          AND s1.BRAND_ID = mtwp.Brand_Id\\
                          AND s1.Source_System_NM = mtwp.Source_System_NM\\
                WHERE mtwp.WEEK_START_DT IS NULL AND mtwp.DMA_CD IS NULL AND mtwp.CAMPAIGN_DETAIL_ID IS NULL\\
                ) s2\\
                WHERE \\
                (  s2.WEEK_START_DT IS Null \\
                  OR s2.DMA_CD IS Null \\
                  OR s2.CAMPAIGN_DETAIL_ID IS Null \\
                  OR s2.SPEND_AMT IS Null \\
                  OR s2.LOAD_TYP IS Null \\
                  OR s2.BRAND_ID IS Null \\
                  OR s2.SOURCE_SYSTEM_NM IS Null \\
                ) \\
        ) src "; 
        

          
     
          var media_partner_select = snowflake.createStatement( {sqlText: media_partner} );
          var resultSet_media_partner = media_partner_select.execute();
          resultSet_media_partner.next();
          var result_media_partner = "MEDIA_PARTNER = " + resultSet_media_partner.getColumnValue(1);
          
          var media_partner_check_null_select = snowflake.createStatement( {sqlText: media_partner_check_null} );
          var resultSet_media_partner_check_null = media_partner_check_null_select.execute();
          resultSet_media_partner_check_null.next();
          var result_media_partner_check_null = "MEDIA_PARTNER_NULL_COLUMNS = " + resultSet_media_partner_check_null.getColumnValue(1);
          
          var media_campaign_select = snowflake.createStatement( {sqlText: media_campaign} );
          var resultSet_media_campaign = media_campaign_select.execute();
          resultSet_media_campaign.next();
          var result_media_campaign = "MEDIA_CAMPAIGN = " + resultSet_media_campaign.getColumnValue(1);
          
          var media_campaign_check_null_select = snowflake.createStatement( {sqlText: media_campaign_check_null} );
          var resultSet_media_campaign_check_null = media_campaign_check_null_select.execute();
          resultSet_media_campaign_check_null.next();
          var result_media_campaign_check_null = "MEDIA_CAMPAIGN_NULL_COLUMNS = " + resultSet_media_campaign_check_null.getColumnValue(1);
          
          var media_strategy_select = snowflake.createStatement( {sqlText: media_strategy} );
          var resultSet_media_strategy = media_strategy_select.execute();
          resultSet_media_strategy.next();
          var result_media_strategy = "MEDIA_STRATEGY = " + resultSet_media_strategy.getColumnValue(1);
          
          var media_strategy_check_null_select = snowflake.createStatement( {sqlText: media_strategy_check_null} );
          var resultSet_media_strategy_check_null = media_strategy_check_null_select.execute();
          resultSet_media_strategy_check_null.next();
          var result_media_strategy_check_null = "MEDIA_STRATEGY_NULL_COLUMNS = " + resultSet_media_strategy_check_null.getColumnValue(1);
             
          var media_campaign_detail_select = snowflake.createStatement( {sqlText: media_campaign_detail} );
          var resultSet_media_campaign_detail = media_campaign_detail_select.execute();
          resultSet_media_campaign_detail.next();
          var result_media_campaign_detail = "MEDIA_CAMPAIGN_DETAIL = " + resultSet_media_campaign_detail.getColumnValue(1);
          
          var media_campaign_detail_check_null_select = snowflake.createStatement( {sqlText: media_campaign_detail_check_null} );
          var resultSet_media_campaign_detail_check_null = media_campaign_detail_check_null_select.execute();
          resultSet_media_campaign_detail_check_null.next();
          var result_media_campaign_detail_check_null = "MEDIA_CAMPAIGN_DETAIL_NULL_COLUMNS = " + resultSet_media_campaign_detail_check_null.getColumnValue(1);
          
          try {
          var media_tracker_week_select = snowflake.createStatement( {sqlText: media_tracker_week} );
          var resultSet_media_tracker_week = media_tracker_week_select.execute();
          resultSet_media_tracker_week.next();
          var result_media_tracker_week = "MEDIA_TRACKER_WEEK = " + resultSet_media_tracker_week.getColumnValue(1);
          
          var media_tracker_check_null_select = snowflake.createStatement( {sqlText: media_tracker_week_check_null} );
          var resultSet_media_tracker_check_null = media_tracker_check_null_select.execute();
          resultSet_media_tracker_check_null.next();
          var result_media_tracker_check_null = "MEDIA_TRACKER_WEEK_NULL_COLUMNS = " + resultSet_media_tracker_check_null.getColumnValue(1);
          }
          catch (err)  {
            var result_media_tracker_week = "WITHOUT MEDIA_TRACKER_WEEK";
            var result_media_tracker_check_null = "WITHOUT MEDIA_TRACKER_WEEK_NULL_COLUMNS";
            };
          
          try {
            var media_spend_week_select = snowflake.createStatement( {sqlText: media_spend_week} );
            var resultSet_media_spend_week = media_spend_week_select.execute();
            resultSet_media_spend_week.next();
            var result_media_spend_week = "MEDIA_SPEND_WEEK = " + resultSet_media_spend_week.getColumnValue(1);

            var media_spend_check_null_select = snowflake.createStatement( {sqlText: media_spend_week_check_null} );
            var resultSet_media_spend_check_null = media_spend_check_null_select.execute();
            resultSet_media_spend_check_null.next();
            var result_media_spend_check_null = "MEDIA_SPEND_WEEK_NULL_COLUMNS = " + resultSet_media_spend_check_null.getColumnValue(1);
            }
          catch (err)  {
            var result_media_spend_week = "WITHOUT MEDIA_SPEND_WEEK";
            var result_media_spend_check_null = "WITHOUT MEDIA_SPEND_WEEK_NULL_COLUMNS";
            };

          
          final_result = result_media_partner + "\\n"+ 
                         result_media_partner_check_null + "\\n" + 
                         result_media_campaign + "\\n" + 
                         result_media_campaign_check_null + "\\n" + 
                         result_media_strategy + "\\n" + 
                         result_media_strategy_check_null + "\\n" +
                         result_media_campaign_detail + "\\n"+
                         result_media_campaign_detail_check_null + "\\n"+
                         result_media_tracker_week + "\\n" +
                         result_media_tracker_check_null + "\\n" +
                         result_media_spend_week + "\\n" +
                         result_media_spend_check_null;
                         
          return final_result;
   ';
CREATE FUNCTION IF NOT EXISTS REMOVE_SPECIAL_CHAR("REPLACE_COLUMN" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS '
        if (REPLACE_COLUMN !== undefined) {
            var final_result = REPLACE_COLUMN.replace(/[^\\w\\s]/gi, '''').replace(/\\s/g, '''').toLowerCase();
            return final_result;
        }
         
     ';
CREATE FUNCTION IF NOT EXISTS WEEK_DATE_FORMAT("DATE_COLUMN" VARCHAR(16777216))
RETURNS DATE
LANGUAGE SQL
AS '
        SELECT to_date(to_char(dateadd(''day'', (extract(''dayofweek_iso'', date(to_timestamp(DATE_FORMAT(DATE_COLUMN)))) * -1) +1 , date(to_timestamp(DATE_FORMAT(DATE_COLUMN)))), ''yyyyMMdd''), ''yyyyMMdd'')
     ';