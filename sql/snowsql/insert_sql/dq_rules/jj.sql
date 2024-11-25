-- DQ rules script
-- These rules are inserted in Snowflake DB under table "DQ_VALIDATION_RULE" based on rule type and brand

-- jj
-- sum of all measures are $0

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
VALUES
('No_value_check','check if sum of all measures are $0(by store, by day)','Critical','IDH','BOS_DAILY_SALES_MEASURE_JJ','SALE_USD_AMOUNT',$$with no_value as( select business_date, rest_id, 
sum(sale_usd_amount) as total, 'BOS' as source, 'NO Value check' as DQ_rule from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_JJ where business_date=current_date()-1 group by business_date, rest_id) 
select concat('BOS','|',business_date,'|',listagg(rest_id,',')) as result_text from no_value
where total=0 group by source,DQ_rule,business_date::STRING;$$,'View', 'Business','TRUE','Active','New Rule','jj','BOS ',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

-- Gross Food Sales <= $250 per day per store

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
VALUES
('Gross Food Sales <= $250','Gross Food Sales <= $250 per day per store','Critical','IDH','BOS_DAILY_SALES_MEASURE_JJ','SALE_USD_AMOUNT',$$with bos_daily as
(select business_date,rest_id, sale_usd_amount as total, MEASURE_NAME from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_JJ where business_date=current_date() -1 and measure_name='Gross Food Sales')
select concat('BOS','|',business_date,'|',listagg(rest_id,',')) as result_text from bos_daily where total <=250 group by business_date::STRING;$$,'View', 'Business','TRUE','Active','New Rule','jj','BOS ',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());