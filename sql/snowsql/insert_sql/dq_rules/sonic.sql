-- DQ rules script
-- These rules are inserted in Snowflake DB under table "DQ_VALIDATION_RULE" based on rule type and brand

-- Sonic
-- Missing stores POS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
VALUES
('Missing stores check','List of missing stores for POS','Critical','IDH','POS_DAILY_SALES_MEASURE_SONIC','REST_ID',$$with missing_stores as (
select listagg( a.rest_id,',') as stores 
from IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE a
LEFT OUTER JOIN (
SELECT distinct rest_id FROM IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_SONIC
where business_date=current_date()-1) b 
ON a.rest_id = b.rest_id 
WHERE b.rest_id IS NULL AND a.brand_id = 'sonic' and current_date()-1 between a.calc_open_date and a.calc_closure_date 
)
select concat ('sonic_pos', '|',current_date()-1,'|',stores,'|','List of missing stores') as result_text 
from missing_stores where stores<>''::STRING;$$,'View', 'Business','TRUE','Active','New Rule','sonic','POS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

-- Missing stores BOS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
VALUES
('Missing stores check','List of missing stores for BOS','Critical','IDH','BOS_DAILY_SALES_MEASURE_SONIC','REST_ID',$$with missing_stores as (
select listagg( a.rest_id,',') as stores 
from IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE a
LEFT OUTER JOIN (
SELECT distinct rest_id FROM IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_SONIC
where business_date=current_date()-1) b 
ON a.rest_id = b.rest_id 
WHERE b.rest_id IS NULL AND a.brand_id = 'sonic' and current_date()-1 between a.calc_open_date and a.calc_closure_date 
)
select concat ('sonic_bos', '|',current_date()-1,'|',stores,'|','List of missing stores') as result_text 
from missing_stores where stores<>''::STRING;$$,'View', 'Business','TRUE','Active','New Rule','sonic','BOS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

--No_Value_Check_POS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
VALUES
('No_value_check','check if sum of all measures are $0(by store, by day)','critical','IDH','POS_DAILY_SALES_MEASURE_SONIC','SALE_USD_AMOUNT',$$with no_value as( select business_date, rest_id, 
sum(sale_usd_amount) as total, 'POS' as source, 'NO Value check' as DQ_rule from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_SONIC where substr(business_date,6,10) not in ('12-25','01-01') and 
business_date=current_date()-1 group by business_date, rest_id) select concat('POS','|',listagg(rest_id,','),'|',business_date,'|','NO Value check') as result_text from no_value
where total=0 group by source,DQ_rule,business_date ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','sonic','POS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), 
TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

-- No_Value_Check_BOS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
VALUES
('No_value_check','check if sum of all measures are $0(by store, by day)','critical','IDH','BOS_DAILY_SALES_MEASURE_SONIC','SALE_USD_AMOUNT',$$with no_value as( select business_date, rest_id, 
sum(sale_usd_amount) as total,'BOS' as source, 'NO Value check' as DQ_rule from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_SONIC where substr(business_date,6,10) not in ('12-25','01-01') and 
business_date=current_date()-1 group by business_date, rest_id) select concat('BOS','|',listagg(rest_id,','),'|',business_date,'|','NO Value check') as result_text from no_value
where total=0 group by  source,DQ_rule,business_date ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','sonic','BOS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), 
TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

--Measure comparision between POS and BOS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
VALUES
('Measure comparision check','Comparision of measures (Gross Food Sales,Discounts,Employee Tips,GC Redeemed,GC Sold,Paid Outs,Sales Tax) between POS vs BOS ','Critical','IDH','BOS_DAILY_SALES_MEASURE_SONIC','SALE_USD_AMOUNT',
$$with bos_month as
(select * from IDH_{{params.env}}.D_FINANCE.bos_daily_sales_measure_sonic where month(business_date)=month(current_date()) and  year(business_date)=year(current_date())),
pos_month as
(select * from IDH_{{params.env}}.D_FINANCE.pos_daily_sales_measure_sonic where month(business_date)=month(current_date()) and  year(business_date)=year(current_date())),
comparison as(
select a.business_date as bos_date, a.rest_id as bos_store, a.measure_name as bos_measure,b.business_date as pos_date, b.rest_id as pos_store, b.measure_name as pos_measure , a.sale_usd_amount as bos, b.sale_usd_amount as pos from bos_month a
left outer join pos_month b on a.rest_id=b.rest_id and a.measure_name=b.measure_name and a.business_date=b.business_date
where lower(a.measure_name) in ('gross food sales','gc redeemed','gc sold','sales tax','paid outs','discounts','employee tips') 
)
select listagg(concat(bos_store,'|',bos_date,'|',bos_measure ),',') as result_text from comparison where bos<>pos ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','sonic','POS & BOS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());
