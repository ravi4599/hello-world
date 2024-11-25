-- DQ rules script
-- These rules are inserted in Snowflake DB under table "DQ_VALIDATION_RULE" based on rule type and brand
-- BWW

Delete from IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE where rule_desc='Daily Total sum of measure = 0' and table_name='BOS_DAILY_SALES_MEASURE_BWW';--627,2606
Delete from IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE where rule_desc='Daily Total sum of measure = 0' and table_name='POS_DAILY_SALES_MEASURE_BWW';
Delete from IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE where rule_type='Daily Net Sales' and rule_desc='3 Consecutive days of Missing Sales' and table_name='BOS_DAILY_SALES_MEASURE_BWW';
Delete from IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE where rule_type='Daily Net Sales' and rule_desc='3 Consecutive days of Missing Sales' and table_name='POS_DAILY_SALES_MEASURE_BWW';

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('No Value Check','Daily Total sum of measure = 0','critical','IDH','BOS_DAILY_SALES_MEASURE_BWW','SALE_USD_AMOUNT',$$
with no_value as( select business_date, rest_id,  sum(sale_usd_amount) as total, 'BOS' as source, 'NO Value check' as DQ_rule from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_BWW
where  business_date=current_date()-1 group by business_date, rest_id) select concat('BOS','|',business_date,'|',listagg(rest_id,',')) as result_text from no_value where total=0 
group by source,DQ_rule,business_date ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','BOS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), 
TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('No Value Check','Daily Total sum of measure = 0','critical','IDH','POS_DAILY_SALES_MEASURE_BWW','SALE_USD_AMOUNT',$$
with no_value as( select business_date, rest_id,  sum(sale_usd_amount) as total, 'POS' as source, 'NO Value check' as DQ_rule from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_BWW
where  business_date=current_date()-1 group by business_date, rest_id) select concat('POS','|',business_date,'|',listagg(rest_id,',')) as result_text from no_value where total=0
group by source,DQ_rule,business_date ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','POS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), 
TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

-- 3 Consecutive days of Missing Sales

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('Daily Net Sales','3 Consecutive days of Missing Sales','critical','IDH','BOS_DAILY_SALES_MEASURE_BWW','SALE_USD_AMOUNT',$$
with active_stores as
(
select lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE,listagg( lc.TEMP_CLOSE_DATE,',') as TEMP_CLOSE_DATE 
from  IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE  lo
left join IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lo.brand_id = lc.brand_id  and  lo.rest_id = lc.rest_id
group by lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE
),
bos_sales as (
Select REST_ID, SUM(SALE_USD_AMOUNT) AS SALE_USD_AMOUNT from "IDH_{{params.env}}"."D_FINANCE"."BOS_DAILY_SALES_MEASURE_BWW" 
where brand_id='bww' and MEASURE_CATEGORY_NAME = 'Sales' 
and BUSINESS_DATE between CURRENT_DATE -3 AND CURRENT_DATE -1 group by rest_id
), final as(
SELECT  b.rest_id, 'BOS' as bos, current_date-1 as business_date from active_stores b
left outer join bos_sales A on a.rest_id=b.rest_id where SALE_USD_AMOUNT<1 and b.BRAND_ID='bww'
and current_date()- 3 between b.calc_open_date and b.calc_closure_date and b.temp_close_date not like concat('%',to_varchar(current_date() - 3),'%'))
select concat(BOS,'|',business_date,'|',LISTAGG(REST_ID, ',')) as result_text from final group by bos, business_date::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','BOS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('Daily Net Sales','3 Consecutive days of Missing Sales','critical','IDH','POS_DAILY_SALES_MEASURE_BWW','SALE_USD_AMOUNT',$$
with active_stores as
(
select lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE,listagg( lc.TEMP_CLOSE_DATE,',') as TEMP_CLOSE_DATE 
from  IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE  lo
left join IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lo.brand_id = lc.brand_id  and  lo.rest_id = lc.rest_id
group by lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE
),
pos_sales as (
Select REST_ID, SUM(SALE_USD_AMOUNT) AS SALE_USD_AMOUNT from "IDH_{{params.env}}"."D_FINANCE"."POS_DAILY_SALES_MEASURE_BWW" 
where brand_id='bww' and MEASURE_CATEGORY_NAME = 'Sales' 
and BUSINESS_DATE between CURRENT_DATE -3 AND CURRENT_DATE -1 group by rest_id
), final as(
SELECT  b.rest_id, 'POS' as pos, current_date-1 as business_date from active_stores b
left outer join pos_sales A on a.rest_id=b.rest_id where SALE_USD_AMOUNT<1 and b.BRAND_ID='bww'
and current_date()- 3 between b.calc_open_date and b.calc_closure_date and b.temp_close_date not like concat('%',to_varchar(current_date() - 3),'%'))
select concat(POS,'|',business_date,'|',LISTAGG(REST_ID, ',')) as result_text from final group by pos, business_date::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','POS',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate());