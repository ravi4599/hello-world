-- DQ rules script
-- These rules are inserted in Snowflake DB under table "DQ_VALIDATION_RULE" based on rule type and brand


-- BWW
-- List of missing stores POS
INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('List of missing stores','store ID compared to list of store open, by date. POS vs. BOS','critical','IDH','POS_DAILY_SALES_MEASURE_BWW','REST_ID',$$with missing_stores as (
select listagg( a.rest_id,',') as stores from (select lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE,listagg( lc.TEMP_CLOSE_DATE,',') as TEMP_CLOSE_DATE 
from  IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo left join IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lo.brand_id = lc.brand_id  and  lo.rest_id = lc.rest_id
group by lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE) a left outer join (SELECT distinct rest_id FROM IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_BWW where 
business_date = current_date()-1) b on a.rest_id=b.rest_id where b.rest_id is null and a.brand_id='bww' and current_date()- 1 between a.calc_open_date and a.calc_closure_date and 
a.temp_close_date not like concat('%',to_varchar(current_date() - 1),'%')) select concat ('bww_pos', '|',stores,'|',current_date()-1,'|','List of missing stores') as result_text 
from missing_stores where stores<>''::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','pos&bos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);
-- List of missing stores BOS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('List of missing stores','store ID compared to list of store open, by date. POS vs. BOS','critical','IDH','BOS_DAILY_SALES_MEASURE_BWW','REST_ID',$$with missing_stores as (
select listagg( a.rest_id,',') as stores from (select lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE,listagg( lc.TEMP_CLOSE_DATE,',') as TEMP_CLOSE_DATE 
from  IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE  lo left join IDS_{{params.env}}.LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lo.brand_id = lc.brand_id  and  lo.rest_id = lc.rest_id
group by lo.BRAND_ID,lo.REST_ID,lo.CALC_OPEN_DATE,lo.CALC_CLOSURE_DATE) a LEFT OUTER JOIN (SELECT distinct rest_id FROM IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_BWW where 
business_date=current_date()-1 ) b ON a.rest_id = b.rest_id WHERE b.rest_id IS NULL AND a.brand_id='bww' and current_date()-1 between a.calc_open_date and a.calc_closure_date and
a.temp_close_date not like concat('%',to_varchar(current_date() - 1),'%')) select concat ('bww_bos', '|',stores,'|',current_date()-1,'|','List of missing stores') as result_text
from missing_stores where stores<>''::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','pos&bos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);

-- POS to BOS mismatch Net Food Sales

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('POS to BOS mismatch Net Food Sales','Compare of Net Food Sales POS vs. BOS POS vs. BOS','critical','IDH','BOS_DAILY_SALES_MEASURE_BWW','Net Food Sales',$$with bos as
(
 select * from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_BWW
),
pos as
(
 select * from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_BWW 
),
comparison as(
select a.business_date as bos_date, a.rest_id as bos_store, a.measure_name as bos_measure,b.business_date as pos_date, b.rest_id as pos_store, b.measure_name as pos_measure , a.sale_usd_amount as bos, b.sale_usd_amount as pos from bos a
left outer join pos b on a.rest_id=b.rest_id and a.measure_name=b.measure_name and a.business_date=b.business_date
where lower(a.measure_name) in ('food sales') 
)
select listagg(concat(pos_store,'|',pos_date ),',') as result_text from comparison where bos<>pos ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','pos&bos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);

-- POS & BOS & GL Daily Sales

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('Daily Sales','2 days prior to current date, daily check POS vs. BOS and gl','critical','IDH','POS_DAILY_SALES_MEASURE_BWW','Net Sales',$$with bos_daily as (
select rest_id,business_date, sum(sale_usd_amount) as bos_net_sales from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_BWW  where business_date=current_date - 2 and measure_category_name='Sales'
group by rest_id,business_date),
pos_daily as (
select rest_id, business_date,sum(sale_usd_amount) as pos_net_sales from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_BWW   where business_date=current_date - 2 and measure_category_name='Sales'
group by rest_id,business_date),
gl_daily as(
select rest_id, business_date,sum(sale_usd_amount) as gl_daily_net_sales from IDH_{{params.env}}.D_FINANCE.GL_DAILY_SALES_MEASURE_BWW where business_date=current_date - 2 group by rest_id,business_date)
select listagg(concat(a.rest_id,'|',a.business_date ),',') as result_text
from bos_daily A
inner join  pos_daily b on a.rest_id=b.rest_id
inner join  gl_daily c on a.rest_id=c.rest_id
where bos_net_sales=pos_net_sales and gl_daily_net_sales=0::STRING;$$,'View', 'Business','TRUE','Active','New Rule','bww','pos&bos&gl',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);