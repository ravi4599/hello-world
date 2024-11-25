-- DQ rules script
-- These rules are inserted in Snowflake DB under table "DQ_VALIDATION_RULE" based on rule type and brand

--Dunkin
--Sum of all measures is $0  POS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('Sum of all measures is $0','Sum of all measures is $0 - POS','critical','IDH','POS_DAILY_SALES_MEASURE_DUNKIN','rest_id',$$
with no_value as
(
select business_date, rest_id, 
sum(sale_usd_amount) as total, 'POS' as source, 'NO Value check' as DQ_rule from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_DUNKIN where 
business_date=current_date()-1 group by business_date, rest_id
) 
select concat('POS','|',listagg(rest_id,','),'|',business_date,'|','NO Value check') as result_text from no_value
where total=0 
group by business_date ::STRING;
$$,'View', 'Business','TRUE','Active','New Rule','dnkn','pos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);

--Sum of all measures is $0  BOS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('Sum of all measures is $0','Sum of all measures is $0 - BOS','critical','IDH','BOS_DAILY_SALES_MEASURE_DUNKIN','rest_id',$$
with no_value as
(
select business_date, rest_id, 
sum(sale_usd_amount) as total, 'BOS' as source, 'NO Value check' as DQ_rule from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_DUNKIN where 
business_date=current_date()-1 group by business_date, rest_id
) 
select concat('BOS','|',listagg(rest_id,','),'|',business_date,'|','NO Value check') as result_text from no_value
where total=0 
group by business_date::STRING;
$$,'View', 'Business','TRUE','Active','New Rule','dnkn','bos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);

--POS / BOS mismatch POS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('POS/BOS mismatch','POS / BOS mismatch gross sales,gc redeemed,gc sold,sales tax,paid outs,discounts,container/bag deposit,other sales,donations','critical','IDH','POS_DAILY_SALES_MEASURE_DUNKIN','rest_id',$$with bos as
(
select a.business_date , a.rest_id , a.measure_name , a.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_DUNKIN  a where business_date=current_date()-1
),
pos as
(
select b.business_date , b.rest_id , b.measure_name , b.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_DUNKIN b where business_date=current_date()-1 
),
mismatch as(
select a.business_date as bos_date, a.rest_id as bos_store, a.measure_name as bos_measure,b.business_date as pos_date, b.rest_id as pos_store, b.measure_name as pos_measure , a.sale_usd_amount as bos, b.sale_usd_amount as pos from bos a
left outer join pos b on a.rest_id=b.rest_id and a.measure_name=b.measure_name and a.business_date=b.business_date
where lower(a.measure_name) in ('gross sales','gc redeemed','gc sold','sales tax','paid outs','discounts','container/bag deposit','other sales','donations') 
)
select 
listagg(concat(bos_store,'|',bos_date,'|',bos_measure )) 
as result_text from mismatch where bos<>pos and bos_store<>'' HAVING COUNT(*) > 0 ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','dnkn','pos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);

--POS / BOS mismatch BOS
INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('POS/BOS mismatch','POS / BOS mismatch gross sales,gc redeemed,gc sold,sales tax,paid outs,discounts,container/bag deposit,other sales,donations','critical','IDH','BOS_DAILY_SALES_MEASURE_DUNKIN','rest_id',$$with bos as
(
select a.business_date , a.rest_id , a.measure_name , a.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_DUNKIN  a where business_date=current_date()-1
),
pos as
(
select b.business_date , b.rest_id , b.measure_name , b.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_DUNKIN b where business_date=current_date()-1 
),
mismatch as(
select a.business_date as bos_date, a.rest_id as bos_store, a.measure_name as bos_measure,b.business_date as pos_date, b.rest_id as pos_store, b.measure_name as pos_measure , a.sale_usd_amount as bos, b.sale_usd_amount as pos from bos a
left outer join pos b on a.rest_id=b.rest_id and a.measure_name=b.measure_name and a.business_date=b.business_date
where lower(a.measure_name) in ('gross sales','gc redeemed','gc sold','sales tax','paid outs','discounts','container/bag deposit','other sales','donations') 
)
select 
listagg(concat(bos_store,'|',bos_date,'|',bos_measure )) 
as result_text from mismatch where bos<>pos and bos_store<>'' HAVING COUNT(*) > 0 ::STRING;$$,'View', 'Business','TRUE','Active','New Rule','dnkn','bos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);

--POS/BOS match but Gross Sales <= $250 per day per store POS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('POS/BOS match but gross sales','POS/BOS match but gross sales <= $250 per day per store','critical','IDH','POS_DAILY_SALES_MEASURE_DUNKIN','gross sales',$$with bos as
(
select a.business_date , a.rest_id , a.measure_name , a.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_DUNKIN  a where business_date=current_date()-1
),
pos as
(
select b.business_date , b.rest_id , b.measure_name , b.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_DUNKIN b where business_date=current_date()-1
),
match as
(
select a.business_date as bos_date, a.rest_id as bos_store, a.measure_name as bos_measure,b.business_date as pos_date, b.rest_id as pos_store, b.measure_name as pos_measure , a.sale_usd_amount as bos, b.sale_usd_amount as pos from bos a
left outer join pos b on a.rest_id=b.rest_id and a.measure_name=b.measure_name and a.business_date=b.business_date
where lower(a.measure_name) in ('gross sales') and (bos = pos) and (bos <= 250 or pos <= 250)
)
select listagg(concat(pos_store,'|',pos_date,'|',bos_measure ),',')  as result_text from match where bos=pos and bos_store<>''::STRING;$$,'View', 'Business','TRUE','Active','New Rule','dnkn','pos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);

--POS/BOS match but Gross Sales <= $250 per day per store BOS

INSERT INTO IDS_{{params.env}}.INT_REF.DQ_VALIDATION_RULE
(RULE_TYPE, RULE_DESC, RULE_SEVERITY_TYPE, TARGET_DATA_ZONE_CODE, TABLE_NAME, COLUMN_NAME, RULE_SQL_TEXT, RULE_LEVEL_TYPE, RULE_OWNER_TYPE, RULE_ACTIVE_IND , RULE_STATUS_TYPE , RULE_STATUS_DESC , BRAND_ID , SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM)
 
VALUES
('POS/BOS match but gross sales','POS/BOS match but gross sales <= $250 per day per store','critical','IDH','BOS_DAILY_SALES_MEASURE_DUNKIN','gross sales',$$with bos as
(
select a.business_date , a.rest_id , a.measure_name , a.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_DUNKIN  a where business_date=current_date()-1
),
pos as
(
select b.business_date , b.rest_id , b.measure_name , b.sale_usd_amount from IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_DUNKIN b where business_date=current_date()-1 
),
match as
(
select a.business_date as bos_date, a.rest_id as bos_store, a.measure_name as bos_measure,b.business_date as pos_date, b.rest_id as pos_store, b.measure_name as pos_measure , a.sale_usd_amount as bos, b.sale_usd_amount as pos from bos a
left outer join pos b on a.rest_id=b.rest_id and a.measure_name=b.measure_name and a.business_date=b.business_date
where lower(a.measure_name) in ('gross sales') and (bos = pos) and (bos <= 250 or pos <= 250)
)
select listagg(concat(bos_store,'|',bos_date,'|',bos_measure ),',')  as result_text from match where bos=pos and bos_store<>''::STRING;$$,'View', 'Business','TRUE','Active','New Rule','dnkn','bos',TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate(), TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')), sysdate()
);
