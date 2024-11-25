USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20220801 /NULL';
--SET end_load_dt = '20220928 / NULL';                      
SET start_load_dt =  '{{params.load_start_dt}}';
SET end_load_dt = '{{params.load_end_dt}}';

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='arbys' and FN_SYSTEM_ID=2 ;

--MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
--USING
--(
CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Arbys_Altametrics_Temp_table AS
SELECT  
brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
sale_amount, sale_count, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM 
(
 WITH CTE_BUSINESS_DATE AS
  (
    SELECT businessdate AS business_date from "RDS_{{params.source_env}}"."ARB_BV"."CASH_ITEM_SALES_AND_COUNTS_BV" 
    WHERE loaddatetime BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION
    SELECT businessdate AS business_date from "RDS_{{params.source_env}}"."ARB_BV"."REPORTING_ITEM_SALES_AND_COUNTS_BV" 
    WHERE loaddatetime BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION
    SELECT REPLACE(businessdate,'-','') AS business_date from "RDS_{{params.source_env}}"."ARB_BV"."CASH_VARIANCE_BV" 
    WHERE loaddatetime BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION
    SELECT REPLACE(businessdate,'-','') AS business_date from "RDS_{{params.source_env}}"."ARB_BV"."PAID_IN_OUT_BV" 
    WHERE loaddatetime BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION
    SELECT REPLACE(businessdate,'-','') AS business_date from "RDS_{{params.source_env}}"."ARB_BV"."DEPOSITS_BV" 
    WHERE loaddatetime BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION
    SELECT REPLACE(calendar_dt,'-','') AS business_date FROM "IDS_{{params.source_env}}"."INT_REF_BV"."DATE_DIM_BV"  
    WHERE calendar_dt BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
  )
    ,FILTERED_CASH_RDS_RECORDS AS   -- This CTE filters data from CASH_ITEM_SALES_AND_COUNTS table
  (
   SELECT 
   storeid, businessdate, description, amount, COUNT 
   FROM 
     (
     SELECT 
     storeid, businessdate, description, amount, COUNT,
     ROW_NUMBER() OVER (PARTITION BY storeid, businessdate ,description
                        ORDER BY folderdate DESC, filedate DESC, loaddatetime DESC) as row_num
     FROM "RDS_{{params.source_env}}"."ARB_BV"."CASH_ITEM_SALES_AND_COUNTS_BV" t1  
     WHERE REPLACE(t1.businessdate, '-', '') IN (SELECT business_date FROM CTE_BUSINESS_DATE) 
    ) WHERE row_num = 1
  ),
  FILTERED_REPORTING_RDS_RECORDS AS  -- This CTE filters data from REPORTING_ITEM_SALES_AND_COUNTS table
  (
   SELECT
   storeid, businessdate, description, amount
   FROM
   (
   SELECT
   storeid, businessdate, description, amount,
   ROW_NUMBER() OVER (PARTITION BY storeid, businessdate, description
                      ORDER BY folderdate DESC, filedate DESC, loaddatetime DESC) AS row_num
   FROM "RDS_{{params.source_env}}"."ARB_BV"."REPORTING_ITEM_SALES_AND_COUNTS_BV" t1
   WHERE REPLACE(t1.businessdate, '-', '') IN (SELECT business_date FROM CTE_BUSINESS_DATE)
   ) WHERE row_num = 1
  ),
  FILTERED_CV_RDS_RECORDS AS   -- This CTE filters data from CASH_VARIANCE table
  (
  SELECT 
  storeid, businessdate, description, amount
  FROM
    (
    SELECT 
    restaurantnumber AS storeid, REPLACE(businessdate,'-','') AS businessdate, description, cashamount AS amount, 
    ROW_NUMBER() OVER (PARTITION BY restaurantnumber, businessdate, description 
                        ORDER BY folderdate DESC, filedate DESC,loaddatetime DESC) AS row_num
    FROM "RDS_{{params.source_env}}"."ARB_BV"."CASH_VARIANCE_BV" t1
    WHERE REPLACE(t1.businessdate, '-', '') IN (SELECT business_date FROM CTE_BUSINESS_DATE)
    ) WHERE row_num = 1
  ),
  FILTERED_PINOUT_RDS_RECORDS AS  -- This CTE filters data from PAID_IN_OUT table
  (
   SELECT 
   storeid, businessdate, description, glaccountid, amount  
   FROM
    (
     SELECT 
     restaurantnumber AS storeid, REPLACE(businessdate,'-','') AS businessdate,
     paidinouttypedescription AS description, glaccountid, amount, 
     ROW_NUMBER() OVER (PARTITION BY restaurantnumber, businessdate, paidinouttypedescription, description, glaccountid 
                         ORDER BY folderdate DESC, filedate DESC, loaddatetime DESC) AS row_num
     FROM "RDS_{{params.source_env}}"."ARB_BV"."PAID_IN_OUT_BV" t1
     WHERE REPLACE(t1.businessdate, '-', '') IN (SELECT business_date FROM CTE_BUSINESS_DATE)
    ) WHERE row_num = 1
  ),
  FILTERED_DEPOSITS_RDS_RECORDS AS   -- This CTE filters data from DEPOSITS table
  (
   SELECT 
   storeid, businessdate, 'Udf Deposits $' AS description, amount  
   FROM
    (
    SELECT 
    restaurantnumber AS storeid, REPLACE(businessdate, '-', '') AS businessdate, bagnumber, validatedamount AS amount,
    ROW_NUMBER() OVER (PARTITION BY restaurantnumber, businessdate, bagnumber 
                       ORDER BY folderdate DESC, filedate DESC, loaddatetime DESC) AS row_num
    FROM "RDS_{{params.source_env}}"."ARB_BV"."DEPOSITS_BV" t1
    WHERE REPLACE(t1.businessdate, '-', '') IN (SELECT business_date FROM CTE_BUSINESS_DATE)
    ) WHERE row_num = 1
  )           
  SELECT
  'arbys'                                                       AS brand_id,
  cisc.businessdate                                             AS business_date,
  cisc.storeid                                                  AS rest_id,
  '2'                                                           AS fn_system_id ,
  fsmr.fn_measure_id                                            AS fn_measure_id,
  COALESCE(cisc.glaccountid,'N/A')                              AS gl_account_code,
  'N/A'                                                         AS gl_cost_ctr, 
  SUM(COALESCE(fsmr.value_adjustment_amount, 1) * cisc.Amount)  AS sale_usd_amount,
  SUM(COALESCE(fsmr.value_adjustment_amount, 1) * cisc.Amount)  AS sale_amount,
  SUM(cisc.COUNT)                                               AS sale_count,
  'USA'                                                         AS country_code,
  'USD'                                                         AS currency_code,
  'altametrics'                                                 AS source_system_name,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                  AS load_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                           AS load_dttm,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                  AS update_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                           AS update_dttm
  FROM 
  (
  SELECT storeid, TO_DATE(businessdate, 'YYYYMMDD') AS businessdate, description, amount, COUNT, 'N/A' AS glaccountid
  FROM FILTERED_CASH_RDS_RECORDS  
  UNION ALL
  SELECT storeid, TO_DATE(businessdate, 'YYYYMMDD') AS businessdate, description, amount,0 AS COUNT, 'N/A' AS glaccountid
  FROM   FILTERED_REPORTING_RDS_RECORDS 
  UNION ALL
  SELECT storeid, TO_DATE(businessdate, 'YYYYMMDD') AS businessdate, description, amount,0 AS COUNT, 'N/A' AS glaccountid
  FROM   FILTERED_CV_RDS_RECORDS 
  UNION ALL
  SELECT storeid, TO_DATE(businessdate, 'YYYYMMDD') AS businessdate, description, amount,0 AS COUNT, glaccountid
  FROM   FILTERED_PINOUT_RDS_RECORDS 
  UNION ALL
  SELECT storeid, TO_DATE(businessdate, 'YYYYMMDD') AS businessdate, description, amount,0 AS COUNT, 'N/A' AS glaccountid
  FROM   FILTERED_DEPOSITS_RDS_RECORDS 
  )cisc
  INNER JOIN "IDS_{{params.target_env}}".TXN_BV.FN_SYSTEM_TO_MEASURE_REFERENCE_BV fsmr 
    ON fsmr.source_sales_system_measure_text = cisc.description AND fsmr.fn_system_id = 2 AND fsmr.brand_id = 'arbys'
  INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'arbys' and cisc.storeid = lo.REST_ID AND cisc.businessdate between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
  --Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'arbys' and cisc.storeid = lc.REST_ID AND cisc.businessdate = lc.TEMP_CLOSE_DATE  
  --Where lc.TEMP_CLOSE_DATE is null
  GROUP BY cisc.businessdate, cisc.storeid, fsmr.fn_measure_id, COALESCE(cisc.glaccountid, 'N/A') 
) am;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  Select * from "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Arbys_Altametrics_Temp_table
) AS ams

ON   
fdrm.business_date = ams.business_date
AND fdrm.brand_id = 'arbys' 
AND fdrm.fn_system_id = '2' 
AND fdrm.fn_measure_id = ams.fn_measure_id 
AND fdrm.gl_account_code = ams.gl_account_code 
AND fdrm.gl_cost_ctr = ams.gl_cost_ctr 
AND fdrm.rest_id = ams.rest_id
WHEN MATCHED THEN 
UPDATE SET 
fdrm.sale_usd_amount = ams.sale_usd_amount,
fdrm.sale_amount = ams.sale_amount,
fdrm.country_code = ams.country_code,
fdrm.currency_code = ams.currency_code,
fdrm.source_system_name = ams.source_system_name,
fdrm.update_id = ams.update_id,
fdrm.update_dttm = ams.update_dttm
WHEN NOT MATCHED THEN
INSERT
(
  brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
)
VALUES
(
  ams.brand_id, ams.business_date,
  ams.rest_id, ams.fn_system_id, ams.fn_measure_id,
  ams.gl_account_code, ams.gl_cost_ctr,
  ams.sale_usd_amount, ams.sale_amount,
  ams.country_code, ams.currency_code,
  ams.source_system_name, ams.load_id,
  ams.load_dttm, ams.update_id, ams.update_dttm
);