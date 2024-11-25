USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20230501';
--SET end_load_dt = '20230907';

SET start_load_dt = '{{params.load_start_dt}}';   --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';       --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD'))
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) >10
AND BRAND_ID='jj' and FN_SYSTEM_ID=15 ;

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_JJ_BOS_JJ_SERVICES_TEMP AS
SELECT  
brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM 
(
 WITH FILTERED_CASH_RDS_RECORDS AS
 (
  SELECT categoryname, amount, storenbr, businessdate
   FROM
   (
   SELECT
     "category id" AS categoryname, amount, SUBSTR("store id", 5, 4) AS storenbr, TO_DATE("sales date",'YYYY/MM/DD') AS businessdate, loadid,
       RANK() OVER (PARTITION BY businessdate, categoryname, storenbr 
     ORDER BY filename desc, folderdate DESC, filedate DESC,loaddatetime DESC) AS rank
   FROM "RDS_{{params.target_env}}"."JJE_BV"."BOS_ALTAMETRICS_SALES_BV" t1
   WHERE (t1.loaddatetime BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')
         OR
          TO_DATE(t1."sales date",'YYYY/MM/DD') BETWEEN TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD'))
   ) WHERE rank = 1
 )
 
 SELECT
 'jj'                                                            AS brand_id,
 cisc.businessdate                                               AS business_date,
 cisc.storenbr                                                   AS rest_id,
 '15'                                                            AS fn_system_id,
 fmsr.fn_measure_id                                              AS fn_measure_id,
 'N/A'                                       	                 AS gl_account_code,
 'N/A'                                                           AS gl_cost_ctr,
 SUM(COALESCE(fmsr.value_adjustment_amount,1) * cisc.amount)     AS sale_usd_amount,
 SUM(COALESCE(fmsr.value_adjustment_amount,1) * cisc.amount)     AS sale_amount,
 'USA'  									                     AS country_code,
 'USD'   										                 AS currency_code,
 'jj services'                                                   AS source_system_name,
 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                    AS load_id,
 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                             AS load_dttm,
 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                    AS update_id,
 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                             AS update_dttm
 FROM FILTERED_CASH_RDS_RECORDS cisc
 INNER JOIN "IDS_{{params.target_env}}".TXN_BV.FN_SYSTEM_TO_MEASURE_REFERENCE_BV fmsr
 ON fmsr.source_sales_system_measure_text = cisc.categoryname AND fmsr.fn_system_id = '15' AND fmsr.brand_id = 'jj'
 INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'jj' and cisc.storenbr = lo.REST_ID AND cisc.businessdate between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
  --Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'jj' and cisc.storenbr = lc.REST_ID AND cisc.businessdate = lc.TEMP_CLOSE_DATE 
  --Where lc.TEMP_CLOSE_DATE is null 
 GROUP BY cisc.businessdate, cisc.storenbr, fmsr.fn_measure_id
) am;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  Select * from "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_JJ_BOS_JJ_SERVICES_TEMP
) AS ams

ON   
fdrm.business_date = ams.business_date
AND fdrm.brand_id = ams.brand_id 
AND fdrm.fn_system_id = ams.fn_system_id
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
  ams.brand_id, ams.business_date, ams.rest_id, ams.fn_system_id, ams.fn_measure_id, ams.gl_account_code, 
  ams.gl_cost_ctr, ams.sale_usd_amount, ams.sale_amount, ams.country_code, ams.currency_code, 
  ams.source_system_name, ams.load_id, ams.load_dttm, ams.update_id, ams.update_dttm
);