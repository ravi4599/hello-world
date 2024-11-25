USE WAREHOUSE {{params.warehouse}};


--SET start_load_dt = '20230501' / NULL;
--SET end_load_dt = '20230907' / NULL;
SET start_load_dt = '{{params.load_start_dt}}';   --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';       --end_load_dt from Airflow Parameters


DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='bww' and FN_SYSTEM_ID=11 ;

--MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
--USING
--(
CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Altametrics_Temp_table AS
SELECT  
brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM 
(
 
 WITH FILTERED_CASH_RDS_RECORDS AS
 (
  SELECT category, amount, storeid, salesdate
   FROM
   (
   SELECT
   REPLACE(category, '"') AS category, REPLACE(amount, '"') AS amount,       -- Replacing here extra quotes from Source data
   REPLACE(storeid, '"') AS storeid, REPLACE(salesdate, '"') AS salesdate, loadid,
   ROW_NUMBER() OVER (PARTITION BY REPLACE(storeid, '"'), REPLACE(salesdate, '"'), REPLACE(category, '"')
   ORDER BY folderdate DESC, filedate DESC, loaddatetime DESC) AS row_num
   FROM "RDS_{{params.source_env}}"."BWW_BV"."BOS_ALTAMETRICS_SALES_BV" t1
   WHERE (t1.loaddatetime BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')
         OR
        REPLACE(t1.salesdate, '"') BETWEEN TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD'))
   ) WHERE row_num = 1
 )
 SELECT
 'bww'                                                        AS brand_id,
 cisc.salesdate                                               AS business_date,
 cisc.storeid                                                 AS rest_id,
 '11'                                                         AS fn_system_id,
 fmsr.fn_measure_id                                           AS fn_measure_id,
 'N/A'                                       	                AS gl_account_code,
 'N/A'                                                        AS gl_cost_ctr,
 SUM(COALESCE(fmsr.value_adjustment_amount,1) * cisc.amount)  AS sale_usd_amount,
 SUM(COALESCE(fmsr.value_adjustment_amount,1) * cisc.amount)  AS sale_amount,
 'USA'  									                                    AS country_code,
 'USD'   										                                  AS currency_code,
 'altametrics'                                                AS source_system_name,
 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                 AS load_id,
 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                          AS load_dttm,
 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                 AS update_id,
 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                          AS update_dttm
 FROM FILTERED_CASH_RDS_RECORDS cisc
 INNER JOIN "IDS_{{params.target_env}}".TXN_BV.FN_SYSTEM_TO_MEASURE_REFERENCE_BV fmsr
 ON fmsr.source_sales_system_measure_text = cisc.category AND fmsr.fn_system_id = '11' AND fmsr.brand_id = 'bww'
  INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and cisc.storeid  = lo.rest_id  AND cisc.salesdate between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
 Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and cisc.storeid  = lc.rest_id  AND cisc.salesdate = lc.TEMP_CLOSE_DATE 
 WHERE cisc.storeid NOT IN ('08888' , '09999') and lc.TEMP_CLOSE_DATE is null
 GROUP BY cisc.salesdate, cisc.storeid, fmsr.fn_measure_id
) am;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  Select * from "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Altametrics_Temp_table
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
	  