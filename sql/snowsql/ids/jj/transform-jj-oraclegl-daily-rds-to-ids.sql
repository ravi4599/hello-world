USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20230301' / NULL;
--SET end_load_dt = '20230929' / NULL;
SET start_load_dt = '{{params.load_start_dt}}';   --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';       --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD'))
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) >10
AND BRAND_ID='jj' and FN_SYSTEM_ID=16 ;

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_GL_Temp_table AS
SELECT brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
       sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM  (
WITH CTE_BUSINESS_DATE AS
  (
   SELECT DISTINCT TRY_TO_DATE(RTRIM(SUBSTR(jel_description, 11, 10)), 'MM/DD/YYYY')  AS calc_gl_date
   FROM "RDS_{{params.source_env}}"."JJE_BV"."ORACLE_GL_BV"  t1
   WHERE ((UPPER(t1.je_source) = 'JJSALES' AND UPPER(t1.batch_name) LIKE '%JJSALES%') OR
         (UPPER(t1.je_source) LIKE '%SPREAD%' AND UPPER(t1.jeh_name) LIKE '%RAT SALES ENTRY%'))
     AND loaddatetime BETWEEN TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')
  UNION
  SELECT CALENDAR_DT as business_date FROM "IDS_{{params.source_env}}"."INT_REF_BV"."DATE_DIM_BV"
  WHERE CALENDAR_DT BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
 )
, FILTERED_RDS_RECORDS AS
 (
    SELECT  batch_name, jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt,
	  CASE WHEN ACCT LIKE '114%' THEN '114X'
        WHEN ACCT = '115214' AND DR_AMT <> 0  THEN '115214-D' 
        WHEN ACCT = '115214' AND CR_AMT <> 0  THEN '115214-C'
        ELSE ACCT END AS ACCT, cc, filedate, folderdate,calc_gl_date business_date,store_id 
    FROM
    (   
      SELECT  batch_name, jeh_name, je_source, jel_description,cur_code, dr_amt, cr_amt, acct, cc, filedate, folderdate,
      TO_DATE( SUBSTRING(filename, POSITION('UDP_', filename, 1)+4,8),'yyyymmdd') AS filedate1,filedate1 AS FolderDate1, 
	    TRY_TO_DATE(RTRIM(SUBSTR(jel_description, 11, 10)), 'MM/DD/YYYY')  AS calc_gl_date,
	    CASE WHEN regexp_instr(JEL_DESCRIPTION,'Store ID:') > 1 THEN SUBSTR(JEL_DESCRIPTION, regexp_instr(JEL_DESCRIPTION,'Store ID:') +10,6)
      ELSE SUBSTRING((RTRIM(t1.loc)), 3, 6) END AS store_id,  
      DENSE_RANK() OVER (PARTITION BY Batch_Name, calc_gl_date, acct, jel_description
      ORDER BY filename DESC, filedate1 DESC, folderdate1 DESC,loaddatetime DESC ) AS dense_rank
	  FROM  "RDS_{{params.source_env}}"."JJE_BV"."ORACLE_GL_BV"  t1
     WHERE  calc_gl_date IS NOT NULL  
      AND ( (UPPER(t1.je_source) = 'JJSALES' AND UPPER(t1.batch_name) LIKE '%JJSALES%') 
      OR    (UPPER(t1.je_source) LIKE '%SPREAD%' AND UPPER(t1.jeh_name) LIKE '%RAT SALES ENTRY%')
      ) AND calc_gl_date IN (SELECT calc_gl_date FROM CTE_BUSINESS_DATE)
    )
	  WHERE DENSE_RANK = 1
 )
, MASTER_TXNS AS
 (
  SELECT
  brand_id, business_date, jel_description, rest_id, fn_system_id, gl_account_code, cr_amt, dr_amt,
  fn_measure_id, gl_cost_ctr, credit_score, debit_Score, country_code, currency_code, source_system_name,
  load_id, load_dttm, update_id, update_dttm
  FROM
	(
    SELECT
	 'jj' AS brand_id, business_date, gl.jel_description,
    SUBSTRING((RTRIM(gl.jel_description)), 34, 4) as rest_id,gl.acct,      
	 mp.fn_system_id AS fn_system_id, gl.acct AS gl_account_code, gl.cr_amt AS cr_amt, gl.dr_amt AS dr_amt,
	 mp.fn_measure_id, gl.cc AS gl_cost_ctr, mp.credit_value_adjustment_amount AS credit_score,
	 mp.debit_value_adjustment_amount AS debit_Score,
   'USA' AS country_code, gl.cur_code AS currency_code,
   'Oracle General Ledger' AS source_system_name,
	 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS load_id,
	 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS load_dttm,
	 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS update_id,
	 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS update_dttm
	 FROM FILTERED_RDS_RECORDS  gl
 	 JOIN (
          SELECT fn_system_id, gl_account_code, credit_value_adjustment_amount, debit_value_adjustment_amount,
          MIN(fn_measure_id) AS fn_measure_id
          FROM IDS_{{params.target_env}}.TXN_BV.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BV
          WHERE  fn_system_id = '16' AND brand_id = 'jj'  AND gl_account_code <> ''
          GROUP BY fn_system_id, gl_account_code, credit_value_adjustment_amount, debit_value_adjustment_amount
	      ) mp  ON mp.gl_account_code = gl.acct 
     )
	
 ) 
  SELECT brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
         SUM((credit_score * cr_amt) + (debit_score * dr_amt)) AS sale_usd_amount,    
         SUM((credit_score * cr_amt) + (debit_score * dr_amt)) AS sale_amount,
         country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
  FROM  MASTER_TXNS
  GROUP BY brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
  country_code, currency_code, source_system_name, load_id,load_dttm, update_id, update_dttm
) gl;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  Select * from  "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_GL_Temp_table
) AS gls

ON fdrm.business_date = gls.business_date
AND fdrm.brand_id = gls.brand_id
AND fdrm.fn_system_id = gls.fn_system_id
AND fdrm.fn_measure_id = gls.fn_measure_id
AND fdrm.gl_account_code = gls.gl_account_code
AND fdrm.gl_cost_ctr = gls.gl_cost_ctr
AND fdrm.rest_id = gls.rest_id
WHEN MATCHED THEN
UPDATE SET 
fdrm.sale_usd_amount = gls.sale_usd_amount, 
fdrm.SALE_AMOUNT = gls.sale_amount, 
fdrm.COUNTRY_CODE = gls.country_code, 
fdrm.CURRENCY_CODE = gls.currency_code, 
fdrm.SOURCE_SYSTEM_NAME = gls.source_system_name, 
fdrm.update_id = gls.update_id, 
fdrm.update_dttm = gls.update_dttm
WHEN NOT MATCHED THEN
INSERT 
(
  brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
)
VALUES 
(
  gls.brand_id, gls.business_date, gls.rest_id, gls.fn_system_id, gls.fn_measure_id, gls.gl_account_code,
  gls.gl_cost_ctr, gls.sale_usd_amount, gls.sale_amount, gls.country_code, gls.currency_code, gls.source_system_name,
  gls.load_id, gls.load_dttm, gls.update_id, gls.update_dttm
);