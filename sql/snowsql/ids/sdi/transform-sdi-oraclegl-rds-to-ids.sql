USE WAREHOUSE {{params.warehouse}};

SET start_load_dt = '{{params.load_start_dt}}'; --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';  --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_WEEKLY_REV_MEASURE"
WHERE (fisc_wk_end_date BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) AND 
DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='sonic' and FN_SYSTEM_ID=9 ;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_WEEKLY_REV_MEASURE" fwrm
USING
(

 WITH WK_ENDING_DATES AS
 (
    SELECT DISTINCT TO_DATE(gl_date, 'MM/DD/YYYY') as wk_end_gl_date
    FROM "RDS_{{params.source_env}}"."SDI_BV"."ORACLE_GL_BV" t1
	  WHERE t1.loaddatetime BETWEEN TO_DATE($start_load_dt, 'YYYYMMDD') AND TO_DATE($end_load_dt, 'YYYYMMDD') 
    
    UNION
    SELECT DISTINCT CASE WHEN TRY_CAST(SUBSTR(jeh_name, 1, 10) AS DATE) IS NOT NULL THEN CAST(SUBSTR(jeh_name, 1, 10) AS DATE)
                WHEN TRY_CAST(SUBSTR(jeh_name, 10, 10) AS DATE) IS NOT NULL THEN CAST(SUBSTR(jeh_name, 10, 10) AS DATE)
                ELSE TO_DATE($start_load_dt,'YYYYMMDD') END wk_end_gl_date
    FROM "RDS_{{params.source_env}}"."SDI_BV"."ORACLE_GL_BV" t1
	  WHERE t1.loaddatetime BETWEEN TO_DATE($start_load_dt, 'YYYYMMDD') AND TO_DATE($end_load_dt, 'YYYYMMDD') 

    UNION

    SELECT CALENDAR_DT as business_date FROM "IDS_{{params.source_env}}"."INT_REF_BV"."DATE_DIM_BV"  
    WHERE CALENDAR_DT BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
 ),

  FILTERED_RDS_RECORDS AS
 (
    SELECT
     batch_name, jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt, acct, cc, filedate, folderdate, 
	 TO_DATE(gl_date, 'MM/DD/YYYY') AS calc_gl_date, loc
    FROM
   (
	SELECT  batch_name, jeh_name, je_source, jel_description,dr_amt, cr_amt, gl_date, cc, acct, filedate, folderdate, loaddatetime, loc, cur_code,
	DENSE_RANK() OVER (PARTITION BY gl_date, cc, loc, dr_amt, acct,batch_name
	                            ORDER BY filedate DESC, folderdate DESC, loaddatetime DESC) AS dense_rank
    FROM "RDS_{{params.source_env}}"."SDI_BV"."ORACLE_GL_BV" t1
	  WHERE UPPER(t1.je_source) = 'SONIC SALES' AND UPPER(t1.batch_name) LIKE '%SLS%'  
    AND TO_DATE(t1.gl_date,'MM/DD/YYYY') in  ( SELECT wk_end_gl_date FROM WK_ENDING_DATES )
   ) WHERE dense_rank = 1
   
 UNION ALL
    SELECT
    batch_name, jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt, acct, cc, filedate, folderdate, calc_gl_date, loc
    FROM
    (
      SELECT
      gl_date, batch_name, jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt, acct, cc, filedate, folderdate, loc,
       CASE WHEN TRY_CAST(SUBSTR(jeh_name, 1, 10) AS DATE) IS NOT NULL THEN CAST(SUBSTR(jeh_name, 1, 10) AS DATE)
                                           WHEN TRY_CAST(SUBSTR(jeh_name, 10, 10) AS DATE) IS NOT NULL THEN CAST(SUBSTR(jeh_name, 10, 10) AS DATE)
                                           ELSE TO_DATE(gl_date, 'MM/DD/YYYY') END calc_gl_date
      , DENSE_RANK() OVER (PARTITION BY calc_gl_date , CC, acct , batch_name ORDER BY filedate DESC, folderdate DESC,loaddatetime DESC) AS dense_rank
      FROM "RDS_{{params.source_env}}"."SDI_BV"."ORACLE_GL_BV" t1
      WHERE UPPER(t1.je_source) LIKE '%SPREAD%' AND UPPER(t1.JEL_DESCRIPTION) NOT LIKE '%INTERCOMPANY%' 
      AND calc_gl_date in ( SELECT wk_end_gl_date FROM WK_ENDING_DATES )
      AND ( TRY_CAST(SUBSTR(jeh_name, 1, 10) AS DATE) IS NOT NULL OR TRY_CAST(SUBSTR(jeh_name, 10, 10) AS DATE) IS NOT NULL )
    )
    WHERE dense_rank =1
   
  ),
 MASTER_TXNS AS
 (
     SELECT brand_id, fisc_wk_end_date, jel_description, rest_id, fn_system_id, mp.gl_account_code, cr_amt, dr_amt
     ,  mp.fn_measure_id, gl_cost_ctr
     , mp.credit_value_adjustment_amount AS credit_score, mp.debit_value_adjustment_amount AS debit_Score
     , country_code, currency_code, source_system_name
     , load_id, load_dttm, update_id, update_dttm FROM
    (
        SELECT
        'sonic' AS brand_id, calc_gl_date AS fisc_wk_end_date, jel_description,
        CASE WHEN loc LIKE '900%' AND UPPER(jel_description) LIKE '%GCARD%' AND acct ='115214' 
        THEN LPAD(SUBSTRING((RTRIM(jel_description)), 10, 4), 5, 0)
        WHEN loc LIKE '1600%' THEN LPAD(SUBSTRING(loc, 5, 4), 5, 0)
        WHEN loc LIKE '900%' AND UPPER(jel_description) LIKE '1600%'
        THEN LPAD(SUBSTRING(jel_description, 5, 4), 5, 0)
        WHEN loc LIKE '900%' AND UPPER(jel_description) NOT LIKE '1600%'
        THEN LPAD(SUBSTRING(jel_description, 12, 4), 5, 0)
        ELSE '00000'END AS rest_id
        , acct , cr_amt AS cr_amt, dr_amt AS dr_amt,
        CASE WHEN UPPER(jel_description) LIKE '%VISA%'AND acct ='115213' THEN '4' --Calculating VisaCard
        WHEN UPPER(jel_description) LIKE '%M/C' AND acct ='115213' THEN '5' --Calculating MasterCard
        WHEN UPPER(jel_description) LIKE '%AMEX' AND acct ='115213' THEN '6' --Calculating Amex
        WHEN UPPER(jel_description) LIKE '%DISC' AND acct ='115213' THEN '7' --Calculating Discover
        WHEN UPPER(jel_description) LIKE '%OTHER DEPOSIT' AND acct ='115213' THEN '29' --Calculating Other Deposits 
        WHEN UPPER(jel_description) LIKE 'CHARGE%' AND acct ='115213' THEN '24' --Calculating Charge tip
        WHEN UPPER(jel_description) LIKE '%RED%' AND acct ='115214' THEN '8' --Calculating GC Redeemed
        WHEN UPPER(jel_description) LIKE '%SOLD%' AND acct ='115214' THEN '26' --Calculating GC Sold
        WHEN UPPER(jel_description) LIKE 'CHARGE%' AND acct ='115254' THEN '30' --Calculating Employee Tip
        WHEN (acct LIKE '112%' OR acct='114182') THEN '3' --Calculating Cash
        ELSE mp1.fn_measure_id END AS measure_id,
        cc AS gl_cost_ctr
        ,'USA' AS country_code, cur_code AS currency_code,
        'Oracle General Ledger' AS source_system_name,
        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS load_id,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS load_dttm,
        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS update_id,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS update_dttm
        , mp1.fn_system_id 
        FROM FILTERED_RDS_RECORDS  gl 
        JOIN (
        SELECT fn_system_id, gl_account_code  ,  MIN(fn_measure_id ) fn_measure_id
        FROM IDS_{{params.target_env}}.TXN_BV.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BV
        WHERE  fn_system_id = '9' AND brand_id = 'sonic' and gl_account_code <> ''   Group by fn_system_id, gl_account_code
        ) mp1 ON mp1.gl_account_code = gl.acct   
    ) t1 
   JOIN (
        SELECT gl_account_code, credit_value_adjustment_amount, debit_value_adjustment_amount     ,   fn_measure_id
        FROM IDS_{{params.target_env}}.TXN_BV.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BV
        WHERE  fn_system_id = '9' AND brand_id = 'sonic' and gl_account_code <> ''
        ) mp	 ON mp.gl_account_code = t1.acct   and mp.fn_measure_id = t1.measure_id
    
 )
  SELECT
  brand_id, fisc_wk_end_date, rest_id,fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
  SUM((credit_score * cr_amt) + (debit_score * dr_amt)) AS sale_usd_amount,
  SUM((credit_score * cr_amt) + (debit_Score * dr_amt)) AS sale_amount,
  country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
  FROM MASTER_TXNS
  GROUP BY brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
  country_code, currency_code, source_system_name, load_id,load_dttm, update_id, update_dttm
) gls
ON fwrm.fisc_wk_end_date = gls.fisc_wk_end_date
AND fwrm.brand_id = gls.brand_id AND fwrm.fn_system_id = gls.fn_system_id
AND fwrm.fn_measure_id = gls.fn_measure_id AND fwrm.gl_account_code = gls.gl_account_code
AND fwrm.gl_cost_ctr = gls.gl_cost_ctr AND fwrm.rest_id = gls.rest_id
WHEN MATCHED THEN
UPDATE SET
fwrm.sale_usd_amount = gls.sale_usd_amount,
fwrm.sale_amount = gls.sale_amount,
fwrm.country_code = gls.country_code,
fwrm.currency_code = gls.currency_code,
fwrm.source_system_name = gls.source_system_name,
fwrm.update_id = gls.update_id,
fwrm.update_dttm = gls.update_dttm
WHEN NOT MATCHED THEN
INSERT
(
  brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
)
VALUES
(
  gls.brand_id, gls.fisc_wk_end_date, gls.rest_id, gls.fn_system_id, gls.fn_measure_id, gls.gl_account_code,
  gls.gl_cost_ctr, gls.sale_usd_amount, gls.sale_amount, gls.country_code, gls.currency_code, gls.source_system_name,
  gls.load_id, gls.load_dttm, gls.update_id, gls.update_dttm
);
            