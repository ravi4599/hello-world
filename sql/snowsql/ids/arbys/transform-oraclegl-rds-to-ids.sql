USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20220901' / NULL;
--SET end_load_dt = '20230915' / NULL;
SET start_load_dt = '{{params.load_start_dt}}';
SET end_load_dt = '{{params.load_end_dt}}';

DELETE FROM IDS_{{params.target_env}}.TXN."FN_WEEKLY_REV_MEASURE"
WHERE (FISC_WK_END_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='arbys' and FN_SYSTEM_ID=4 ;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_WEEKLY_REV_MEASURE" fwrm
USING
(
  WITH DATE_TO_BE_PROCESSED AS  --Selecting maximum update_dttm from Source table
  (
   SELECT 
   TO_CHAR(CAST(MAX(loaddatetime) AS DATE), 'YYYY-MM-DD') AS process_dt,
   TO_DATE($start_load_dt,'YYYYMMDD') AS sdate,
   TO_DATE($end_load_dt,'YYYYMMDD') AS edate 
   FROM "RDS_{{params.source_env}}"."ARB_BV"."ORACLE_GL_BV"
  ),  
  FILTERED_RDS_RECORDS AS    -- This CTE filters data from ORACLE_GL with no duplicates for specific dates from the above CTE
  (
    SELECT
    batch_name, jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt, acct, f2, cc, filedate, folderdate,
    TO_DATE(gl_date, 'MM/DD/YYYY') AS calc_gl_date
    FROM
    (
      SELECT
      gl_date, batch_name, jeh_name, je_source, jel_description,
      cur_code, dr_amt, cr_amt, acct, f2, cc, filedate, folderdate
       , to_Date( substring(filename, POSITION('UDP_', filename, 1)+4,8),'yyyymmdd') AS filedate1       ,  filedate1 as FolderDate1
      , DENSE_RANK() OVER (PARTITION BY gl_date, CC, acct,batch_name
                         ORDER BY filedate1 DESC, folderdate1 DESC,loaddatetime DESC) AS dense_rank
      FROM "RDS_{{params.source_env}}"."ARB_BV"."ORACLE_GL_BV" t1
      WHERE UPPER(t1.je_source) = 'SALES' AND UPPER(t1.batch_name) LIKE '%SLS%'
            AND (t1.loaddatetime BETWEEN (SELECT COALESCE(sdate, process_dt) FROM DATE_TO_BE_PROCESSED) 
                                 AND     (SELECT COALESCE(edate, DATEADD(DAY, +1, process_dt)) FROM DATE_TO_BE_PROCESSED))
    )
    WHERE dense_rank = 1
    UNION ALL
    SELECT
    batch_name, jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt, acct, f2, cc, filedate, folderdate,  calc_gl_date
    FROM
    (
      SELECT
      gl_date, batch_name, jeh_name, je_source, jel_description, cur_code,
      dr_amt, cr_amt, acct, f2, cc, filedate, folderdate
      , to_Date( substring(filename, POSITION('UDP_', filename, 1)+4,8),'yyyymmdd') AS filedate1       ,  filedate1 as FolderDate1
      , CASE WHEN TRY_CAST(SUBSTR(jeh_name, 1, 10) AS DATE) IS NOT NULL THEN CAST(SUBSTR(jeh_name, 1, 10) AS DATE)
         WHEN TRY_CAST(SUBSTR(jeh_name, 10, 10) AS DATE) IS NOT NULL THEN CAST(SUBSTR(jeh_name, 10, 10) AS DATE)
         ELSE TO_DATE(gl_date, 'MM/DD/YYYY')  END AS calc_gl_date
      , DENSE_RANK() OVER (PARTITION BY calc_gl_date, CC, acct, batch_name 
                                           ORDER BY filedate DESC, folderdate DESC,loaddatetime DESC) AS dense_rank
      FROM "RDS_{{params.source_env}}"."ARB_BV"."ORACLE_GL_BV" t1
      WHERE UPPER(t1.je_source) LIKE '%SPREAD%' AND UPPER(t1.JEL_DESCRIPTION) NOT LIKE '%INTERCOMPANY%' 
            AND (TRY_CAST(SUBSTR(jeh_name, 1, 10) AS DATE) IS NOT NULL OR TRY_CAST(SUBSTR(jeh_name, 10, 10) AS DATE) IS NOT NULL) 
            AND (t1.loaddatetime BETWEEN (SELECT COALESCE(sdate, process_dt) FROM DATE_TO_BE_PROCESSED) 
                                AND     (SELECT COALESCE(edate, DATEADD(DAY, +1, process_dt)) FROM DATE_TO_BE_PROCESSED))
    )
    WHERE dense_rank =1
  ),
  MASTER_TXNS AS     -- This is the main CTE which calculates specific rest_ids, needed measures, sale_usd_amount
  (
    SELECT
    'arbys' AS brand_id,calc_gl_date AS fisc_wk_end_date,
	CASE when gl.acct = '115214' then RIGHT(gl.jel_description, 5) 
	WHEN gl.cc LIKE '1100%' THEN LPAD(SUBSTRING(gl.cc,4,5),5,0)
     WHEN (gl.acct LIKE '1152%' OR gl.acct LIKE '112%') AND gl.jel_description LIKE '110%' THEN LPAD(SUBSTRING(gl.jel_description,4,5),5,0)
     WHEN gl.cc LIKE '115%' AND UPPER(gl.jel_description) LIKE '%RED%' THEN LPAD(replace(gl.jel_description,'GCARD RED',''),5,0)
     WHEN gl.acct IN ('230222','610101','230232','732105','739101','739104','115243','230341','230342','610126') THEN LPAD(RIGHT(gl.f2,5),5,0)
    ELSE '00000' END AS rest_id,
    MP.fn_system_id AS fn_system_id,
    CASE
         WHEN gl.acct = '115213' AND UPPER(gl.jel_description) LIKE '%ONLINE%' THEN '29'
         WHEN gl.acct = '115213' AND UPPER(gl.jel_description) LIKE '%AMEX' THEN '6'
         WHEN gl.acct = '115213' AND UPPER(gl.jel_description) LIKE '%DISC' THEN '7'
         WHEN gl.acct = '115213' AND UPPER(gl.jel_description) LIKE '%M/C' THEN '5'
         WHEN gl.acct = '115213' AND UPPER(gl.jel_description) LIKE '%VISA' THEN '4'
         WHEN gl.acct IN ('230341','230342','115214') AND UPPER(gl.jel_description) LIKE 'GCARD RED%' THEN '8'
         WHEN gl.acct IN ('230341','230342','115214') AND UPPER(gl.jel_description) LIKE 'GIFT CARD SOLD%' THEN '26'
	     WHEN gl.acct LIKE '112%' THEN '3'
	     WHEN gl.acct LIKE '7%' AND gl.acct NOT IN ('739101','739104') THEN '17'
    ELSE MP.fn_measure_id END AS fn_measure_id,
    gl.acct AS gl_account_code, gl.cc AS gl_cost_ctr, gl.dr_amt, gl.cr_amt, 'USA' AS country_code,
    gl.cur_code AS currency_code, 'Oracle General Ledger' AS source_system_name,
    TO_NUMBER( TO_CHAR(CURRENT_DATE, 'YYYYMMDD') ):: STRING AS load_id,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP):: STRING AS load_dttm,
    TO_NUMBER( TO_CHAR(CURRENT_DATE, 'YYYYMMDD') ):: STRING AS update_id,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP):: STRING AS update_dttm
    FROM FILTERED_RDS_RECORDS gl
    JOIN (
         SELECT
         fn_system_id, gl_account_code, MIN(fn_measure_id) AS fn_measure_id
         FROM IDS_{{params.source_env}}.TXN_BV.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BV
         WHERE fn_system_id = '4' AND brand_id = 'arbys'
         GROUP BY fn_system_id, gl_account_code
         ORDER BY fn_measure_id ASC
         ) MP
         ON MP.gl_account_code = (CASE WHEN gl.acct LIKE '112%' THEN '112X'
  	                                   WHEN gl.acct LIKE '7%' AND gl.acct NOT IN ('739101','739104')  THEN '7X'
                                       ELSE gl.acct
                                  END)
  )
  SELECT
  brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, 
  SUM(CASE WHEN fn_measure_id IN (1,2,22,23,24,26,28,30) THEN (cr_amt - dr_amt)
           WHEN fn_measure_id IN (3,4,5,6,7,8,9,10,11,12,13,14,17,18,20,21,25,27,29,31) THEN (dr_amt - cr_amt) 
      ELSE 0 END)AS sale_usd_amount, 
  SUM(CASE WHEN fn_measure_id IN (1,2,22,23,24,26,28,30) THEN (cr_amt - dr_amt)
           WHEN fn_measure_id IN (3,4,5,6,7,8,9,10,11,12,13,14,17,18,20,21,25,27,29,31) THEN (dr_amt - cr_amt) 
      ELSE 0 END) AS sale_amount,
  country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
  FROM MASTER_TXNS
  GROUP BY brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
           country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
) gls
ON fwrm.fisc_wk_end_date = gls.fisc_wk_end_date
AND fwrm.brand_id = 'arbys'
AND fwrm.fn_system_id = '4'
AND fwrm.fn_measure_id = GLS.fn_measure_id
AND fwrm.gl_account_code = GLS.gl_account_code
AND fwrm.gl_cost_ctr = GLS.gl_cost_ctr
AND fwrm.rest_id = GLS.rest_id
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
  gls.brand_id, gls.fisc_wk_end_date, gls.rest_id, gls.fn_system_id, GLS.fn_measure_id, gls.gl_account_code,
  gls.gl_cost_ctr, gls.sale_usd_amount, gls.sale_amount, gls.country_code, gls.currency_code, gls.source_system_name,
  gls.load_id, gls.load_dttm, gls.update_id, gls.update_dttm
);
