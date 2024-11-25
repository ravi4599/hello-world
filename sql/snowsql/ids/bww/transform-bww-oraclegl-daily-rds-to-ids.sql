USE WAREHOUSE {{params.warehouse}};


--SET start_load_dt = '20230301' / NULL;
--SET end_load_dt = '20230901' / NULL;
SET start_load_dt = '{{params.load_start_dt}}';   --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';       --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='bww' and FN_SYSTEM_ID=13 ;

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_GL_Temp_table AS
SELECT brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
       sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM  (
WITH MIN_RAT_GL_DATE AS
    (
        SELECT MIN(
                    CASE WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%ALTAMETRICS%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 20, 10)), 'MM/DD/YYYY')  
                         WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%NBO%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 17, 8)), 'MM/DD/YY')
                         ELSE TO_DATE(gl_date, 'MM/DD/YYYY') END
                  ) AS min_rat_gl_date
        FROM "RDS_{{params.source_env}}"."BWW_BV"."ORACLE_GL_BV"  t1
        WHERE ( UPPER(t1.JEL_DESCRIPTION) NOT LIKE '%INTERCOMPANY%' )
          AND ( t1.loaddatetime >= TO_DATE($start_load_dt,'YYYYMMDD')
	              OR to_date(t1.gl_date) >= TO_DATE($start_load_dt,'YYYYMMDD') )
    )
, FILTERED_RDS_RECORDS AS
(
      SELECT dr_amt, cr_amt, gl_date, cc, acct, filedate, folderdate, loaddatetime, loc, jeh_name, je_source, jel_description, cur_code, batch_name, business_date
      FROM
       (   
       SELECT dr_amt, cr_amt, gl_date, cc, acct, filedate, folderdate, loaddatetime, loc, jeh_name, je_source, jel_description, cur_code, batch_name,
                DENSE_RANK() OVER (PARTITION BY filename
                                     ORDER BY filedate DESC, folderdate DESC,loaddatetime DESC) AS dense_rank
              , CASE  WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%ALTAMETRICS%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 20, 10)), 'MM/DD/YYYY')  
                    WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%NBO%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 17, 8)), 'MM/DD/YY')
                    ELSE TO_DATE(gl_date, 'MM/DD/YYYY') END AS business_date
		   FROM   "RDS_{{params.source_env}}"."BWW_BV"."ORACLE_GL_BV"  t1
           WHERE 
              TO_DATE(t1.gl_date,'MM/DD/YYYY') >= (SELECT min_rat_gl_date FROM MIN_RAT_GL_DATE)
              OR ( business_date >= (SELECT min_rat_gl_date FROM MIN_RAT_GL_DATE ))
       ) WHERE dense_rank =1
)
, MASTER_TXNS_NONGC AS
  (
   SELECT brand_id, business_date, rest_id, fn_system_id, gl_account_code, cr_amt, dr_amt, fn_measure_id, gl_cost_ctr,
          credit_score, debit_Score, country_code, currency_code, source_system_name,jeh_name,jel_description, load_id, load_dttm, update_id, update_dttm 
   FROM(				
        SELECT 'bww' AS brand_id, business_date, 
	        CASE 	WHEN gl.loc LIKE '3100%' 
			  	        THEN LPAD(SUBSTRING((RTRIM(gl.loc)), 5, 4), 5, 0)
				  WHEN gl.loc LIKE '900%' 
			    	THEN LPAD(RIGHT((RTRIM(REPLACE(gl.jel_description,' RED', ' '))), 4), 5, 0)
					ELSE '00000' 
		    END AS rest_id,   -- Deriving REST_ID here with the help of Columns (loc, jel_description, acct)
	        mp.fn_system_id, gl.acct AS gl_account_code, gl.cr_amt, gl.dr_amt, mp.fn_measure_id, gl.cc AS gl_cost_ctr,gl.jeh_name,gl.jel_description,
	        mp.credit_value_adjustment_amount AS credit_score, mp.debit_value_adjustment_amount AS debit_Score,
	        'USA' AS country_code, gl.cur_code AS currency_code, 'Oracle General Ledger' AS source_system_name,
	        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS load_id,
	        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS load_dttm,
	        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS update_id,
	        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS update_dttm
	        FROM FILTERED_RDS_RECORDS gl
	           JOIN IDS_{{params.source_env}}.TXN.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE mp 
	                 ON mp.gl_account_code = gl.acct AND mp.fn_system_id = '13' AND mp.brand_id = 'bww' AND mp.gl_account_code <> '' AND mp.gl_account_pattern_ind = 'FALSE'
          WHERE gl.acct <> '115214'
	    )  
  )
, MASTER_TXNS_GC_RED AS
  (
   SELECT brand_id, business_date, rest_id, fn_system_id, gl_account_code, cr_amt, dr_amt, fn_measure_id, gl_cost_ctr,
          credit_score, debit_Score, country_code, currency_code, source_system_name,jeh_name,jel_description, load_id, load_dttm, update_id, update_dttm 
   FROM(				
	        SELECT 'bww' AS brand_id, 
          CASE WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%ALTAMETRICS%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 20, 10)), 'MM/DD/YYYY')  
               WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%NBO%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 17, 8)), 'MM/DD/YY')
               ELSE TO_DATE(gl_date, 'MM/DD/YYYY') END AS business_date, 
	        CASE WHEN gl.loc LIKE '3100%' 
			  	        THEN LPAD(SUBSTRING((RTRIM(gl.loc)), 5, 4), 5, 0)
               WHEN gl.loc LIKE '900%' 
			  		      THEN LPAD(RIGHT((RTRIM(REPLACE(gl.jel_description,' RED', ' '))), 4), 5, 0)
	            ELSE '00000' END AS rest_id,                                     -- Deriving REST_ID here with the help of Columns (loc, jel_description, acct)
	        mp.fn_system_id, gl.acct AS gl_account_code, gl.cr_amt, gl.dr_amt, mp.fn_measure_id, 
          gl.cc AS gl_cost_ctr,gl.jeh_name,gl.jel_description,
	        mp.credit_value_adjustment_amount AS credit_score, 
          mp.debit_value_adjustment_amount AS debit_Score,
	        'USA' AS country_code, gl.cur_code AS currency_code, 'Oracle General Ledger' AS source_system_name,
	        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS load_id,
	        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS load_dttm,
	        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS update_id,
	        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS update_dttm
	        FROM FILTERED_RDS_RECORDS gl
	           JOIN IDS_{{params.source_env}}.TXN.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE mp 
	                 ON mp.gl_account_code = gl.acct AND mp.fn_system_id = '13' AND mp.brand_id = 'bww' AND mp.gl_account_code <> '' AND mp.gl_account_pattern_ind = 'FALSE'
          WHERE gl.acct = '115214' AND mp.fn_measure_id = '8'
          AND
          (        ((UPPER(je_source) LIKE '%ALTAMETRICS%' OR  UPPER(je_source) LIKE '%NBO%') AND UPPER(batch_name) NOT LIKE '%REVERSES%' AND gl.dr_amt > 0)
                OR ((UPPER(je_source) LIKE '%ALTAMETRICS%' OR  UPPER(je_source) LIKE '%NBO%') AND UPPER(batch_name) LIKE '%REVERSES%' AND gl.cr_amt > 0)
                OR (UPPER(je_source) LIKE '%SPREAD%' AND UPPER(jel_description) LIKE '%RED%')
          )
          
	    )  
  )
, MASTER_TXNS_GC_SOLD AS
  (
   SELECT brand_id, business_date, rest_id, fn_system_id, gl_account_code, cr_amt, dr_amt, fn_measure_id, gl_cost_ctr,
          credit_score, debit_Score, country_code, currency_code, source_system_name,jeh_name,jel_description, load_id, load_dttm, update_id, update_dttm 
   FROM(				
	        SELECT 'bww' AS brand_id, 
          CASE WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%ALTAMETRICS%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 20, 10)), 'MM/DD/YYYY')  
               WHEN UPPER(je_source) LIKE '%SPREAD%' AND  UPPER(jel_description) LIKE '%NBO%' THEN TO_DATE(RTRIM(SUBSTR(jel_description, 17, 8)), 'MM/DD/YY')
               ELSE TO_DATE(gl_date, 'MM/DD/YYYY') END AS business_date, 
	        CASE WHEN gl.loc LIKE '3100%' 
			  	        THEN LPAD(SUBSTRING((RTRIM(gl.loc)), 5, 4), 5, 0)
               WHEN gl.loc LIKE '900%' 
			  		      THEN LPAD(RIGHT((RTRIM(REPLACE(gl.jel_description,' RED', ' '))), 4), 5, 0)
	            ELSE '00000' END AS rest_id,                                     -- Deriving REST_ID here with the help of Columns (loc, jel_description, acct)
	        mp.fn_system_id, gl.acct AS gl_account_code, gl.cr_amt, gl.dr_amt, mp.fn_measure_id, 
          gl.cc AS gl_cost_ctr,gl.jeh_name,gl.jel_description,
	        mp.credit_value_adjustment_amount AS credit_score, 
          mp.debit_value_adjustment_amount AS debit_Score,
	        'USA' AS country_code, gl.cur_code AS currency_code, 'Oracle General Ledger' AS source_system_name,
	        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS load_id,
	        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS load_dttm,
	        TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS update_id,
	        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS update_dttm
	        FROM FILTERED_RDS_RECORDS gl
	           JOIN IDS_{{params.source_env}}.TXN.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE mp 
	                 ON mp.gl_account_code = gl.acct AND mp.fn_system_id = '13' AND mp.brand_id = 'bww' AND mp.gl_account_code <> '' AND mp.gl_account_pattern_ind = 'FALSE'
          WHERE gl.acct = '115214' AND mp.fn_measure_id = '26'
          AND
          (        ((UPPER(je_source) LIKE '%ALTAMETRICS%' OR  UPPER(je_source) LIKE '%NBO%') AND UPPER(batch_name) NOT LIKE '%REVERSES%' AND gl.cr_amt > 0)
                OR ((UPPER(je_source) LIKE '%ALTAMETRICS%' OR  UPPER(je_source) LIKE '%NBO%') AND UPPER(batch_name) LIKE '%REVERSES%' AND gl.dr_amt > 0)
                OR (UPPER(je_source) LIKE '%SPREAD%' AND UPPER(jel_description) NOT LIKE '%RED%')
          )
          
	    )  
  )
  SELECT brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
         SUM((credit_score * cr_amt) + (debit_score * dr_amt)) AS sale_usd_amount,    
         SUM((credit_score * cr_amt) + (debit_score * dr_amt)) AS sale_amount,
         country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
  FROM 
  (
  SELECT * FROM MASTER_TXNS_NONGC
  UNION ALL
  SELECT * FROM MASTER_TXNS_GC_RED
  UNION ALL
  SELECT * FROM MASTER_TXNS_GC_SOLD
  )
  WHERE fn_measure_id NOT IN (13,44)  -- Currently data is not needed for these measures (Waitr, Eat street)
  GROUP BY brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
           country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
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