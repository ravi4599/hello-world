USE WAREHOUSE {{params.warehouse}};

---SET start_load_dt = '20230301' / NULL;
---SET end_load_dt = '20230901' / NULL;
SET start_load_dt = '{{params.load_start_dt}}';   --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';       --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN TO_DATE($start_load_dt, 'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) >10
AND BRAND_ID='dnkn' and FN_SYSTEM_ID=19;

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE_GL_TEMP_TABLE" AS
SELECT brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
       sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM  
(
    With CTE_BUSINESS_DATE  AS

    (
        SELECT CALENDAR_DT as business_date FROM "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV"
        WHERE CALENDAR_DT BETWEEN TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')
        union 
        select distinct TRY_TO_DATE(RTRIM(SUBSTR(jel_description, 19, 10)), 'MM/DD/YYYY') 
        FROM "RDS_{{params.target_env}}"."DUN_BV"."ORACLE_GL_BV" 
        where loaddatetime BETWEEN TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')
    )
    , GL_DATA AS
	(
		SELECT
		Case WHEN regexp_instr(JEL_DESCRIPTION,'DLYSLS') > 1 then SUBSTR(JEL_DESCRIPTION, regexp_instr(JEL_DESCRIPTION,'DLYSLS') +7,10)
		WHEN UPPER(je_source) LIKE '%CRUNCHTIMESALES%' OR  UPPER(jel_description) LIKE '%JOURNAL%' THEN TRY_TO_DATE(RTRIM(SUBSTR(jel_description, 19, 10)), 'MM/DD/YYYY')
        WHEN (UPPER(je_source) LIKE '%SPREAD%' AND UPPER(jeh_name) LIKE '%RAT SALES ENTRY%' )then 
 		(TRY_TO_DATE(RTRIM(SUBSTR(jel_description,19, 10)), 'MM/DD/YYYY'))
		ELSE TO_DATE(gl_date, 'MM/DD/YYYY') END
		AS gl_date_new,
		Case WHEN regexp_instr(JEL_DESCRIPTION,'Store ID:') > 1 then SUBSTR(JEL_DESCRIPTION, regexp_instr(JEL_DESCRIPTION,'Store ID:') +10,6)
		Else SUBSTRING((RTRIM(t1.loc)), 3, 6) END AS store_id,		
		dr_amt, cr_amt, gl_date, cc, acct, filedate, folderdate, loaddatetime, loc, jeh_name, je_source, jel_description, cur_code, batch_name , filename
		FROM "RDS_{{params.target_env}}"."DUN_BV"."ORACLE_GL_BV"  t1
		 where gl_date_new is not null and gl_date_new in (select  business_date from CTE_BUSINESS_DATE)
	),
	FILTERED_RDS_RECORDS AS
	(
		SELECT dr_amt, cr_amt, gl_date_new, store_id, cc
        , Case when ACCT like '114%' then '114435' else ACCT end as ACCT  -- taking care for Cash deposite
        , filedate, folderdate, loaddatetime, loc, jeh_name, je_source, jel_description, cur_code, batch_name
        FROM
        (   
            SELECT 
            dr_amt, cr_amt, gl_date_new,store_id, cc, acct, filedate, folderdate, loaddatetime, loc, jeh_name, je_source, jel_description, cur_code, batch_name,
            to_Date( substring(filename, POSITION('UDP_', filename, 1)+4,8),'yyyymmdd') AS filedate1
            ,filedate1 as FolderDate1
           ,DENSE_RANK() OVER (PARTITION BY gl_date_new,store_id,acct,jel_description,batch_name ORDER BY filename desc, filedate DESC,folderdate DESC, loaddatetime DESC) AS dense_rank
            FROM GL_DATA t1
            WHERE ((UPPER(je_source) LIKE '%CRUNCHTIMESALES%' OR UPPER(jel_description) LIKE '%Journal%') 
					OR
				( UPPER(t1.je_source) LIKE '%SPREAD%' AND UPPER(t1.JEL_DESCRIPTION) NOT LIKE '%INTERCOMPANY%'))
        ) where dense_rank = 1					
	), 
	MASTER_TXNS AS
	  (
		SELECT 
		brand_id, business_date, rest_id, fn_system_id, gl_account_code, cr_amt, dr_amt, fn_measure_id, gl_cost_ctr,
		credit_score, debit_Score, country_code, currency_code, source_system_name,jeh_name,jel_description,TRIM(SUBSTR(JEL_DESCRIPTION, 46, 25)) GC_TYPE,
		load_id, load_dttm, update_id, update_dttm
		FROM(               
			SELECT
			'dnkn' as brand_id,
		  gl_date_new as business_date,
		  gl.jel_description,
		  store_id AS rest_id,
		  mp.fn_system_id as fn_system_id, gl.acct AS gl_account_code, 
		  gl.cr_amt as cr_amt, gl.dr_amt, mp.fn_measure_id,
		  gl.cc AS gl_cost_ctr, gl.jeh_name,
		  mp.credit_value_adjustment_amount AS credit_score,
		  mp.debit_value_adjustment_amount AS debit_Score,
		  'USA' AS country_code, gl.cur_code AS currency_code, 
		  'Oracle General Ledger' AS source_system_name,
		  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS load_id,
		  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS load_dttm,
		  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::STRING AS update_id,
		  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::STRING AS update_dttm
			FROM FILTERED_RDS_RECORDS gl
			JOIN(
				SELECT fn_system_id, gl_account_code, credit_value_adjustment_amount, debit_value_adjustment_amount,
				  MIN(fn_measure_id) AS fn_measure_id, brand_id
				  FROM IDS_{{params.target_env}}.TXN_BV.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BV
			  WHERE  fn_system_id = '19' AND brand_id = 'dnkn'
				GROUP BY fn_system_id, gl_account_code, credit_value_adjustment_amount, debit_value_adjustment_amount, brand_id
				ORDER BY fn_measure_id ASC
			) mp
			ON mp.gl_account_code = gl.acct AND mp.fn_system_id = '19' AND mp.brand_id = 'dnkn' AND mp.gl_account_code <> ''
		  )
	  )
	SELECT 
	brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
	Sum (
      Case when gl_account_code <> 115214 then (credit_score * cr_amt) + (debit_score * dr_amt) 
        else 
            case when fn_measure_id = 8 and GC_TYPE in ('GIFT CARD REDEMPTION','GIFT CARD REDEEM','GIFT CARD REFUND','RED') THEN (credit_score * cr_amt) + (debit_score * dr_amt) 
                when fn_measure_id = 26 and (GC_TYPE = 'GIFT CARD SALES' or len(GC_TYPE) = 0) THEN (credit_score * cr_amt) + (debit_score * dr_amt) 
             else 0 end
      end   
   ) AS sale_usd_amount, sale_usd_amount  AS  sale_amount,
	country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
	FROM MASTER_TXNS
	GROUP BY brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr,
         country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
) gl;
MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  select * from "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE_GL_TEMP_TABLE"
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