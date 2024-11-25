USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20230801' / NULL;
--SET end_load_dt = '20230910' / NULL;                  
SET start_load_dt = '{{params.load_start_dt}}'; --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';  --end_load_dt from Airflow Parameters
SET start_wk_dt = (SELECT REPLACE((fiscal_week_end_dt  -7),'-','') AS start_wk_dt
                   FROM "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv  WHERE dv.date_key = $start_load_dt);
SET end_wk_dt   = (SELECT REPLACE((fiscal_week_end_dt ),'-','')
                   FROM "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv WHERE dv.date_key = $end_load_dt);
				   
MERGE INTO "IDS_{{params.target_env}}"."TXN"."FN_WEEKLY_REV_MEASURE" fwrm
USING
(
  WITH DATE_TO_BE_PROCESSED AS
  (
    SELECT MAX(dv.fiscal_week_end_dt) AS process_dt,
    TO_DATE($start_wk_dt,'YYYYMMDD') AS sdate,
    TO_DATE($end_wk_dt,'YYYYMMDD') AS edate,
    MAX(CAST(ss.update_dttm  AS DATE)) AS udate
    FROM "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE" ss
    JOIN "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv  ON ss.business_date = dv.calendar_dt
    WHERE ss.fn_system_id IN (14) AND ss.brand_id = 'jj'
  ),
  FILTERED_RDS_RECORDS AS
  (
   SELECT
   brand_id, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
   sale_count, country_code, currency_code, source_system_name, business_date
   FROM
   (
    SELECT
    fdss.brand_id, fdss.rest_id, fdss.fn_system_id, fdss.fn_measure_id, fdss.gl_account_code,
    fdss.gl_cost_ctr, fdss.sale_usd_amount, fdss.sale_count, fdss.country_code,
    fdss.currency_code, fdss.source_system_name, fdss.business_date,
    ROW_NUMBER() OVER (PARTITION BY fdss.fn_system_id, fdss.business_date, fdss.brand_id, fdss.rest_id,
                     fdss.fn_measure_id, fdss.gl_account_code, fdss.gl_cost_ctr ORDER BY fdss.load_dttm DESC) AS row_num
	  FROM "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE" fdss
	  JOIN "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv ON fdss.business_date = dv.calendar_dt
    WHERE fn_system_id IN (14) AND dv.fiscal_week_end_dt  BETWEEN
                                             (SELECT COALESCE(sdate,udate) FROM DATE_TO_BE_PROCESSED) AND
                                             (SELECT COALESCE(edate,DATEADD(Day ,+7, udate)) FROM DATE_TO_BE_PROCESSED)
   )					 
   WHERE row_num = 1 AND brand_id = 'jj'
  ),
  MASTER_TXNS AS
  ( 
   SELECT
   fds.brand_id AS brand_id, dv.fiscal_week_end_dt  AS fisc_wk_end_date,
   fds.rest_id AS rest_id, fds.fn_system_id AS fn_system_id, fds.fn_measure_id AS fn_measure_id,
   fds.gl_account_code AS gl_account_code, fds.gl_cost_ctr AS gl_cost_ctr, SUM(fds.sale_usd_amount) AS sale_usd_amount,
   SUM(fds.sale_usd_amount) AS sale_amount, SUM(fds.sale_count) AS sale_count, fds.country_code AS country_code,
   fds.currency_code AS currency_code, fds.source_system_name AS source_system_name,
   TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS load_id, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS load_dttm,
   TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS update_id, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS update_dttm 
   FROM FILTERED_RDS_RECORDS fds 
   INNER JOIN "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv ON fds.business_date = dv.calendar_dt
   GROUP BY fds.brand_id, dv.fiscal_week_end_dt ,fds.rest_id, fds.fn_system_id, fds.fn_measure_id,
   fds.gl_account_code, fds.gl_cost_ctr, fds.country_code, fds.currency_code, fds.source_system_name
  )  
  SELECT
  brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
  FROM MASTER_TXNS
) pbs
ON fwrm.fisc_wk_end_date = pbs.fisc_wk_end_date
AND fwrm.brand_id = pbs.brand_id
AND fwrm.fn_system_id = pbs.fn_system_id
AND fwrm.fn_measure_id = pbs.fn_measure_id
AND fwrm.gl_account_code = pbs.gl_account_code
AND fwrm.gl_cost_ctr = pbs.gl_cost_ctr
AND fwrm.rest_id = pbs.rest_id
WHEN MATCHED THEN
UPDATE SET
fwrm.sale_usd_amount = pbs.sale_usd_amount,
fwrm.sale_amount = pbs.sale_amount,
fwrm.country_code = pbs.country_code,
fwrm.currency_code = pbs.currency_code,
fwrm.source_system_name = pbs.source_system_name,
fwrm.update_id = pbs.update_id,
fwrm.update_dttm  = pbs.update_dttm
WHEN NOT MATCHED THEN
INSERT
(
  brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
)
VALUES
(
  pbs.brand_id, pbs.fisc_wk_end_date, pbs.rest_id, pbs.fn_system_id, pbs.fn_measure_id, pbs.gl_account_code,
  pbs.gl_cost_ctr, pbs.sale_usd_amount, pbs.sale_amount, pbs.country_code, pbs.currency_code, pbs.source_system_name,
  pbs.load_id, pbs.load_dttm, pbs.update_id, pbs.update_dttm
);