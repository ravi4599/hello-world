USE WAREHOUSE {{params.warehouse}};

---SET start_load_dt = '20220701' / NULL;
---SET end_load_dt = '20230415' / NULL;               
SET start_load_dt = '{{params.load_start_dt}}';
SET end_load_dt = '{{params.load_end_dt}}';
SET start_wk_dt = (SELECT REPLACE((fiscal_week_end_dt -7),'-','') AS start_wk_dt
                   FROM "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv WHERE dv.date_key = $start_load_dt);
SET end_wk_dt = (SELECT REPLACE((fiscal_week_end_dt),'-','')  
                 FROM "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv WHERE dv.date_key = $end_load_dt);

MERGE INTO "IDS_{{params.target_env}}"."TXN"."FN_WEEKLY_REV_MEASURE" fwrm
USING
(
  WITH DATE_TO_BE_PROCESSED AS
  (
  SELECT MAX(dv.fiscal_week_end_dt) AS process_dt,
  TO_DATE($start_wk_dt,'YYYYMMDD') AS sdate,
  TO_DATE($end_wk_dt,'YYYYMMDD') AS edate,
  MAX(CAST(ss.update_dttm AS DATE)) AS udate
  FROM "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE" ss
  JOIN "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv
  ON ss.business_date = dv.calendar_dt WHERE ss.fn_system_id = '18' AND ss.brand_id ='dnkn'
  ),
  FILTERED_RDS_RECORDS AS
  (
  SELECT
  brand_id, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount, sale_count,
  country_code, currency_code, source_system_name, business_date
  FROM
    (
      SELECT
      fdss.brand_id, fdss.rest_id, fdss.fn_system_id, fdss.fn_measure_id,
      fdss.gl_account_code, fdss.gl_cost_ctr, fdss.sale_usd_amount, fdss.sale_count,
      fdss.country_code, fdss.currency_code, fdss.source_system_name, fdss.business_date,
      ROW_NUMBER() OVER (PARTITION BY fdss.fn_system_id, fdss.business_date, fdss.brand_id, fdss.rest_id,
      fdss.fn_measure_id, fdss.gl_account_code, fdss.gl_cost_ctr ORDER BY fdss.load_dttm DESC) AS row_num
      FROM "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE" fdss
      JOIN "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv ON fdss.business_date = dv.calendar_dt
      WHERE fn_system_id = '18' AND fdss.brand_id = 'dnkn' AND dv.fiscal_week_end_dt BETWEEN
                         (SELECT COALESCE(sdate, udate) FROM DATE_TO_BE_PROCESSED)
                         AND  (SELECT COALESCE(edate, DATEADD(DAY ,+7, udate)) FROM DATE_TO_BE_PROCESSED)
    )
    WHERE row_num = 1
  ),
  MASTER_TXNS AS
  (
    SELECT
    fds.brand_id                                  AS brand_id,
    dv.fiscal_week_end_dt                         AS fisc_wk_end_date,
    fds.rest_id                                   AS rest_id,
    fds.fn_system_id                              AS fn_system_id,
    fds.fn_measure_id                             AS fn_measure_id,
    fds.gl_account_code                           AS gl_account_code,
    fds.gl_cost_ctr                               AS gl_cost_ctr,
    SUM(fds.sale_usd_amount)                      AS sale_usd_amount,
    SUM(fds.sale_usd_amount)                      AS sale_amount,
    SUM(fds.sale_count)                           AS sale_count,
    fds.country_code                              AS country_code,
    fds.currency_code                             AS currency_code,
    fds.source_system_name                        AS source_system_name,
    TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))  AS load_id,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)           AS load_dttm,
    TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))  AS update_id,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)           AS update_dttm
    FROM FILTERED_RDS_RECORDS FDS
    INNER JOIN "IDS_{{params.target_env}}"."INT_REF_BV"."DATE_DIM_BV" dv ON fds.business_date = dv.calendar_dt
    GROUP BY fds.brand_id, dv.fiscal_week_end_dt, fds.rest_id, fds.fn_system_id, fds.fn_measure_id,
    fds.gl_account_code, fds.gl_cost_ctr, fds.country_code, fds.currency_code, fds.source_system_name
  )
  SELECT
  brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
  FROM MASTER_TXNS
) ams
ON   
fwrm.fisc_wk_end_date = ams.fisc_wk_end_date
AND fwrm.brand_id = ams.brand_id
AND fwrm.fn_system_id = ams.fn_system_id
AND fwrm.fn_measure_id = ams.fn_measure_id
AND fwrm.gl_account_code = ams.gl_account_code
AND fwrm.gl_cost_ctr = ams.gl_cost_ctr
AND fwrm.rest_id = ams.rest_id
WHEN MATCHED THEN
UPDATE SET
fwrm.sale_usd_amount = ams.sale_usd_amount,
fwrm.sale_amount = ams.sale_amount,
fwrm.country_code = ams.country_code,
fwrm.currency_code = ams.currency_code,
fwrm.source_system_name = ams.source_system_name,
fwrm.update_id = ams.update_id,
fwrm.update_dttm = ams.update_dttm
WHEN NOT MATCHED THEN
INSERT 
(
  brand_id, fisc_wk_end_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
)
VALUES
(
  ams.brand_id, ams.fisc_wk_end_date, ams.rest_id, ams.fn_system_id, ams.fn_measure_id, ams.gl_account_code,
  ams.gl_cost_ctr, ams.sale_usd_amount, ams.sale_amount, ams.country_code, ams.currency_code,
  ams.source_system_name, ams.load_id, ams.load_dttm, ams.update_id, ams.update_dttm
);