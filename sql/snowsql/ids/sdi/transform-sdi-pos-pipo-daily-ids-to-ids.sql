USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20230301' / NULL;
--SET end_load_dt = '20230715' / NULL;
SET start_load_dt = '{{params.load_start_dt}}';   --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}'; --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (business_date BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) AND 
DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='sonic' and FN_SYSTEM_ID in (5,6) AND fn_measure_id = 17;


MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING 
(
 WITH FILTERED_PAIDINOUT_RECORDS AS
 (
  SELECT
  brand_id, business_date, rest_id, fn_measure_id, sale_usd_amount, sale_amount, sale_count,
  source_system_name, fn_system_id, country_code, currency_code
  FROM IDS_{{params.source_env}}.TXN."FN_DAILY_REV_MEASURE"
  WHERE brand_id='sonic' AND fn_system_id IN ('7', '8') AND fn_measure_id IN (17) ----Calculating the PaidOut from BOS
  AND ( 
  load_dttm BETWEEN (TO_DATE($start_load_dt, 'YYYYMMDD')) AND  (TO_DATE($end_load_dt, 'YYYYMMDD'))
  OR business_date BETWEEN (TO_DATE($start_load_dt, 'YYYYMMDD')) AND (TO_DATE($end_load_dt, 'YYYYMMDD'))
      )
 ) 
 SELECT
 brand_id                                                                          AS brand_id,
 business_date                                                                     AS business_date,
 rest_id                                                                           AS rest_id,
 CASE WHEN fn_system_id='7' THEN '5' WHEN fn_system_id='8' THEN '6' ELSE '0' END   AS fn_system_id,
 fn_measure_id                                                                     AS fn_measure_id,
 'N/A'                                                                             AS gl_account_code,
 'N/A'                                                                             AS gl_cost_ctr,
 SUM(sale_usd_amount)                                                              AS sale_usd_amount,
 SUM(sale_amount)                                                                  AS sale_amount,
 country_code                                                                      AS country_code,
 currency_code                                                                     AS currency_code,
 source_system_name                                                                AS source_system_name,
 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                                      AS load_id,
 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                               AS load_dttm,
 TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                                      AS update_id,
 TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                               AS update_dttm
 FROM FILTERED_PAIDINOUT_RECORDS PIPO
 GROUP BY fn_system_id, brand_id, business_date, rest_id, fn_measure_id, currency_code,
          country_code, source_system_name
) ams
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
  ams.gl_cost_ctr, ams.sale_usd_amount, ams.sale_amount, ams.country_code, ams.currency_code, ams.source_system_name,
  ams.load_id, ams.load_dttm, ams.update_id, ams.update_dttm
);
