USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20230301' / NULL;
--SET end_load_dt = '20230715' / NULL;
SET start_load_dt = '{{params.load_start_dt}}'; --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}'; --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='sonic' and FN_SYSTEM_ID=8 ;


CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_SDI_BOS_MICROS_TEMP AS
(SELECT  brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM
(
  WITH FILTERED_DATES_MICROS_SALES_RECORDS AS
  (
        SELECT Distinct To_date(SUBSTR(t1.raw_data, 5, 8),'MMDDYYYY') as Business_Date 
        FROM  "RDS_{{params.source_env}}"."SDI_BV"."BOS_MICROS_SALES_BV" t1
        WHERE t1.loaddatetime 
        BETWEEN TO_DATE($start_load_dt, 'YYYYMMDD') AND TO_DATE($end_load_dt, 'YYYYMMDD') 
        UNION
        SELECT CALENDAR_DT as business_date FROM "IDS_{{params.source_env}}"."INT_REF_BV"."DATE_DIM_BV"  
        WHERE CALENDAR_DT BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))    
  ),  
  FILTERED_MICROS_SALES_RECORDS AS
  (
    SELECT
    storeid, salesdate, description, account_number, amount
    FROM
     (
      SELECT
      LPAD(SUBSTR(t1.raw_data, 1,4),5 ,0) AS storeid,
      SUBSTR(t1.raw_data, 5,8) AS salesdate,
      CASE
       WHEN SUBSTR(t1.raw_data, 23,5) = '10110' THEN 'BANK' --Calculating Cash Deposit
       WHEN SUBSTR(t1.raw_data, 23,5) = '11433' AND SUBSTR(t1.raw_data, 34, 100) LIKE '%VISA%' THEN 'VISA' --Calculating VisaCard
       WHEN SUBSTR(t1.raw_data, 23,5) = '11433' AND SUBSTR(t1.raw_data, 34, 100) LIKE '%MASTERCARD%' THEN 'MASTERCARD' --Calculating MasterCard
       WHEN SUBSTR(t1.raw_data, 23,5) = '11431' THEN 'AMERICAN EXPRESS' --Calculating Amex
       WHEN SUBSTR(t1.raw_data, 23,5) IN ('11432','11433') AND SUBSTR(t1.raw_data,34,100) LIKE '%DISCOVER%' THEN 'DISCOVER' --Calculating Discover
       WHEN SUBSTR(t1.raw_data, 23,5) = '50915' THEN 'DISCOUNT' --Calculating Discounts
       WHEN SUBSTR(t1.raw_data, 23,5) = '11438' THEN 'UBEREATS' --Calculating Ubereats
       WHEN SUBSTR(t1.raw_data, 23,5) = '11437' THEN 'GRUBHUB' --Calculating Grubhub
       WHEN SUBSTR(t1.raw_data, 23,5) = '11436' THEN 'DOORDASH' --Calculating Doordash
       WHEN SUBSTR(t1.raw_data, 23,5) = '11439' THEN 'POSTMATES' --Calculating postmates
       WHEN SUBSTR(t1.raw_data, 23,5) = '11440' THEN 'WAITR' --Calculating Waiter
       WHEN SUBSTR(t1.raw_data, 23,5) = '11525' THEN 'TIPS' --Calculating Employee Tips
       WHEN SUBSTR(t1.raw_data, 23,5) = '50190' AND t1.raw_data LIKE '%NTFOODSALE%'THEN 'NON TAXABLE'  -- Calculating Non Taxable
       WHEN SUBSTR(t1.raw_data, 10,28) LIKE '%50190%' AND t1.raw_data LIKE '%TEFOODSALE%' THEN 'TAX EXEMPT' --Calculating Tax Exemption
       WHEN SUBSTR(t1.raw_data, 23,5) = '62401' THEN RTRIM(SUBSTR(t1.raw_data, 13,10))  --getting PAIDIN OR PAIDOUT VALUE
       WHEN SUBSTR(t1.raw_data, 23,5) = '11475' AND SUBSTR(t1.raw_data, 13, 100) LIKE '%MYSONICACT%' THEN 'MYSONICACT' --Calculating GC Sold
       WHEN SUBSTR(t1.raw_data, 23,5) = '11475' AND SUBSTR(t1.raw_data, 13, 100) LIKE '%SONICCARD%' THEN 'SONIC CARD' --Calculating GC Redeemed
       WHEN SUBSTR(t1.raw_data, 23,5) = '32370' THEN 'TAXES' --Calculating Sales Tax
       WHEN SUBSTR(t1.raw_data, 23,5) = '62415' THEN 'OVER SHORT' --Calculating Overshot
       WHEN SUBSTR(t1.raw_data, 23,5) IN ('50110') THEN 'FOOD SALES' --Calculating Net Food Sales
       ELSE 'N/A' 
      END AS description, 
      SUBSTR(t1.raw_data, 23, 5) AS account_number,
      CASE
       WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) IN ('}','J','K','L','M','N','O','P','Q','R')
       THEN
        CASE 
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = '}'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'}',0),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'}',0)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'}',0),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'J'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'J',1),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'J',1)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'J',1),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'K'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'K',2),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'K',2)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'K',2),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'L'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'L',3),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'L',3)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'L',3),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'M'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'M',4),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'M',4)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'M',4),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'N'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'N',5),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'N',5)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'N',5),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'O'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'O',6),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'O',6)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'O',6),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'P'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'P',7),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'P',7)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'P',7),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'Q'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'Q',8),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'Q',8)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'Q',8),-2)))
        WHEN RIGHT(SUBSTR(t1.raw_data, 34,13),1) = 'R'
        THEN 0 - TO_DOUBLE(CONCAT(LEFT(REPLACE(SUBSTR(t1.raw_data, 34,13),'R',9),
        LEN(REPLACE(SUBSTR(t1.raw_data, 34,13),'R',9)) - 2),'.', SUBSTR(REPLACE(SUBSTR(t1.raw_data, 34,13),'R',9),-2)))
        END
       ELSE TO_DOUBLE(CONCAT(LEFT(SUBSTR(t1.raw_data, 34,13),
       LEN(SUBSTR(t1.raw_data, 34,13))-2),'.',SUBSTR(SUBSTR(t1.raw_data, 34,13),-2)))
      END AS amount,
      RANK() OVER (PARTITION BY SUBSTR(t1.filename, 33,18) 
                            ORDER BY t1.folderdate DESC, t1.filedate DESC, SUBSTR(t1.filename, 52,6) DESC, t1.loaddatetime DESC) AS rank   
      FROM "RDS_{{params.source_env}}"."SDI_BV"."BOS_MICROS_SALES_BV" t1
      INNER JOIN "IDS_{{params.source_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo   on lo.brand_id = 'sonic' AND LPAD(SUBSTR(t1.raw_data, 1,4),5 ,0)  = lo.rest_id 
      AND TO_DATE((SUBSTR(t1.raw_data, 5,8)),'MMDDYYYY') between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
      --temp close logic is not included becoz of bad data in REST_SCD TABLE for SONIC 
      WHERE To_date(SUBSTR(t1.raw_data, 5, 8),'MMDDYYYY') in (Select Business_Date from FILTERED_DATES_MICROS_SALES_RECORDS) 
      )
  WHERE rank = 1
  ),
  FILTERED_MICROS_SALES_RECORDS_FINAL AS
  (
    SELECT 
    storeid, salesdate,
    CASE WHEN UPPER(description) IN ( 'MYSONICACT') AND  R.RN = 2 THEN 'NON TAXABLE'
	WHEN UPPER(description) IN ('TAX EXEMPT','NON TAXABLE', 'TAXES') AND R.RN = 2 THEN 'FOOD SALES'
         WHEN UPPER(description) IN ('PAIDIN', 'PAIDOUT') THEN 'PAID OUT'  
    ELSE description END AS Measure_Desc, --Calculating Netfood sales need to do duplicate some measure. Formula: (FOOD SALES + TAX EXEMPT - TAXES - DISCOUNT).
    account_number, 
    CASE WHEN (UPPER(description) IN ('TAXES') AND R.RN = 2) OR UPPER(description) ='PAIDIN' THEN -1 * AMOUNT 
	WHEN UPPER(description) IN ( 'MYSONICACT') AND  R.RN = 2 THEN -1 * AMOUNT
	ELSE AMOUNT END  AS AMOUNT
    FROM FILTERED_MICROS_SALES_RECORDS
    INNER JOIN (SELECT 1 AS RN UNION ALL SELECT 2 AS RN) R ON (UPPER(description) IN ('TAX EXEMPT','NON TAXABLE','TAXES','MYSONICACT')
                                                               AND R.RN = 2) OR R.RN = 1
  )
  SELECT
  'sonic'                                                      AS brand_id,
  TO_DATE(cisc.salesdate,'MMDDYYYY')                           AS business_date,
  cisc.storeid                                                 AS rest_id,
  '8'                                                          AS fn_system_id,
  fsmr.fn_measure_id                                           AS fn_measure_id,
  'N/A'                                                        AS gl_account_code,
  'N/A'                                                        AS gl_cost_ctr,
  SUM(COALESCE(fsmr.value_adjustment_amount,1) * cisc.amount)  AS sale_usd_amount,
  SUM(COALESCE(fsmr.value_adjustment_amount,1) * cisc.amount)  AS sale_amount,
  'USA'                                                        AS country_code,
  'USD'                                                        AS currency_code,
  'micros'                                                     AS source_system_name,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                 AS load_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                          AS load_dttm,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))                 AS update_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                          AS update_dttm
  FROM FILTERED_MICROS_SALES_RECORDS_FINAL cisc
  INNER JOIN "IDS_{{params.target_env}}".TXN.FN_SYSTEM_TO_MEASURE_REFERENCE fsmr
  ON TRIM(fsmr.source_sales_system_measure_text) = TRIM(cisc.Measure_Desc)
  AND fsmr.fn_system_id = '8' AND fsmr.brand_id='sonic'
  GROUP BY cisc.salesdate, cisc.storeid, fsmr.fn_measure_id
));
MERGE INTO "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE fdrm
USING
( 
SELECT
	brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
	sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM  "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_SDI_BOS_MICROS_TEMP
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