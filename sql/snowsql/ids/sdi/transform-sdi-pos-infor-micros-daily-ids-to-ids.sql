USE WAREHOUSE {{params.warehouse}};


--SET start_load_dt = '20230101';
--SET end_load_dt   = '20230530';
SET start_load_dt = '{{params.load_start_dt}}'; --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}'; --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) AND 
DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='sonic' AND FN_SYSTEM_ID IN(5,6) AND fn_measure_id NOT IN (17);

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_SDI_POS_INFOR_MICROS_TEMP AS
(SELECT  brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM
(
  WITH FILTERED_RECORDS AS
  ( 
   SELECT t.brand_id, t.rest_id, t.business_date, t.tax_exempt_ind, t.void_ind, t.source_system_name, t.order_id,
   t.derived_net_amt, t.source_net_amt, t.tax_amt, t.misc_charge_amt, t.derived_gross_amt, t.gratuity_amt, t.derived_discount_amt
   FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS" t
   INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on
   lo.brand_id = 'sonic' AND 
   t.rest_id = lo.REST_ID AND 
   t.business_date between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
   --temp close logic is not included becoz of bad data in REST_SCD TABLE for SONIC 
   WHERE t.brand_id ='sonic' AND void_ind = 'FALSE' 
   AND (    ( t.update_dttm BETWEEN  (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD')) )
        OR  ( t.business_date BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD')) )
        )
  ),
  CTE_TRANSLINE AS
  (
    SELECT  t.brand_id, t.rest_id, t.business_date, 26 as fn_measure_id, t.source_system_name
    ,SUM(tl.misc_charge_amt) AS sales_usd_amount    , count(*) sales_count
    FROM FILTERED_RECORDS t 
    JOIN "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE" tl ON t.brand_id = tl.brand_id AND t.rest_id = tl.rest_id  AND t.business_date = tl.business_date AND t.order_id = tl.order_id
    where tl.void_ind = 'FALSE' AND UPPER(tl.item_type_code) = 'GIFTCARD'
    GROUP BY t.source_system_name, t.brand_id, t.rest_id, t.business_date , fn_measure_id
   
  ),
  CTE_PAYMENT AS
  (
   SELECT t.brand_id, t.rest_id, t.business_date, t.source_system_name,
     CASE WHEN UPPER(p.card_type) = 'DOORDASH' THEN 11 --Calculating Doordash
          WHEN UPPER(p.card_type) = 'UBEREATS' THEN 10 --Calculating Ubereats
          WHEN UPPER(p.card_type) = 'GRUBHUB' THEN 12 --Calculating Grubhub
		  WHEN UPPER(P.card_type) = 'WAITER' THEN 13 --Calculating Waiter
          WHEN UPPER(p.card_type) = 'POSTMATES' THEN 25 --Calculating Posstmates
          WHEN UPPER(p.card_type) = 'CREDITCARD' AND UPPER(p.card_issuer_name) = 'AMEX' THEN 6  --Calculating Amex
          WHEN UPPER(p.card_type) = 'CREDITCARD' AND UPPER(p.card_issuer_name) = 'VISA' THEN 4 --Calculating VisaCard
          WHEN UPPER(p.card_type) = 'CREDITCARD' AND UPPER(p.card_issuer_name) = 'MASTERCARD' THEN 5 --Calculating MasterCard
          WHEN UPPER(p.card_type) = 'CREDITCARD' AND UPPER(p.card_issuer_name) = 'DISC' THEN 7 --Calculating Discover
          WHEN UPPER(p.card_type) = 'SONICGIFTCARD' AND t.misc_charge_amt = 0 THEN 8 --Calculating GC Redeemed
          WHEN UPPER(p.card_type) = 'CASH' OR UPPER(p.card_type) IS NULL THEN 3 --Calculating Cash
		  ELSE 29 END AS fn_measure_id --Calculating Other Deposit
	 ,sum(ifnull(p.payment_amt,0)) sales_usd_amount --order_amt and payment_amt are sourcing from POS CARD
     ,COUNT(*) sales_count
    FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS_PAYMENT" p
    join FILTERED_RECORDS t using (brand_id, business_date,rest_id, order_id)   
    where IFNULL(is_void_ind,'N') = 'N'   
    GROUP BY brand_id, rest_id, business_date, fn_measure_id, t.source_system_name
  ),
  CTE_CASH_DEPO AS  --Calculating the cash deposit (Cash_deposit - Paidout)
  (
  SELECT cp.brand_id, cp.business_date, cp.rest_id, cp.fn_measure_id, cp.source_system_name
    , nvl(cp.sales_usd_amount,0) - nvl(cpo.sale_usd_amount,0) AS sales_usd_amount,  cp.sales_count
    --, cp.sales_usd_amount, cpo.sale_usd_amount AS fn_sales_usd_amount,  cp.sales_count
    FROM CTE_PAYMENT cp
    Left outer join  "IDS_{{params.source_env}}".TXN."FN_DAILY_REV_MEASURE" cpo  ON cp.brand_id = cpo.brand_id    AND cp.rest_id = cpo.rest_id    AND cp.business_date = cpo.business_date  AND cpo.fn_system_id IN ('7', '8') AND cpo.fn_measure_id IN (17)
    where cp.fn_measure_id = 3 
  ),
  CTE_TRANS_MEASURE_ID AS
  (
  select * from  (values (2),(15),(30),(33),(43)) as t (fn_measure_id) -- Sales Tax, Tax Exempt Sales,  GRATUITY_AMT, DERIVED_DISCOUNT_AMT, Net Food Sales
  ),
  CTE_TRANS_CALC AS
  (
   SELECT t.brand_id, t.business_date, t.rest_id , t.source_system_name, dc.fn_measure_id
   ,SUM(CASE WHEN dc.fn_measure_id = 43 THEN t.derived_gross_amt      --Calculating Net food Sales
                      WHEN dc.fn_measure_id = 2  THEN t.tax_amt               --Calculating Sales Tax
                      WHEN dc.fn_measure_id = 30 THEN t.gratuity_amt          --Calculating Employee Tip
                      WHEN dc.fn_measure_id = 33 THEN t.derived_discount_amt  --Calculating Discount
                      WHEN dc.fn_measure_id = 15 AND t.tax_exempt_ind = 'Y' 
                          THEN t.derived_net_amt                              --Calculating Tax Exemption
                    else 0   END) sales_usd_amount,
   COUNT(t.rest_id ) sales_count   
   FROM FILTERED_RECORDS  t
   JOIN CTE_TRANS_MEASURE_ID dc
   GROUP BY t.brand_id ,t.rest_id, t.business_date , t.source_system_name, dc.fn_measure_id
  ),
  MASTER_TXNS AS
  (
   SELECT brand_id, business_date, rest_id, fn_measure_id, source_system_name, sales_usd_amount, sales_count  
   FROM CTE_PAYMENT WHERE fn_measure_id NOT IN ('3')
   UNION ALL
   SELECT brand_id, business_date, rest_id, fn_measure_id, source_system_name, sales_usd_amount, sales_count  
   FROM CTE_CASH_DEPO  WHERE fn_measure_id IN ('3')
   UNION ALL
   SELECT  brand_id, business_date, rest_id, fn_measure_id, source_system_name, sales_usd_amount, sales_count  
   FROM CTE_TRANS_CALC 
   UNION ALL
   SELECT  brand_id, business_date, rest_id, fn_measure_id, source_system_name, sales_usd_amount, sales_count -- Add Gift Card
   FROM CTE_TRANSLINE
   UNION ALL
   SELECT  brand_id, business_date, rest_id, '43', source_system_name, sales_usd_amount * -1 , sales_count     -- Subtract Gift card from gross food sales (derived_gross_amt - gift card)
   FROM CTE_TRANSLINE where fn_measure_id = 26
  ) 
  SELECT
  'sonic' AS brand_id, cisc.business_date AS business_date, cisc.rest_id AS rest_id,
  CASE WHEN source_system_name ='infor' THEN '5'
       WHEN source_system_name ='micros' THEN '6' ELSE '0'
  END AS fn_system_id,
  cisc.fn_measure_id AS fn_measure_id, 'N/A' AS gl_account_code, 'N/A' AS gl_cost_ctr,
  SUM(cisc.sales_usd_amount) AS sale_usd_amount,
  SUM(cisc.sales_usd_amount) AS sale_amount,
  SUM(cisc.sales_count) AS sale_count,
  'USA' AS country_code,
  'USD' AS currency_code,
  cisc.source_system_name AS source_system_name,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS load_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS load_dttm,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS update_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS update_dttm
  FROM MASTER_TXNS cisc
  GROUP BY cisc.source_system_name, cisc.business_date, cisc.rest_id, cisc.fn_measure_id
));
MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
 SELECT
 brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
 sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
 FROM  "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_SDI_POS_INFOR_MICROS_TEMP 
) pbs
ON fdrm.business_date = pbs.business_date 
AND fdrm.brand_id = pbs.brand_id
AND fdrm.fn_system_id = pbs.fn_system_id
AND fdrm.fn_measure_id = pbs.fn_measure_id 
AND fdrm.source_system_name = pbs.source_system_name
AND fdrm.gl_account_code = pbs.gl_account_code 
AND fdrm.gl_cost_ctr = pbs.gl_cost_ctr 
AND fdrm.rest_id = pbs.rest_id
WHEN MATCHED THEN
UPDATE SET
fdrm.sale_usd_amount = pbs.sale_usd_amount,
fdrm.sale_amount = pbs.sale_amount,
fdrm.country_code = pbs.country_code,
fdrm.currency_code = pbs.currency_code,
fdrm.source_system_name = pbs.source_system_name,
fdrm.update_id = pbs.update_id,
fdrm.update_dttm = pbs.update_dttm 
WHEN NOT MATCHED THEN
INSERT
(
  brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
)
VALUES
(
  pbs.brand_id, pbs.business_date, pbs.rest_id, pbs.fn_system_id, pbs.fn_measure_id, pbs.gl_account_code,
  pbs.gl_cost_ctr, pbs.sale_usd_amount, pbs.sale_amount, pbs.country_code, pbs.currency_code, pbs.source_system_name,
  pbs.load_id, pbs.load_dttm, pbs.update_id, pbs.update_dttm
);