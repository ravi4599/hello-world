USE WAREHOUSE {{params.warehouse}};

SET start_load_dt = '{{params.load_start_dt}}';        ---start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';            --end_load_dt from Airflow Parameters

DELETE FROM "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE"
WHERE (business_date BETWEEN TO_DATE($start_load_dt, 'YYYYMMDD') AND TO_DATE($end_load_dt, 'YYYYMMDD'))
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND brand_id = 'dnkn' and fn_system_id = 17;

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE_DUNKIN_SYMPHONY_TEMP_TABLE" AS
SELECT
brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM
(
WITH CTE_BUSINESS_DATE_DNKN AS
(
SELECT distinct business_date FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS_DNKN"
WHERE brand_id = 'dnkn' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
UNION
SELECT distinct business_date FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE_DNKN"
WHERE brand_id = 'dnkn' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
UNION
SELECT distinct business_date FROM  "IDS_{{params.source_env}}"."TXN_BV"."PAID_IN_OUT_ACTIVITY_BV"
WHERE brand_id ='dnkn' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
UNION
SELECT distinct calendar_dt AS business_date FROM "IDS_{{params.source_env}}"."INT_REF_BV"."DATE_DIM_BV"
WHERE calendar_dt BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
),
CTE_FALSE_PASS AS (
    SELECT DISTINCT Rest_id 
    FROM "IDH_{{params.source_env}}".D_LOC.REST_SCD 
    WHERE brand_id = 'dnkn' AND UPPER(city) = 'FALSE PASS'
),

FILTERED_RECORDS_DNKN AS
(
SELECT
  trans.order_id, trans.business_date, trans.rest_id, trans.check_nbr,
  trans.tax_amt, trans.brand_id, trans.source_system_name, trans.derived_gross_amt, trans.derived_discount_amt,
  trans.derived_net_amt, trans.misc_charge_amt, trans.third_party_delivery_charge_amt
FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS_DNKN" trans
INNER JOIN 
        "IDS_{{params.source_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo ON lo.brand_id = trans.brand_id AND trans.rest_id = lo.REST_ID AND trans.business_date between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
      --LEFT OUTER JOIN 
      --  "IDS_{{params.source_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc ON lc.brand_id = trans.brand_id AND trans.rest_id = lc.REST_ID AND trans.business_date = lc.TEMP_CLOSE_DATE  
      LEFT OUTER JOIN 
        CTE_FALSE_PASS FP ON trans.rest_id = FP.REST_ID 
  WHERE   
     -- lc.TEMP_CLOSE_DATE IS NULL AND 
     FP.Rest_id IS NULL AND  trans.brand_id='dnkn'
  AND trans.business_date IN (SELECT business_date FROM CTE_BUSINESS_DATE_DNKN)
),

CTE_TRANSLINE_DNKN AS
(
SELECT rest_id, business_date, fn_measure_id,
CASE WHEN fn_measure_id in ('26', '54','28')  THEN SUM(misc_charge_amt)
     WHEN fn_measure_id = '33' THEN SUM(derived_discount_amt)
     WHEN fn_measure_id = '23' THEN SUM(derived_gross_amt+delivery)
     WHEN fn_measure_id = '43' THEN SUM(derived_gross_amt+delivery)
     ELSE NULL
END AS sales_usd_amount,
COUNT(*) sales_count
FROM (
  SELECT
  t.rest_id, t.business_date, t.tax_amt, tl.derived_gross_amt, tl.derived_discount_amt, tl.misc_charge_amt,nvl(tl.third_party_delivery_charge_amt,0) as delivery,
       Case when tl.SOURCE_ITEM_ID IN (SELECT configuration_value FROM IDS_{{params.source_env}}."INT_REF"."TRANSFORMATION_CONFIGURATION" WHERE configuration_name = 'DUNKIN_CHARITY') AND tl.MISC_CHARGE_AMT <> 0 then 28 --Donations
	        WHEN (tl.item_type_code ilike '%Gift Card%'  OR tl.item_type_code ilike '%GC Void%'  ) THEN 26 --gc sold
            WHEN (tl.item_type_code ilike 'Bag Deposit%' OR tl.item_type_code iLIKE 'Bottle Dep%')  THEN 54 -- Bag Deposit
            WHEN (t2.MAJOR_GROUP_NUMBER = '1005')  THEN 23 --Other Sales
            WHEN (t2.MAJOR_GROUP_NUMBER != '1005')  THEN 43 --Gross Food Sales
  ELSE 0 END AS fn_measure_id
  FROM FILTERED_RECORDS_DNKN t
  INNER JOIN "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE_DNKN" tl ON t.brand_id = tl.brand_id AND
  t.rest_id = tl.rest_id AND t.business_date = tl.business_date AND t.order_id = tl.order_id AND tl.brand_id='dnkn'
  LEFT OUTER JOIN RDS_{{params.source_env}}.DUN_BV.BR_MENUITEMCODE_DEDUP_BV t2 ON t2.MENU_ITEM_NUMBER = tl.SOURCE_ITEM_PLU_ID AND tl.brand_id = t2.brand_id
    ) h
GROUP BY rest_id, business_date, fn_measure_id
),

CTE_TENDER_DNKN AS
(SELECT FN_measure_id ,rest_id, business_date,sum(PAYMENT_AMT) sales_usd_amount, COUNT(*) sales_count FROM (SELECT pos_cdtl.rest_id, business_date,PAYMENT_AMT, 
CASE WHEN CARD_TYPE IN ( 'VISA' ,'VISA - KIOSK' ,'CLOVER GO' ) THEN 4
WHEN CARD_TYPE in ( 'MASTERCARD' ,'MASTERCARD - KIOSK' ) THEN 5
WHEN CARD_TYPE in ( 'AMERICAN EXPRESS' ,'AMERICAN EXPRESS - KIOSK' ) THEN 6
WHEN CARD_TYPE in ( 'DISCOVER' ,'DISCOVER - KIOSK' ) THEN 7
WHEN CARD_TYPE = 'GRUBHUB TENDER' THEN 12
WHEN CARD_TYPE = 'UBER EATS TENDER' THEN 10
WHEN CARD_TYPE = 'DOORDASH TENDER' THEN 11
WHEN CARD_TYPE in ( 'GIFT CARD REDEEM' ,'GIFT CARD REDEMPTION' ,'GIFT CARD REFUND' ,'GIFT CERTIFICATE' ,'GIFT OF JOY' ,'KIOSK GIFT CARD' ) THEN 8
WHEN CARD_TYPE IN ( 'CASH' ,'KIOSK CASH' ) THEN 3
ELSE 29 END AS FN_measure_id 
FROM "IDH_{{params.source_env}}".D_TRANS.TRANS_PAYMENT_DNKN pos_cdtl
 INNER JOIN "IDS_{{params.source_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo 
 ON lo.brand_id = 'dnkn' AND  pos_cdtl.REST_ID = lo.REST_ID 
 AND business_date between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
      --LEFT OUTER JOIN 
       -- "IDS_{{params.source_env}}"".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc ON lc.brand_id = 'dnkn' AND pos_cdtl.profit_center = lc.REST_ID AND business_date1 = lc.TEMP_CLOSE_DATE  
 LEFT OUTER JOIN  CTE_FALSE_PASS FP ON pos_cdtl.rest_id = FP.REST_ID 
 WHERE  
      --lc.TEMP_CLOSE_DATE IS NULL AND 
      FP.Rest_id IS NULL 
        AND business_date in (SELECT business_date FROM CTE_BUSINESS_DATE_DNKN)
) T 
GROUP BY FN_measure_id,rest_id,business_date
),

CTE_TRANS_DNKN AS
(
  SELECT rest_id, business_date, fn_measure_id, COUNT(rest_id) AS sales_count,
    SUM(CASE WHEN fn_measure_id IN (2) THEN tax_amt                   --Tax
            WHEN fn_measure_id IN (33) THEN derived_discount_amt      --Discount
        ELSE (derived_gross_amt + delivery) END                       --Gross Sales
       ) AS  sales_usd_amount
  FROM
  (
    SELECT
    rest_id, business_date,tax_amt, derived_gross_amt, derived_discount_amt, derived_net_amt, delivery,
    CASE WHEN fn_measure_id2 = 991              THEN 2  --Tax
        WHEN fn_measure_id2 = 992               THEN 33 --Discount
        --WHEN fn_measure_id2 = 993               THEN 56 --Gross Sales
    ELSE 0 END AS fn_measure_id
    FROM
    (
      SELECT
        t.rest_id, t.business_date,tax_amt, derived_gross_amt, derived_discount_amt, derived_net_amt, nvl(third_party_delivery_charge_amt,0) as delivery,
        r.fn_measure_id2
        FROM FILTERED_RECORDS_DNKN t
        INNER JOIN (SELECT         991 AS fn_measure_id2  -- Tax
                  UNION ALL SELECT 992 AS fn_measure_id2  -- Discount
                  --UNION ALL SELECT 993 AS fn_measure_id2  -- Gross Sales
                   ) r
    )
  )
  GROUP BY rest_id, business_date, fn_measure_id
),

CTE_PAID_OUT_DNKN AS            --This CTE is being created for Measures # Paid Out and Cash deposit
(
  SELECT pc.rest_id, pc.business_date, fn_measure_id, COUNT(rest_id) AS sales_count,
  SUM(CASE WHEN fn_measure_id IN (17) THEN pc.paid_in_out_amt       --Paid In/Out
           WHEN fn_measure_id IN (3) THEN pc.paid_in_out_amt * -1   --Cash Deposit
      ELSE 0 END
      ) AS  sales_usd_amount
  FROM
  (
    SELECT
    p.rest_id, p.business_date, p.paid_in_out_amt, 
    CASE WHEN fn_measure_id2 = 1              THEN 17 --Paid In/out
        WHEN fn_measure_id2 = 2               THEN 3  --Cash Deposit
    ELSE 0 END AS fn_measure_id
    FROM
    (
      SELECT
      t.rest_id, t.business_date, COALESCE(t.paid_in_out_amt,0) * -1 AS paid_in_out_amt
      FROM  "IDS_{{params.source_env}}"."TXN_BV"."PAID_IN_OUT_ACTIVITY_BV" t
      INNER JOIN 
        "IDS_{{params.source_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo ON lo.brand_id = t.brand_id AND t.rest_id = lo.REST_ID AND t.business_date between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
      --LEFT OUTER JOIN 
      --  "IDS_{{params.source_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc ON lc.brand_id = t.brand_id AND t.rest_id = lc.REST_ID AND t.business_date = lc.TEMP_CLOSE_DATE  
      LEFT OUTER JOIN 
        CTE_FALSE_PASS FP ON t.rest_id = FP.REST_ID 
  WHERE   
     -- lc.TEMP_CLOSE_DATE IS NULL AND 
     FP.Rest_id IS NULL AND  t.brand_id='dnkn'
      AND t.business_date  IN (SELECT business_date FROM CTE_BUSINESS_DATE_DNKN)
    ) p 
    INNER JOIN (SELECT         1 AS fn_measure_id2  -- Paid In out
              UNION ALL SELECT 2 AS fn_measure_id2  -- Cash deposit (need to subtract paid in out from cash deposit)
              ) r
  ) pc
  GROUP BY  rest_id, business_date, fn_measure_id
),

MASTER_TXNS_DNKN AS   --This CTE collates info from all above CTE's and prepares the dataset to merge with the existing data.
(
  SELECT business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count FROM CTE_TENDER_DNKN where fn_measure_id <> 0
  UNION ALL
  SELECT business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count FROM CTE_TRANSLINE_DNKN where fn_measure_id <> 0
  UNION ALL
  SELECT business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count FROM CTE_TRANS_DNKN where fn_measure_id <> 0
  UNION ALL
  SELECT business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count FROM CTE_PAID_OUT_DNKN where fn_measure_id <> 0
)

SELECT
'dnkn' AS brand_id,
cisc.business_date AS business_date,
cisc.rest_id AS rest_id,
'17' AS fn_system_id,
cisc.fn_measure_id  AS fn_measure_id,
'N/A' AS gl_account_code,
'N/A' AS gl_cost_ctr,
SUM(cisc.sales_usd_amount) AS sale_usd_amount,
SUM(cisc.sales_usd_amount) AS sale_amount,
SUM(cisc.sales_count) AS sale_count,
'USA' AS country_code,
'USD' AS currency_code,
'symphony' AS source_system_name,
TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS load_id,
TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS load_dttm,
TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS update_id,
TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS update_dttm
FROM MASTER_TXNS_DNKN cisc
GROUP BY cisc.business_date, cisc.rest_id, cisc.fn_measure_id
) pb;


MERGE INTO "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE" fdrm
USING
(
  SELECT * FROM "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE_DUNKIN_SYMPHONY_TEMP_TABLE"
) AS pbs
ON fdrm.business_date = pbs.business_date
AND fdrm.brand_id = pbs.brand_id
AND fdrm.fn_system_id = pbs.fn_system_id
AND fdrm.fn_measure_id = pbs.fn_measure_id
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