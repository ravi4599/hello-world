USE WAREHOUSE {{params.warehouse}};
--SET start_load_dt = '20240301';
--SET end_load_dt   = '20240301';
SET start_load_dt = '{{params.load_start_dt}}';
SET end_load_dt = '{{params.load_end_dt}}';

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='arbys' and FN_SYSTEM_ID=1 ;

--MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
--USING
--(
CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Arbys_Parbrinks_Temp_table AS
SELECT  
brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM
(
   WITH CTE_BUSINESS_DATE AS
(
    SELECT DISTINCT business_date FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS" 
    WHERE brand_id ='arbys' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION 
    SELECT DISTINCT business_date FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE"
    WHERE brand_id ='arbys' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION
    SELECT  DISTINCT business_date FROM "IDS_{{params.source_env}}".TXN_BV.PAID_IN_OUT_ACTIVITY_BV 
    WHERE  brand_id ='arbys' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
    UNION
    SELECT DISTINCT CALENDAR_DT as business_date FROM "IDS_{{params.source_env}}"."INT_REF_BV"."DATE_DIM_BV" 
    WHERE CALENDAR_DT BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
),
FILTERED_RECORDS AS    -- This CTE brings the data from UDP sales source table(TRANS) for only required stores and business_dates
( 
    SELECT
    t.brand_id,t.rest_id, t.business_date, t.tax_exempt_ind, t.order_id,
    t.derived_net_amt, t.tax_amt, t.misc_charge_amt, t.channel_id, t.derived_gross_amt, t.derived_discount_amt
    FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS" t
    INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = t.brand_id and t.rest_id = lo.REST_ID AND t.business_date between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
    --LEFT OUTER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = t.brand_id and t.rest_id = lc.REST_ID AND t.business_date = lc.TEMP_CLOSE_DATE  
    WHERE t.brand_id = 'arbys' 
    --and lc.TEMP_CLOSE_DATE is null
    AND t.business_date IN (SELECT business_date FROM CTE_BUSINESS_DATE)   
),
CTE_TRANSLINE AS   --This CTE brings the data from UDP sales source table(TRANSLINE) and here we are calculating fn_measure_ids
(
    SELECT
    tl.brand_id, tl.rest_id , tl.business_date,
    CASE WHEN UPPER(tl.item_type_code) = 'GIFTCARD' THEN 26 -- gc sold
         WHEN UPPER(tl.source_item_desc) LIKE '%CHARITY%' AND r.rn = 2 THEN 1 -- Net Sales
         WHEN UPPER(tl.source_item_desc) LIKE '%CHARITY%' THEN 28 -- donations
         WHEN UPPER(tl.source_item_desc) LIKE '%DONATION%' THEN 28 -- donations
    END AS fn_measure_id,
    SUM( CASE WHEN UPPER(tl.item_type_code) = 'GIFTCARD' THEN tl.misc_charge_amt
              WHEN UPPER(tl.source_item_desc) LIKE '%CHARITY%' AND r.rn = 2 THEN (tl.derived_gross_amt) * -1
              WHEN UPPER(tl.source_item_desc) LIKE '%CHARITY%' THEN tl.derived_gross_amt
              WHEN UPPER(tl.source_item_desc) LIKE '%DONATION%' THEN  tl.derived_gross_amt
         END ) AS sales_usd_amount,
    COUNT(tl.rest_id) AS sales_count
    FROM FILTERED_RECORDS t
    JOIN (
         SELECT brand_id, rest_id, business_date, order_line_type, item_type_code, mdm_item_id,
        order_id, derived_gross_amt, derived_discount_amt, misc_charge_amt, tax_amt, discount_code,
        source_discount_type, source_item_plu_id, source_item_desc
        FROM IDH_{{params.source_env}}.D_TRANS.TRANS_LINE tline
         WHERE tline.brand_id ='arbys' AND tline.business_date in (SELECT business_date FROM CTE_BUSINESS_DATE)
         AND COALESCE(tline.void_ind,'N') = 'N' AND COALESCE (tline.deleted_ind,'N') = 'N'
        ) tl ON t.brand_id = tl.brand_id AND t.rest_id = tl.rest_id
               AND t.business_date = tl.business_date AND t.order_id = tl.order_id
    INNER JOIN (SELECT 1 AS rn UNION ALL SELECT 2 AS rn) r ON (UPPER(tl.source_item_desc) LIKE '%CHARITY%'
               AND r.rn = 2) OR r.rn = 1
    WHERE tl.brand_id='arbys' AND ((UPPER(tl.order_line_type)='MISC' and UPPER(tl.item_type_code) = 'GIFTCARD')
    OR UPPER(tl.source_item_desc) LIKE '%CHARITY%' OR UPPER(tl.source_item_desc) LIKE '%DONATION%')
    GROUP BY tl.brand_id, tl.rest_id, tl.business_date, fn_measure_id
),
CTE_PAID_OUT AS    -- This CTE is for calculating Paid_In_Out (Cash flow in and out from the cash counter)
(
    SELECT	t.brand_id,t.rest_id, t.business_date ,SUM( (CASE WHEN t.paid_in_out_type = '0' THEN -1	ELSE 1  END) * t.paid_in_out_amt) AS sales_usd_amount
    , '17' AS fn_measure_id ,  COUNT(*) AS sales_count 	FROM  "IDS_{{params.source_env}}".TXN_BV.PAID_IN_OUT_ACTIVITY_BV t
    INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = t.brand_id and t.rest_id = lo.REST_ID AND t.business_date between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
    --Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = t.brand_id and t.rest_id = lc.REST_ID AND t.business_date = lc.TEMP_CLOSE_DATE  
    WHERE t.brand_id = 'arbys'-- and lc.TEMP_CLOSE_DATE is null
    AND ( ( t.paid_in_out_type = '1' AND t.paid_in_out_petty_account_id NOT IN ('2') )            -- Paid Out
    OR    ( t.paid_in_out_type = '0' AND t.paid_in_out_petty_account_id NOT IN ('1') )  )         -- Paid In
    AND t.business_date  IN (SELECT business_date FROM CTE_BUSINESS_DATE)
    GROUP BY t.brand_id,t.rest_id,t.business_date,fn_measure_id
),
CTE_CREDIT AS   --This CTE is find the credit related parameters(Net Sales, Sales Tax etc) measure_ids
(
    SELECT
    1 AS fn_measure_id -- Net Sales
    UNION ALL SELECT 2  AS fn_measure_id -- Sales Tax
    UNION ALL SELECT 15  AS fn_measure_id -- Tax Exempt Sales
),
CTE_DEBITS AS   --This CTE is find the debit related parameters(AMEX, MASTERCARD, VISA etc) measure_ids
(
    SELECT
    t.brand_id, t.business_date, t.rest_id,  t.fn_measure_id, SUM(t.order_amt) sales_usd_amount,
    COUNT(*) sales_count        
    FROM
    (
      SELECT
      t.brand_id ,t.rest_id, t.business_date, t.tax_exempt_ind,
     CASE when upper(p.payment_type) = 'CREDITCARD'      and UPPER(p.card_type) = 'AMEX'	then 	6
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'AMEX - EXTERNAL TENDER'	then 	6
 when upper(p.payment_type) = 'CREDITCARD'      and UPPER(p.card_type) = 'DISCOVER'	then 	7
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'DISCOVER - EXTERNAL TENDER'	then 	7
 when upper(p.payment_type) = 'CREDITCARD'      and UPPER(p.card_type) = 'VISA'	then 	4
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'VISA - EXTERNAL TENDER'	then 	4
 when upper(p.payment_type) = 'CREDITCARD'      and UPPER(p.card_type) = 'MASTERCARD'	then 	5
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'MASTERCARD - EXTERNAL TENDER'	then 	5
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'ONLINE PAYMENT'	then 	29
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'UBER EATS'	then 	10
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'GRUBHUB'	then 	12
 when upper(p.payment_type) = 'EXTERNAL'        and UPPER(p.card_type) = 'DOORDASH'	then 	11
 when upper(p.payment_type) = 'GIFTCARD'        and UPPER(p.card_type) = 'GIFT CARD'	then 	8
 when upper(p.payment_type) = 'GIFTCERTIFICATE' and UPPER(p.card_type) = 'MEAL CARD'	then 	29
 when upper(p.payment_type) = 'CASH'			then 	3 -- Cash Deposits
           --WHEN UPPER(p.card_type) = 'CREDITCARD' THEN 31  -- Other CC
           ELSE 29   -- Other Deposits
      END AS fn_measure_id,
      p.order_amt AS order_amt
      FROM FILTERED_RECORDS t
      LEFT OUTER JOIN "IDH_{{params.source_env}}"."D_REF"."CHANNEL" c ON t.channel_id = c.channel_id
	  LEFT OUTER JOIN "IDH_{{params.source_env}}"."D_TRANS"."TRANS_PAYMENT" p ON t.brand_id = p.brand_id
      AND t.rest_id = p.rest_id AND t.business_date = p.business_date AND t.order_id = p.order_id
      AND COALESCE(p.is_void_ind,'N') = 'N'
    ) t
    GROUP BY t.brand_id, t.rest_id, t.business_date, t.fn_measure_id
),
CTE_CREDITS AS   --This CTE calculated sales_usd_amount for the credit related parameters(Net Sales, Sales Tax etc) measure_ids
(
    SELECT
    t.brand_id , t.business_date, t.rest_id , credits.fn_measure_id,
    COALESCE(SUM(CASE WHEN credits.fn_measure_id = 1 THEN t.derived_gross_amt-t.derived_discount_amt
                      WHEN credits.fn_measure_id = 2 THEN t.tax_amt
                      WHEN credits.fn_measure_id = 15 AND t.tax_exempt_ind = 'Y' THEN t.derived_net_amt
                 END), 0) sales_usd_amount , COUNT(t.rest_id ) sales_count
    FROM CTE_CREDIT credits
    JOIN FILTERED_RECORDS T
    WHERE t.brand_id = 'arbys' AND NOT (credits.fn_measure_id = 15 AND COALESCE(t.tax_exempt_ind,'N') = 'N')
    GROUP BY t.brand_id ,t.rest_id, t.business_date , credits.fn_measure_id
),
CTE_ITEM_REV_CTR AS
(
Select 
IC.brand_id, IC.rev_ctr_item_id, IC.rev_ctr_id, IC.source_item_id, IC.source_system_name, IC.source_item_desc
,RC.source_rev_ctr_id,RC.rev_ctr_desc,
TC.domain_name,TC.transformation_name,TC.configuration_name,TC.configuration_key_label,TC.configuration_value,
TC.configration_operator_type,TC.configuration_start_date,TC.operaton_return_value,TC.operator_return_type,	
TC.configuration_end_date
from  "IDS_{{params.target_env}}"."INT_REF"."REV_CTR_ITEM" IC 
Inner join  "IDS_{{params.target_env}}"."INT_REF"."REV_CTR" RC on IC.REV_CTR_ID = RC.REV_CTR_ID
Inner join  "IDS_{{params.target_env}}"."INT_REF"."TRANSFORMATION_CONFIGURATION" TC on RC.SOURCE_REV_CTR_ID = TC.CONFIGURATION_VALUE AND CONFIGURATION_NAME='Other Sales'
),
CTE_OTHER_SALES AS   
(
    SELECT 
    tl.brand_id, tl.rest_id , tl.business_date,
    23 AS fn_measure_id,
    SUM(tl.derived_gross_amt - tl.derived_discount_amt) AS sales_usd_amount,
    COUNT(tl.rest_id) AS sales_count
	FROM FILTERED_RECORDS t
	INNER JOIN 
    (    
        SELECT tline.brand_id, tline.rest_id, tline.business_date, tline.order_line_type, tline.item_type_code, tline.mdm_item_id,
        tline.order_id, tline.derived_gross_amt, tline.derived_discount_amt, tline.misc_charge_amt, tline.tax_amt, tline.discount_code,
        tline.source_discount_type, tline.source_item_plu_id, tline.source_item_desc
		    FROM IDH_{{params.source_env}}.D_TRANS.TRANS_LINE tline        
        Inner join CTE_ITEM_REV_CTR IRC ON IRC.source_item_id = tline.source_item_id
         WHERE tline.brand_id ='arbys' and tline.business_date in (SELECT business_date FROM CTE_BUSINESS_DATE)
		 AND COALESCE(tline.void_ind,'N') = 'N' AND COALESCE (tline.deleted_ind,'N') = 'N'         
	) tl ON 
    t.brand_id = tl.brand_id AND t.rest_id = tl.rest_id AND t.business_date = tl.business_date  AND t.order_id = tl.order_id
	WHERE t.brand_id='arbys' 
    GROUP BY tl.brand_id, tl.rest_id, tl.business_date, fn_measure_id
),
MASTER_TXNS AS   --This CTE collates info from all above CTE's and prepares the dataset to merge with the existing data.
(
    SELECT
    brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count 
    FROM CTE_DEBITS 
    UNION ALL
    SELECT
    brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count 
    FROM CTE_CREDITS 
    UNION ALL
    SELECT
    brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count 
    FROM CTE_TRANSLINE 
    UNION ALL
    SELECT
    brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count 
    FROM CTE_PAID_OUT
    UNION ALL
    SELECT
    brand_id, business_date, rest_id, 1 as fn_measure_id, sales_usd_amount * -1, sales_count 
    FROM CTE_OTHER_SALES  
    UNION ALL
    SELECT
    brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount , sales_count 
    FROM CTE_OTHER_SALES  
)
SELECT
'arbys' AS brand_id, 
cisc.business_date AS business_date, 
cisc.rest_id AS rest_id,
'1' AS fn_system_id,
cisc.fn_measure_id  AS fn_measure_id, 
'N/A' AS gl_account_code, 
'N/A' AS gl_cost_ctr,
SUM(cisc.sales_usd_amount) AS sale_usd_amount, 
SUM(cisc.sales_usd_amount) AS sale_amount,
SUM(cisc.sales_count) AS sale_count, 
'USA'  AS country_code, 
'USD' AS currency_code,
'parbrinks' AS source_system_name,
TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS load_id,
TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS load_dttm,
TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS update_id,
TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS update_dttm
FROM MASTER_TXNS cisc
GROUP BY cisc.business_date, cisc.rest_id, cisc.fn_measure_id
) pb;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  Select * from "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Arbys_Parbrinks_Temp_table
) AS pbs

ON fdrm.business_date = pbs.business_date
AND fdrm.brand_id = 'arbys' 
AND fdrm.fn_system_id = '1' 
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


