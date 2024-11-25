USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20230801';
--SET end_load_dt   = '20230910';

SET start_load_dt = '{{params.load_start_dt}}'; --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}'; --end_load_dt from Airflow Parameters

DELETE FROM "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) AND DAYNAME(TO_DATE(CURRENT_DATE()))='Sun'
AND BRAND_ID='jj' and FN_SYSTEM_ID IN (14) ;


CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE_JJ_POS_PDQ_TEMP" AS
(SELECT  brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM
 (
   WITH FILTERED_RECORDS AS
   ( 
    SELECT t.brand_id, t.rest_id, t.business_date, t.tax_exempt_ind, t.void_ind, t.source_system_name, t.order_id,t.channel_id,
    t.derived_net_amt, t.source_net_amt, t.tax_amt, t.misc_charge_amt, t.derived_gross_amt, t.gratuity_amt, t.derived_discount_amt
    FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS" t
    INNER JOIN "IDH_{{params.source_env}}"."D_LOC"."REST_SCD" rs    
    ON t.rest_id = rs.rest_id AND t.brand_id = rs.brand_id AND rs.fran_ind = FALSE AND
    rs.rest_status_type IN ('Re-Open', 'Open') 
    AND t.business_date BETWEEN rs.effective_start_date AND rs.effective_end_date
    WHERE t.brand_id ='jj' AND t.void_ind = 'FALSE'
    AND (    ( t.update_dttm BETWEEN  (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD')) )
         OR  ( t.business_date BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD')) )
        )
   ) 
   
    ,CTE_BUSINESS_DATE AS -- --This CTE is being created for distinct business_date for performance issue
   (
    SELECT DISTINCT business_date FROM FILTERED_RECORDS
   )  
   
   ,CTE_CREDIT_NET AS  -- --This CTE is being created for calculating the netsale and derived_net_amt    
   (
    SELECT 995  AS fn_measure_id              
   )
  
   ,CTE_DEPOSIT AS -- --This CTE is being created for TRANS_LINE to get derived_net_amt with filter of Deposit(995)
   (
     SELECT tl.rest_id, tl.business_date, tl.brand_id, tl.source_system_name, t.fn_measure_id,
     COALESCE(SUM(CASE WHEN t.fn_measure_id = 995 THEN tl.derived_net_amt  --Calculating for netsale                      
                  END), 0) AS sales_usd_amount, 
     COUNT(*) sales_count
     FROM CTE_CREDIT_NET t 
     INNER JOIN   
     "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE" tl 
     ON tl.brand_id = 'jj' AND tl.void_ind = 'FALSE'
     AND tl.SOURCE_ITEM_ID = '995' 
     INNER JOIN "IDH_{{params.source_env}}"."D_LOC"."REST_SCD" rs
     ON tl.rest_id = rs.rest_id AND tl.brand_id = rs.brand_id AND rs.fran_ind = FALSE
     AND rs.rest_status_type IN ('Re-Open', 'Open' )
     AND tl.business_date BETWEEN rs.effective_start_date AND rs.effective_end_date
     WHERE tl.business_date IN (SELECT business_date FROM CTE_BUSINESS_DATE)  
     GROUP BY tl.rest_id, tl.business_date, tl.brand_id, tl.source_system_name, t.fn_measure_id
   )    
      
   ,CTE_NET_SALE AS      --This CTE is being created for TRANS for total net amount
   (
    SELECT fr.business_date, fr.rest_id,fr.brand_id, 1 as fn_measure_id,fr.source_system_name, SUM(fr.source_net_amt-fr.misc_charge_amt) AS net_amt, COUNT(*) AS sales_count
    from FILTERED_RECORDS fr
    JOIN CTE_CREDIT_NET AS ccn 
    where fr.void_ind = 'FALSE' AND fr.brand_id ='jj' 
    group by fr.business_date, fr.rest_id,fr.brand_id,fn_measure_id,fr.source_system_name
   )
     
   ,CTE_TRANSLINE AS   --This CTE is being created for TRANS_LINE for Calculating Discount,GC Sold and Comp Meals
   ( 
       SELECT brand_id, rest_id, business_date, fn_measure_id, source_system_name, SUM(sales_usd_amount) AS sales_usd_amount, COUNT(*) sales_count
       FROM 
     (
         SELECT t.brand_id, t.rest_id, t.business_date,t.source_system_name,
	            CASE WHEN UPPER(tl.item_type_code) = 'GIFTCARD'
			         THEN 26                                                                                                                     --Calculating GC Sold
                     WHEN tl.ORDER_LINE_TYPE = 'discount' AND tl.DISCOUNT_NAME NOT IN ('Manager Meal','Employee Meal','Employee Discount') 
			         THEN 33                                                                                                                     --Calculating Discount
			         WHEN tl.ORDER_LINE_TYPE = 'discount' AND tl.DISCOUNT_NAME IN ('Manager Meal','Employee Meal','Employee Discount')
			         THEN 39                                                                                                                     --Calculating Complemetary Meals
                ELSE 0
	            END AS fn_measure_id,
	            CASE WHEN UPPER(tl.item_type_code) = 'GIFTCARD'
			         THEN tl.misc_charge_amt
                     WHEN tl.ORDER_LINE_TYPE = 'discount' AND tl.DISCOUNT_NAME NOT IN ('Manager Meal','Employee Meal','Employee Discount')
			         THEN tl.DERIVED_DISCOUNT_AMT
			         WHEN tl.ORDER_LINE_TYPE = 'discount' AND tl.DISCOUNT_NAME IN ('Manager Meal','Employee Meal','Employee Discount')
			         THEN tl.DERIVED_DISCOUNT_AMT
                ELSE 0
	            END AS sales_usd_amount
         FROM FILTERED_RECORDS t  
	     LEFT OUTER JOIN "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE" tl
		 ON t.brand_id = tl.brand_id AND t.rest_id = tl.rest_id AND
		    t.business_date = tl.business_date AND t.order_id = tl.order_id	
         WHERE tl.brand_id ='jj' AND tl.void_ind = FALSE 
	     AND tl.business_date IN (SELECT business_date FROM CTE_BUSINESS_DATE) AND  fn_measure_id <> 0
     )
       GROUP BY brand_id, rest_id, business_date, fn_measure_id, source_system_name
    )     
       
   ,CTE_PAYMENT AS
   (
    SELECT
    brand_id, rest_id, business_date, fn_measure_id, source_system_name,
    COALESCE(SUM(payment_amt),0) sales_usd_amount, 
    COUNT(*) sales_count
    FROM 
     (
      SELECT t.brand_id, t.rest_id, t.business_date, t.source_system_name,
       CASE WHEN UPPER(p.card_type) = 'DOORDASH' THEN 11 --Calculating Doordash
            WHEN UPPER(p.card_type) = 'HOUSE ACCT' THEN 27 --Calculating Catering
            WHEN UPPER(p.card_type) = 'OTHER' THEN 10 --Calculating Ubereats          
            WHEN UPPER(p.card_type) = 'CREDIT' AND UPPER(p.card_issuer_name) = 'AMEX' THEN 6  --Calculating Amex
            WHEN UPPER(p.card_type) = 'CREDIT' AND UPPER(p.card_issuer_name) = 'VISA' THEN 4 --Calculating VisaCard
            WHEN UPPER(p.card_type) = 'CREDIT' AND UPPER(p.card_issuer_name) = 'MC' THEN 5 --Calculating MasterCard
            WHEN UPPER(p.card_type) = 'CREDIT' AND UPPER(p.card_issuer_name) = 'DISC' THEN 7 --Calculating Discover
            WHEN UPPER(p.card_type) = 'GIFT' AND t.misc_charge_amt = 0 THEN 8 --Calculating GC Redeemed
            ELSE 0 END AS fn_measure_id, --Calculating Other Deposit
	        p.payment_amt --Currently payment_amt is matching instead order_amt
      FROM FILTERED_RECORDS t
      INNER JOIN 
       (   
        SELECT
        brand_id, rest_id, business_date, payment_amt, order_amt, order_id, card_type, card_issuer_name,
        is_void_ind, payment_id, payment_auth_code, payment_first6_nbr, payment_last_4
        FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS_PAYMENT" pbd
        WHERE pbd.brand_id = 'jj' AND pbd.business_date IN (SELECT business_date FROM CTE_BUSINESS_DATE)    
       ) p     
      ON t.brand_id = p.brand_id AND t.rest_id = p.rest_id 
      AND t.business_date = p.business_date AND t.order_id = p.order_id
      AND p.is_void_ind = 0
      WHERE t.brand_id = 'jj' and fn_measure_id <> 0
     )
    GROUP BY brand_id, rest_id, business_date, fn_measure_id, source_system_name
   )
   
   ,CTE_TRANS_MEASEURE_ID AS
   (
    SELECT 2  AS fn_measure_id --SALES TAX 
    UNION ALL 
    SELECT 30  AS fn_measure_id --GRATUITY_AMT or EMPLOYEE TIPS       
   )
   
  ,CTE_TRANS_CALC AS
   (
    SELECT t.brand_id, t.business_date, t.rest_id, t.source_system_name, dc.fn_measure_id,
    COALESCE(SUM(CASE  WHEN dc.fn_measure_id = 2 THEN t.tax_amt --Calculating Employee Tip
                       WHEN dc.fn_measure_id = 30 THEN t.gratuity_amt --Calculating Employee Tip                                        
                       END),0) sales_usd_amount,
    COUNT(t.rest_id ) sales_count   
    FROM CTE_TRANS_MEASEURE_ID dc  
    JOIN 
     (
      SELECT
      brand_id, rest_id, business_date, tax_amt, gratuity_amt, source_net_amt,misc_charge_amt, derived_discount_amt,
      tax_exempt_ind, source_system_name, derived_net_amt
      FROM FILTERED_RECORDS
      ) t
    WHERE t.brand_id = 'jj'
    GROUP BY t.brand_id, t.rest_id, t.business_date, t.source_system_name, dc.fn_measure_id
   )
   
   ,MASTER_TXNS AS 
   (
    SELECT brand_id, business_date, rest_id, fn_measure_id, source_system_name, sales_usd_amount, sales_count  
    FROM CTE_TRANSLINE
    UNION ALL
    SELECT  brand_id, business_date, rest_id, fn_measure_id, source_system_name, sales_usd_amount, sales_count  
    FROM CTE_PAYMENT 
    UNION ALL
    SELECT  brand_id, business_date, rest_id, fn_measure_id, source_system_name, sales_usd_amount, sales_count 
    FROM CTE_TRANS_CALC
    UNION ALL
    SELECT brand_id, business_date, rest_id, 
    CASE WHEN fn_measure_id = 995 THEN 1           
    ELSE fn_measure_id END as fn_measure_id, 
    source_system_name,  -1 * sales_usd_amount AS sales_usd_amount, sales_count  --Calculating (netsales - derived net amt)
    FROM CTE_DEPOSIT 
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, source_system_name, net_amt AS sales_usd_amount, sales_count      --Calculating netsales
    FROM CTE_NET_SALE
   )
   
   SELECT
   cisc.brand_id, cisc.business_date AS business_date, cisc.rest_id AS rest_id,  
   sy.fn_system_id, cisc.fn_measure_id AS fn_measure_id, 
   'N/A' AS gl_account_code, 'N/A' AS gl_cost_ctr,
   SUM(cisc.sales_usd_amount) AS sale_usd_amount,
   SUM(cisc.sales_usd_amount) AS sale_amount,
   SUM(cisc.sales_count) AS sale_count,
   'USA' AS country_code,
   'USD' AS currency_code,
   'pdq' AS source_system_name,
   TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS load_id,
   TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS load_dttm,
   TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS update_id,
   TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS update_dttm
   FROM MASTER_TXNS cisc
   JOIN "IDS_{{params.target_env}}".TXN.FN_SYSTEM sy
   ON sy.system_name = cisc.source_system_name AND sy.fn_system_id = '14'   
   GROUP BY cisc.brand_id, cisc.source_system_name, cisc.business_date, cisc.rest_id, cisc.fn_measure_id, sy.fn_system_id
 )
);
MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
 SELECT
 brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
 sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
 FROM  "IDS_{{params.target_env}}"."TXN"."FN_DAILY_REV_MEASURE_JJ_POS_PDQ_TEMP"
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