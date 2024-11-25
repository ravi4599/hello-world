create or replace view IDH_{{params.env}}.D_FINANCE.DAILY_PETTY_CASH_ARBYS(
	BRAND_ID,
	REST_ID,
	BUSINESS_DATE,
	MEASURE_NAME,
	MEASURE_AMT
) COMMENT='Daily Petty Cash Arbys show the different sub categories of a restaurants daily Paid In/Paid Outs. '
 as
SELECT
    brand_id,
    rest_id,
    business_date,
    CASE
        WHEN paid_in_out_petty_account_id = '640221120' THEN 'decrease_safe_amt'
        WHEN paid_in_out_petty_account_id = '640221121' THEN 'increase_safe_amt'
    END AS measure_name,
    SUM(paid_in_out_amt) AS measure_amt
FROM
     IDS_GOLD.TXN_BV.PAID_IN_OUT_ACTIVITY_BV t
    -- IDS_UAT.TXN_BV.PAID_IN_OUT_ACTIVITY_BV t
WHERE
    t.brand_id = 'arbys'
    -- AND business_date BETWEEN DATEADD(day, -120, CURRENT_DATE) AND CURRENT_DATE
    AND paid_in_out_petty_account_id in ('640221120', '640221121')
GROUP BY
   brand_id,
   rest_id,    
   business_date,
   measure_name
;