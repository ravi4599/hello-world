USE WAREHOUSE {{params.warehouse}};


--SET start_load_dt = '20230401';
--SET end_load_dt = '20230419';
SET start_load_dt = '{{params.load_start_dt}}';
SET end_load_dt = '{{params.load_end_dt}}';

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='bww' and FN_SYSTEM_ID=12 ;

--MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
--USING
--(

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_NBO_Temp_table AS
SELECT  
brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM 
(

 WITH FILTERED_RDS_RECORDS AS
    (
    SELECT
     sitenumber, storeid, salesdate, salestax, overshort, foodsales, othersales, wineliquorsales, bevsales, beersales,
     giftcardsold, donations, takeout, salesdelivery, alcoholcompsgaming,foodcompsgaming, complementarymeals, discounts,
     paidouts1, paidouts2, amex, discover, mastercard, visacard, giftcardredeemed, otherdeposits, housecharge,
     employeetips, cashdeposits, netsales
    FROM
     (
     SELECT
     t.sitenumber, LPAD(t.storeid, 5, 0) AS storeid, t.salesdate, t.salestax, t.overshort, t.foodsales,
     t.othersales, t.wineliquorsales, t.bevsales, t.beersales, t.giftcardsold, t.donations, t.takeout,
     t.salesdelivery, t.alcoholcompsgaming, t.foodcompsgaming, t.complementarymeals, t.discounts,
     t.paidouts1, t.paidouts2, t.amex, t.discover, t.mastercard,
     CASE WHEN LOWER(r.country_name) = 'ca' THEN t.visacard_ca ELSE t.visacard END AS visacard,
     t.giftcardredeemed, t.otherdeposits, t.housecharge, t.employeetips, t.cashdeposits, t.netsales,
  	 DENSE_RANK() OVER (PARTITION BY salesdate ORDER BY filedate DESC, folderdate DESC, loaddatetime DESC) AS dense_rank
     FROM "RDS_{{params.source_env}}"."BWW_BV"."NBO_ST_REGISTER_BV" t
  	  INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and LPAD(t.storeid,5,0) = LPAD(lo.rest_id,5,0)  AND t.salesdate between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
      inner join (  Select rest_id,Max(country_name) as country_name from "IDH_{{params.source_env}}"."D_LOC"."REST_SCD"  where brand_id = 'bww'  group by rest_id  ) r on lo.rest_id = r.rest_id
     Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and LPAD(t.storeid,5,0) = LPAD(lc.rest_id,5,0)  AND t.salesdate = lc.TEMP_CLOSE_DATE
      WHERE  (t.loaddatetime BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')
         OR
        REPLACE(t.salesdate, '"') BETWEEN TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD'))
     and   lc.TEMP_CLOSE_DATE is null
     ) WHERE dense_rank = 1
    ),
   PIVOT_CTE AS
   (
     SELECT fr.sitenumber, fr.storeid, fr.salesdate,
     fr.total_sales * COALESCE (mp.value_adjustment_amount,1) AS total_sales, mp.fn_measure_id, mp.fn_system_id
     FROM
        (
          SELECT
          sitenumber, storeid, salesdate, total_sales, description
          FROM FILTERED_RDS_RECORDS
          UNPIVOT(total_sales FOR description IN (salestax, overshort, foodsales, othersales, wineliquorsales, bevsales,
          beersales, giftcardsold, donations, takeout, salesdelivery, alcoholcompsgaming, foodcompsgaming,
          complementarymeals, discounts, paidouts1, paidouts2, amex, discover, mastercard, visacard, giftcardredeemed,
          otherdeposits, housecharge, employeetips, cashdeposits, netsales))
        ORDER BY sitenumber
        ) fr
        JOIN "IDS_{{params.target_env}}".TXN.FN_SYSTEM_TO_MEASURE_REFERENCE mp ON mp.source_sales_system_measure_text = fr.description AND mp.fn_system_id = '12'
   )
  SELECT
  'bww'                                                  AS brand_id,
  TO_DATE(LPAD(salesdate,10), 'YYYY-MM-DD')              AS business_date,
  LPAD(storeid,5,0)                                      AS rest_id,
  fn_system_id                                           AS fn_system_id,
  fn_measure_id                                          AS fn_measure_id,
  'N/A'                                       	       	 AS gl_account_code,
  'N/A'                                                  AS gl_cost_ctr,
  SUM(total_sales)                                       AS sale_usd_amount,
  SUM(total_sales)                                       AS sale_amount,
  'USA'                                                  AS country_code,
  'USD'                                                  AS currency_code,
  'nbo'                                                  AS source_system_name,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::string   AS load_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::string            AS load_dttm,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'))::string   AS update_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)::string            AS update_dttm
  FROM PIVOT_CTE
  GROUP BY TO_DATE(LPAD(salesdate,10), 'YYYY-MM-DD'), LPAD(storeid,5,0), fn_system_id, fn_measure_id
) nb;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  Select * from "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_NBO_Temp_table
) AS nbo

ON   
fdrm.business_date = nbo.business_date
AND fdrm.brand_id = nbo.brand_id
AND fdrm.fn_system_id = nbo.fn_system_id
AND fdrm.fn_measure_id = nbo.fn_measure_id
AND fdrm.rest_id = nbo.rest_id
WHEN MATCHED THEN
UPDATE SET
fdrm.sale_usd_amount     =  nbo.sale_usd_amount,
fdrm.sale_amount         =  nbo.sale_amount,
fdrm.update_id           =  nbo.update_id,
fdrm.source_system_name  =  nbo.source_system_name,
fdrm.update_dttm         =  nbo.update_dttm
WHEN NOT MATCHED THEN
INSERT
(
  brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
  sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
)
VALUES
(
  nbo.brand_id, nbo.business_date, nbo.rest_id, nbo.fn_system_id, nbo.fn_measure_id, nbo.gl_account_code,
  nbo.gl_cost_ctr, nbo.sale_usd_amount, nbo.sale_amount, nbo.country_code, nbo.currency_code, nbo.source_system_name,
  nbo.load_id, nbo.load_dttm, nbo.update_id, nbo.update_dttm
);