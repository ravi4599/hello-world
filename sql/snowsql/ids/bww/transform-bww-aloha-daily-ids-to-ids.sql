USE WAREHOUSE {{params.warehouse}};

--SET start_load_dt = '20230301';
--SET end_load_dt = '20230530';
SET start_load_dt = '{{params.load_start_dt}}';        --start_load_dt from Airflow Parameters
SET end_load_dt = '{{params.load_end_dt}}';            --end_load_dt from Airflow Parameters

DELETE FROM IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE"
WHERE (BUSINESS_DATE BETWEEN  TO_DATE($start_load_dt,'YYYYMMDD') AND TO_DATE($end_load_dt,'YYYYMMDD')) 
AND DATEDIFF(day, TO_DATE($start_load_dt, 'YYYYMMDD'), TO_DATE($end_load_dt, 'YYYYMMDD')) > 10
AND BRAND_ID='bww' and FN_SYSTEM_ID=10 ;

CREATE OR REPLACE TEMPORARY TABLE "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Aloha_Temp_table AS
SELECT  
brand_id, business_date, rest_id, fn_system_id, fn_measure_id, gl_account_code, gl_cost_ctr, sale_usd_amount,
sale_amount, country_code, currency_code, source_system_name, load_id, load_dttm, update_id, update_dttm
FROM 
(
 WITH CTE_BUSINESS_DATE AS
  (
  SELECT DISTINCT business_date from "IDH_{{params.source_env}}"."D_TRANS"."TRANS" 
  WHERE brand_id ='bww' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
  UNION 
  SELECT DISTINCT business_date from "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE"
  WHERE brand_id ='bww' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
  UNION
  SELECT DISTINCT business_date FROM  "IDS_{{params.source_env}}".TXN_BV.PAID_IN_OUT_ACTIVITY_BV 
  WHERE  brand_id ='bww' AND update_dttm BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
  UNION
  SELECT CALENDAR_DT as business_date FROM "IDS_{{params.source_env}}"."INT_REF_BV"."DATE_DIM_BV"  
  WHERE CALENDAR_DT BETWEEN (TO_DATE($start_load_dt,'YYYYMMDD')) AND (TO_DATE($end_load_dt,'YYYYMMDD'))
  )

  ,FILTERED_RECORDS AS
  (
   SELECT
   t.brand_id, t.rest_id, t.business_date, t.tax_exempt_ind, t.order_id, t.channel_id, t.void_ind,
   t.gratuity_amt, t.surcharge_amt, t.derived_gross_amt, t.derived_discount_amt, t.source_payment_amt,
   t.derived_payment_amt, t.derived_net_amt, t.tax_amt, t.misc_charge_amt
   FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS" t
   INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and t.rest_id = lo.REST_ID AND t.business_date between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
   Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and t.rest_id = lc.REST_ID AND t.business_date = lc.TEMP_CLOSE_DATE   
   WHERE t.brand_id ='bww' AND t.void_ind = FALSE and lc.TEMP_CLOSE_DATE is null
   AND t.business_date IN (SELECT business_date FROM CTE_BUSINESS_DATE)
    )
  ,CTE_TRANSLINE AS       --This CTE is being created for Measures # GC Sold, Donations, Alcohol Comp Gaming, Food Comp Gaming, Discounts, Complementary Meals 
  (                      
   SELECT
   brand_id, rest_id, business_date, fn_measure_id, COUNT(rest_id) AS sales_count,
   SUM(CASE WHEN fn_measure_id IN ('26') THEN misc_charge_amt + derived_gross_amt     -- GC Sold - misc_charge_amt when order_line_type=MISC, derived_gross_amt when order_line_type=ITEM
      WHEN fn_measure_id IN ('28') THEN misc_charge_amt                               -- Donations
      WHEN fn_measure_id IN ('38') THEN derived_net_amt + misc_charge_amt             -- Sales Delivery (Delivery fee and service fee for 1PD first party delivery)
      WHEN fn_measure_id IN ('33','39','40','41') THEN derived_discount_amt 
      ELSE (derived_gross_amt - tax_amt)                                              -- All the sales categories, remove inclusive tax
   END) AS  sales_usd_amount
   FROM
   (
    SELECT
    t.brand_id, t.rest_id, t.business_date, tl.derived_gross_amt, tl.derived_net_amt,
    tl.derived_discount_amt, tl.tax_amt, tl.misc_charge_amt,
    CASE
    WHEN UPPER(tl.order_line_type) = 'ITEM' AND tl.source_item_desc = 'GC ACTIVATE'                             THEN 26 -- GC Sold coming in AS item
    WHEN UPPER(tl.order_line_type) = 'MISC' AND upper(source_item_desc) like '%DONATION%'                       THEN 28 -- Donations
    WHEN UPPER(tl.order_line_type) = 'MISC' AND UPPER(tl.item_type_code) IN ('GIFT CARD', 'GUEST RECOVERY GC')  THEN 26 -- GC Sold
    WHEN (upper(source_item_desc) like 'DELIVERY FEE%' or upper(source_item_desc) like 'SERVICE FEE%')          THEN 38 -- Sales Delivery (Delivery fee and service fee for 1PD first party delivery)
    ELSE 0
    END AS fn_measure_id
    FROM FILTERED_RECORDS t
    JOIN (
        SELECT channel_id, order_channel_name, fulfillment_channel_name, fulfillment_channel_hierarchy_l1
        FROM "IDH_{{params.source_env}}"."D_REF"."CHANNEL"
         ) c ON t.channel_id = c.channel_id
    LEFT OUTER JOIN (
        SELECT brand_id, rest_id, business_date, order_line_item_key, order_line_type, item_type_code, mdm_item_id,
        order_id, derived_gross_amt, derived_discount_amt, derived_net_amt, misc_charge_amt, tax_amt, discount_code,
        source_discount_type, source_item_plu_id, source_item_desc
        FROM "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE" tline
         WHERE tline.brand_id ='bww' AND tline.void_ind = FALSE
         AND tline.business_date IN (SELECT business_date FROM CTE_BUSINESS_DATE)
         ) tl ON t.brand_id = tl.brand_id AND t.rest_id = tl.rest_id
              AND t.business_date = tl.business_date AND t.order_id = tl.order_id
   )
    GROUP BY brand_id, rest_id, business_date , fn_measure_id
  ),
  CTE_SALESSUMMARY AS --This CTE is being created for Measures # Beer Sales, Wine & Liquor Sales, Other Sales
  ( 
  SELECT
   brand_id, rest_id, business_date, fn_measure_id, COUNT(rest_id) AS sales_count,
   SUM(amount) AS  sales_usd_amount
   FROM
   (
    SELECT
    lo.brand_id, lo.rest_id, CAST(dateofbusiness AS DATE) AS business_date, ss.amount,
    CASE
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('52','19','20') AND ss.typeid in ('3')           THEN 34 -- Beer Sales
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('52','19','20') AND ss.typeid in ('2','4')       THEN 53 -- Wine & Liquor Sales
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('52','19','20') AND ss.typeid in ('5','6','10')  THEN 23 -- Other Sales
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('52','19','20') AND ss.typeid in ('8','14')      THEN 22 -- Bev Sales
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('52','19','20') AND ss.typeid in ('1')           THEN 32 -- Food Sales
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('5') AND ss.typeid NOT in ('7','8')              THEN 39 -- Complementary Meals
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('5') AND ss.typeid in ('8')                      THEN 40 -- Food Comp Gaming
    WHEN r.fn_measure_id2 = 99 AND ss.type IN ('5') AND ss.typeid in ('7')                      THEN 41 -- Alcohol Comp Gaming
    WHEN r.fn_measure_id2 = 33 AND ss.type IN ('20')                                            THEN 33 -- Discounts
    ELSE 0
    END AS fn_measure_id
    FROM  (
          select dateofbusiness, fkstoreid, type, typeid, amount FROM
          (
            SELECT dateofbusiness, fkstoreid, type, typeid, amount,
            RANK() OVER (PARTITION BY fkstoreid, CAST(dateofbusiness AS DATE) ORDER BY loaddatetime DESC, filename DESC) AS rank
            FROM "RDS_{{params.source_env}}".BWW_BV.CORP_PAID_IN_OUT_RECORD_BV
            WHERE CAST(dateofbusiness AS DATE) IN (SELECT business_date FROM CTE_BUSINESS_DATE)
          ) item WHERE rank = 1
        ) ss 
       INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and cast(ss.fkstoreid AS INTEGER) = cast(lo.rest_id AS INTEGER) AND ss.dateofbusiness between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
        Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and cast(ss.fkstoreid AS INTEGER) = cast(lc.rest_id AS INTEGER) AND ss.dateofbusiness = lc.TEMP_CLOSE_DATE 
        INNER JOIN (SELECT 33 AS fn_measure_id2                     -- Discount
                     UNION ALL SELECT 99 AS fn_measure_id2          -- Rest of the measures
                    ) r
		where lc.TEMP_CLOSE_DATE is null 
   )
    GROUP BY brand_id, rest_id, business_date , fn_measure_id
  ),
  CTE_TAKEOUT_FOOD_BEV AS   --This CTE is being created for Measures # Food Sales, Beverage sales, Take Out
  (
   SELECT
   'bww' AS brand_id,business_date, rest_id, fn_measure_id, SUM(amount) AS sales_usd_amount, COUNT(*) AS sales_count
   FROM (
        SELECT
        SUBSTRING (dateofbusiness, 1,10) AS business_date, LPAD(fkstoreid, 5, 0) AS rest_id, amount * -1 AS amount,
        CASE WHEN typeid = 1 THEN 32   --Food Sales
             WHEN typeid = 8 THEN 22   --Beverage sales
             WHEN typeid = 14 THEN 22  --Beverage sales
        END AS fn_measure_id
        FROM (
          SELECT 
          to_date(dateofbusiness,'yyyymmdd') as dateofbusiness, fkstoreid, amount, typeid,
          ROW_NUMBER() OVER (PARTITION BY  dateofbusiness, fkstoreid, fkrevenueid, period, type, typeid, typeid2,
          openhour, fkoccasionid ORDER BY datetimestamp DESC, folderdate DESC, filedate DESC, loadid DESC) AS row_num
           FROM "RDS_{{params.source_env}}"."BWW_BV"."INSIGHT_DPVHSTSALESBYINTERVAL_HISTORY_BV"
           WHERE SUBSTRING(dateofbusiness, 1, 10) IN (SELECT TO_VARCHAR(business_date::DATE,'yyyymmdd') as business_date FROM CTE_BUSINESS_DATE)
           AND fkrevenueid IN (10, 11, 12) AND type = 1 AND typeid IN (1, 8, 14) and source = 'corp'
        ) t
        INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and LPAD(t.fkstoreid, 5, 0) = lo.REST_ID AND SUBSTRING(dateofbusiness, 1, 10) between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
       Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and LPAD(t.fkstoreid, 5, 0) = lc.REST_ID AND SUBSTRING(dateofbusiness, 1, 10) = lc.TEMP_CLOSE_DATE   
        WHERE row_num =1 and lc.TEMP_CLOSE_DATE is null   --Selecting latest data using partition by specified columns
   )
    GROUP BY rest_id, business_date, fn_measure_id
  ),
  CTE_TAKEOUT AS    --This CTE is being created for Measures # Take Out
  (
   SELECT brand_id, business_date, rest_id, 35 AS fn_measure_id, sales_usd_amount * -1 AS sales_usd_amount, sales_count
   FROM CTE_TAKEOUT_FOOD_BEV
  ),
  CTE_MODE_CHARGE AS   --This CTE is being created for Measures # Take Out
  (
    SELECT 'bww' AS brand_id, business_date, rest_id, fn_measure_id,
      CASE WHEN fn_measure_id = 35 THEN SUM(modecharge) 
           WHEN fn_measure_id = 15 THEN SUM(taxexemptamt)
          ELSE 0 END AS sales_usd_amount, COUNT(*) AS sales_count
    FROM (
         SELECT
         SUBSTRING(dateofbusiness, 1,10) AS business_date, LPAD(fkstoreid, 5, 0) AS rest_id,
         modecharge, taxexemptamt
         FROM (
        		SELECT
        		fkstoreid, to_date(dateofbusiness,'yyyymmdd') as dateofbusiness, modecharge, taxexemptamt,
  		    ROW_NUMBER() OVER (PARTITION BY  dateofbusiness, fkstoreid, checkid
  		    ORDER BY folderdate DESC, filedate DESC, loadid DESC) AS row_num
               FROM "RDS_{{params.source_env}}"."BWW_BV"."INSIGHT_DPVHSTCHECKSUMMARY_HISTORY_BV"
               WHERE SUBSTRING(dateofbusiness,1,10) IN (SELECT TO_VARCHAR(business_date::DATE,'yyyymmdd') as business_date FROM CTE_BUSINESS_DATE) and source = 'corp'
             ) cmc
              INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and LPAD(cmc.fkstoreid, 5, 0) = lo.REST_ID AND cmc.dateofbusiness between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
           Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and LPAD(cmc.fkstoreid, 5, 0) = lc.REST_ID AND cmc.dateofbusiness = lc.TEMP_CLOSE_DATE   
          WHERE row_num = 1 and lc.TEMP_CLOSE_DATE is null
         )
         INNER JOIN (SELECT 35 AS fn_measure_id                   -- Takeout
                     UNION ALL SELECT 15 AS fn_measure_id         -- Taxexempt sales
                    ) r
   GROUP BY rest_id, business_date, fn_measure_id
  ),
  CTE_TENDER AS   --This CTE is being created for Measures # Visa, MC, AMEX, Discover, House Charge, GC Redeemed, other deposits, cash deposits
  (
    SELECT brand_id, rest_id, business_date, CASE WHEN fn_measure_id2 = 1 THEN '30' ELSE fn_measure_id END AS fn_measure_id , amount, tip, sales_count
    FROM
    (
      SELECT 'bww' AS brand_id, rest_id, business_date, fn_measure_id, SUM(tip) AS tip, 
            SUM(amount) AS amount, COUNT(*) AS sales_count
      FROM
      (
        SELECT
        LPAD(t.fkstoreid, 5, 0) AS rest_id, SUBSTRING(t.dateofbusiness, 1,10) AS business_date, t.amount, t.tip, t.filedate,
        CASE WHEN t.TYPE = 1 AND t.typeid IN (85,500,510,520,300)                               THEN 4 -- Visa
        WHEN t.TYPE = 1 AND t.typeid IN (84,501,511,521)                                        THEN 5 --MC
        WHEN t.TYPE = 1 AND t.typeid IN (83,502,512,522)                                        THEN 6 --Amex
        WHEN t.TYPE = 1 AND t.typeid IN (86,87,503, 513, 523, 505, 515, 525)                    THEN 7 --Discover
        WHEN t.TYPE = 1 AND t.typeid IN (10,11,605,614,618,623,624,627,635,641)                 THEN 9 --House Charge
        WHEN t.TYPE = 1 AND t.typeid IN (50,88, 504, 514, 524)                                  THEN 8 --GC Redeemed
        WHEN t.TYPE = 1 AND t.typeid IN (200)                                                   THEN 29 --other deposits
        WHEN t.TYPE = 1 AND t.typeid IN (1)                                                     THEN 3 --cash deposits
        WHEN t.TYPE = 1 AND t.typeid IN (601)                                                   THEN 11 --Doordash
        WHEN t.TYPE = 1 AND t.typeid IN (602)                                                   THEN 12 --Grubhub
        WHEN t.TYPE = 1 AND t.typeid IN (603)                                                   THEN 13 --Waitr
        WHEN t.TYPE = 1 AND t.typeid IN (604)                                                   THEN 10 --UberEats
        ELSE 0 END AS fn_measure_id
        FROM
        (
          SELECT
          fkstoreid, to_date(dateofbusiness,'yyyymmdd') as dateofbusiness, amount, tip, type, typeid, filedate,
          RANK() OVER (PARTITION BY dateofbusiness, fkstoreid
            ORDER BY folderdate desc, FILEDATE desc, loaddatetime desc) AS rank
          FROM "RDS_{{params.source_env}}"."BWW_BV"."INSIGHT_DPVHSTGNDTENDER_HISTORY_BV"
          WHERE SUBSTRING(dateofbusiness,1,10) IN (SELECT TO_VARCHAR(business_date::DATE,'yyyymmdd') as business_date FROM CTE_BUSINESS_DATE) AND type = '1' AND source = 'corp'
          AND typeid IN (1, 200, 85, 500, 510, 520, 300, 84, 501, 511, 521, 83, 502, 512, 522, 86, 87, 503,
              513, 523, 505, 515, 525, 10, 11, 601, 602, 603, 604, 605, 614, 618, 623, 624, 627,
              635, 641, 50, 88, 504, 514, 524)
        ) t
        INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and LPAD(t.fkstoreid, 5, 0) = lo.REST_ID AND t.dateofbusiness between lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
           Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and LPAD(t.fkstoreid, 5, 0) = lc.REST_ID AND t.dateofbusiness = lc.TEMP_CLOSE_DATE
        WHERE rank = 1   and lc.TEMP_CLOSE_DATE is null      --Selecting latest data using partition by specified columns like ID, type, typeid, etc
      )
      WHERE fn_measure_id IS NOT NULL
      GROUP BY rest_id, business_date, fn_measure_id, filedate
    ) td
    INNER JOIN (SELECT 1 AS fn_measure_id2                   -- Duplicating the data by creating this join to get to employee tips and credit card measures from the same row.
                UNION ALL SELECT 2 AS fn_measure_id2
               ) r
  ),
  CTE_CREDIT AS      --This CTE is being created for TRANS_LINE for total tips (employee + driver)
  (
   Select 9930 AS fn_measure_id  -- this is total Tips (includes employee + driver tips), as currently driver tips are included in gratuity at trans level
  ),
  CTE_INSTANT_TIPS_STORES AS  -- This CTE has Stores that has Instant Tips Enabled, will go away in few months, so hardcoded instead of in config table 
  (
  SELECT 
  '00021' as rest_id              
  UNION ALL SELECT '00028' UNION ALL SELECT '00040' UNION ALL SELECT '00055' UNION ALL SELECT '00069'
  UNION ALL SELECT '00076' UNION ALL SELECT '00088' UNION ALL SELECT '00089' UNION ALL SELECT '00093'
  UNION ALL SELECT '00110' UNION ALL SELECT '00115' UNION ALL SELECT '00118' UNION ALL SELECT '00119'
  UNION ALL SELECT '00123' UNION ALL SELECT '00126' UNION ALL SELECT '00128' UNION ALL SELECT '00129'
  UNION ALL SELECT '00133' UNION ALL SELECT '00141' UNION ALL SELECT '00165' UNION ALL SELECT '00186'
  UNION ALL SELECT '00189' UNION ALL SELECT '00194' UNION ALL SELECT '00195' UNION ALL SELECT '00198'
  UNION ALL SELECT '00200' UNION ALL SELECT '00207' UNION ALL SELECT '00227' UNION ALL SELECT '00231'
  UNION ALL SELECT '00255' UNION ALL SELECT '00266' UNION ALL SELECT '00271' UNION ALL SELECT '00282'
  UNION ALL SELECT '00285' UNION ALL SELECT '00292' UNION ALL SELECT '00298' UNION ALL SELECT '00302'
  UNION ALL SELECT '00318' UNION ALL SELECT '00319' UNION ALL SELECT '00355' UNION ALL SELECT '00375'
  UNION ALL SELECT '00386' UNION ALL SELECT '00392' UNION ALL SELECT '00398' UNION ALL SELECT '00414'
  UNION ALL SELECT '00453' UNION ALL SELECT '00454' UNION ALL SELECT '00455' UNION ALL SELECT '00456'
  UNION ALL SELECT '00458' UNION ALL SELECT '00463' UNION ALL SELECT '00479' UNION ALL SELECT '00481'
  UNION ALL SELECT '00489' UNION ALL SELECT '00496' UNION ALL SELECT '00498' UNION ALL SELECT '00499'
  UNION ALL SELECT '00513' UNION ALL SELECT '00531' UNION ALL SELECT '00560' UNION ALL SELECT '00583'
  UNION ALL SELECT '00601' UNION ALL SELECT '00605' UNION ALL SELECT '00632' UNION ALL SELECT '00645'
  UNION ALL SELECT '00670' UNION ALL SELECT '00687' UNION ALL SELECT '00690'
  ),
  CTE_TRANS AS            --This CTE is being created for total Employee Tips
  (
    SELECT
    t.brand_id, t.business_date, t.rest_id, t.fn_measure_id, COUNT(*) AS sales_count
    ,COALESCE(SUM(CASE 
                       WHEN t.fn_measure_id = 9930 THEN t.gratuity_amt        -- Calculating total gratuity (currently includes driver tips, as they come in gratuity amount in trans table)
                  END), 0
             ) as sales_usd_amount
    FROM(
        SELECT
        t.brand_id , t.business_date, t.rest_id , credits.fn_measure_id, t.tax_amt, t.derived_net_amt, 
        t.tax_exempt_ind, t.derived_discount_amt, t.gratuity_amt, t.misc_charge_amt, t. surcharge_amt
        FROM CTE_CREDIT credits
        JOIN FILTERED_RECORDS t
        WHERE t.brand_id = 'bww' AND t.business_date in (SELECT business_date FROM CTE_BUSINESS_DATE)
       ) t
    GROUP BY t.brand_id, t.rest_id, t.business_date , fn_measure_id
  ),
  CTE_TAX_DELIVERY_FEE_SURCHARGE AS      --This CTE is being created for Measures # Sales Tax from Trans Line table
  (
   SELECT k.business_date, k.rest_id, k.brand_id,
      SUM(CASE WHEN UPPER(k.order_line_type) = 'ITEM' THEN NVL(k.tax_amt, 0) END) AS tl_incl_tx,
      SUM(CASE WHEN UPPER(k.order_line_type) = 'SURCHARGE' AND UPPER(k.source_item_desc) like '%FEE%'
            THEN NVL(k.derived_gross_amt,0) END) AS tl_delivery_fee,
      SUM(CASE WHEN UPPER(k.order_line_type) = 'SURCHARGE' AND UPPER(k.source_item_desc) like '%SURCHARGE%'
            THEN NVL(k.derived_gross_amt,0) END) AS tl_surcharge
    FROM  "IDH_{{params.source_env}}"."D_TRANS"."TRANS_LINE" k
   WHERE k.brand_id ='bww'
    AND ( UPPER(k.order_line_type) = 'ITEM' 
          OR ( UPPER(k.order_line_type) = 'SURCHARGE' AND ( UPPER(k.source_item_desc) like '%FEE%' OR UPPER(k.source_item_desc) like '%SURCHARGE%') ) )
   AND k.void_ind = FALSE AND k.business_date in (SELECT business_date FROM CTE_BUSINESS_DATE)
   GROUP BY k.business_date, k.rest_id, k.brand_id
  ),
  CTE_TAX AS            --This CTE is being created for Measures # Sales Tax
  (
    SELECT
      t.brand_id, t.rest_id, t.business_date,NVL(sum(t.surcharge_amt),0) trans_surcharge_amt,
      NVL(sum(t.tax_amt),0) trans_tax_amt,
      COUNT(rest_id) AS sales_count,'2' AS fn_measure_id
    FROM FILTERED_RECORDS t
    GROUP BY t.brand_id, t.rest_id, t.business_date
  ),
  CTE_PAID_OUT AS            --This CTE is being created for Measures # Paid Out
  (
   SELECT
   brand_id, rest_id, business_date, fn_measure_id,
   SUM(COALESCE(paid_in_out_amt, 0)) AS sales_usd_amount, COUNT(*) AS sales_count
   FROM (
         SELECT
         t.brand_id, t.rest_id, t.business_date, t.paid_in_out_amt * -1 AS paid_in_out_amt, '17' AS fn_measure_id
         FROM  "IDS_{{params.source_env}}".TXN_BV.PAID_IN_OUT_ACTIVITY_BV t
        INNER JOIN "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_OPENCLOSE_DATE lo on lo.brand_id = 'bww' and t.rest_id = lo.REST_ID AND t.business_date between  lo.CALC_OPEN_DATE and lo.CALC_CLOSURE_DATE
       Left outer join "IDS_{{params.target_env}}".LOCN_BV.FN_REST_ICR_TEMPCLOSE_DATE lc on lc.brand_id = 'bww' and t.rest_id = lc.REST_ID AND t.business_date = lc.TEMP_CLOSE_DATE 
         WHERE t.brand_id='bww'  and lc.TEMP_CLOSE_DATE is null  
            AND t.business_date  IN (SELECT business_date FROM CTE_BUSINESS_DATE) 
        )
   GROUP BY brand_id, rest_id, business_date, fn_measure_id
  ),
  MASTER_TXNS AS
  (
    SELECT brand_id, business_date, rest_id, 
      CASE WHEN fn_measure_id = 9930 THEN 38              -- Total Gratuity is being added to sales delivery (remove employee tips below)
          ELSE fn_measure_id END as fn_measure_id,
      sales_usd_amount, sales_count
    FROM CTE_TRANS 
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count
    FROM CTE_TRANSLINE
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count
    FROM CTE_SALESSUMMARY
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count
    FROM CTE_TAKEOUT_FOOD_BEV
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count
    FROM CTE_TAKEOUT
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count
    FROM CTE_MODE_CHARGE
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, 
      CASE WHEN fn_measure_id <> 30  THEN amount+tip  -- all credit cards
          WHEN fn_measure_id = 30 AND rest_id IN (SELECT rest_id FROM CTE_INSTANT_TIPS_STORES) THEN tip -- Employee tips for instant store only
          ELSE 0 END AS sales_usd_amount,
          sales_count
    FROM CTE_TENDER
    UNION ALL
    SELECT brand_id, business_date, rest_id, 3 AS fn_measure_id, -1 * tip AS sales_usd_amount, sales_count    -- subtract employee tips from cash deposit for non-instant stores
    FROM CTE_TENDER WHERE fn_measure_id = 30 AND rest_id NOT IN (SELECT rest_id FROM CTE_INSTANT_TIPS_STORES)
    UNION ALL
    SELECT brand_id, business_date, rest_id, 38 AS fn_measure_id, -1 * tip AS sales_usd_amount, sales_count    -- subtract employee tips from sales delivery
    FROM CTE_TENDER WHERE fn_measure_id = 30
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, final_sales_tax AS sales_usd_amount, sales_count  --This Select Statement is being used for Sales Tax
    FROM(
        SELECT
        CASE WHEN tl_surcharge_amt = 0 AND trans_surcharge_amt <> 0 THEN trans_tax_amt + tl_incl_tax
             ELSE trans_tax_amt + tl_surcharge_amt + tl_incl_tax
        END AS final_sales_tax,
        brand_id, rest_id, business_date, fn_measure_id, trans_tax_amt, trans_surcharge_amt,
        tl_delivery_fee, tl_surcharge_amt, tl_incl_tax, sales_count
        FROM(
            SELECT
              ct.brand_id, ct.rest_id, ct.business_date, ct.fn_measure_id, COUNT(ct.sales_count) AS sales_count,
              NVL(SUM(ct.trans_tax_amt),0) trans_tax_amt,NVL(SUM(ct.trans_surcharge_amt),0) trans_surcharge_amt,
              NVL(SUM(ctdfs.tl_delivery_fee),0) tl_delivery_fee, NVL(SUM(ctdfs.tl_surcharge),0) tl_surcharge_amt,
              NVL(SUM(ctdfs.tl_incl_tx),0) tl_incl_tax
            FROM CTE_TAX ct
            LEFT JOIN CTE_TAX_DELIVERY_FEE_SURCHARGE ctdfs
            ON ct.brand_id = ctdfs.brand_id AND ct.rest_id = ctdfs.rest_id AND ct.business_date = ctdfs.business_date
            GROUP BY ct.brand_id, ct.rest_id, ct.business_date, ct.fn_measure_id
           ) a
       )
    UNION ALL
    SELECT brand_id, business_date, rest_id, fn_measure_id, sales_usd_amount, sales_count
    FROM CTE_PAID_OUT
    UNION ALL
    SELECT brand_id, business_date, rest_id, '3' AS fn_measure_id, -1 * sales_usd_amount, sales_count -- Subtracting PaidOuts from Cash Deposits
    FROM CTE_PAID_OUT 
  )
  SELECT
  'bww' AS brand_id,
  cisc.business_date AS business_date,
  cisc.rest_id AS rest_id,
  '10' AS fn_system_id,
  cisc.fn_measure_id AS fn_measure_id,
  'N/A' AS gl_account_code,
  'N/A' AS gl_cost_ctr,
  SUM(cisc.sales_usd_amount) AS sale_usd_amount,
  SUM(cisc.sales_usd_amount) AS sale_amount,
  'USA' AS COUNTRY_CODE,
  'USD' AS currency_code,
  'aloha' AS source_system_name,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS load_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS load_dttm,
  TO_NUMBER(TO_CHAR(CURRENT_DATE, 'YYYYMMDD')) AS update_id,
  TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS update_dttm
  FROM MASTER_TXNS cisc
  where fn_measure_id < 1000          -- This filter is to remove temporary measure_ids created in the code for calculation
  GROUP BY cisc.business_date, cisc.rest_id, cisc.fn_measure_id
) pb;

MERGE INTO IDS_{{params.target_env}}.TXN."FN_DAILY_REV_MEASURE" fdrm
USING
(
  Select * from "IDS_{{params.target_env}}".TXN.FN_DAILY_REV_MEASURE_Aloha_Temp_table
) AS pbs

ON fdrm.business_date = pbs.business_date 
AND fdrm.brand_id = pbs.brand_id
AND fdrm.fn_system_id = pbs.fn_system_id
AND fdrm.fn_measure_id = pbs.fn_measure_id 
AND fdrm.gl_account_code = pbs.gl_account_code
AND fdrm.gl_cost_ctr= pbs.gl_cost_ctr 
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