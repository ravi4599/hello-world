create or replace view GL_DAILY_SALES_MEASURE_BWW(
	BUSINESS_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE,
	FISC_WK_END_DATE
) as
select business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
from (
SELECT business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) as sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
FROM 
(
SELECT 
   'GL' AS FN_SYSTEM_CATEGORY_CODE
  , d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.business_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by  d.cal_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
-- TRUE -- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between '2023-01-01' AND '2023-04-02') d
-- TRUE --INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and business_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and business_date  between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'GL' 
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.cal_date = dsd.business_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          business_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
	 ,Fisc_Wk_end_date
) vw;

create or replace view BOS_DAILY_SALES_MEASURE_BWW(
	BUSINESS_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE,
	FISC_WK_END_DATE
) as
select business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
from (
SELECT business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) as sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
FROM 
(
SELECT 
	'BOS' AS FN_SYSTEM_CATEGORY_CODE
  , d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.business_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.cal_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
-- TRUE -- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between '2023-01-01' AND '2023-04-02') d
-- TRUE --INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and business_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and business_date  between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'BOS' 
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.cal_date = dsd.business_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          business_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
	 ,Fisc_Wk_end_date
) vw;

create or replace view BOS_DAILY_SALES_MEASURE_SONIC(
	BUSINESS_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE,
	FISC_WK_END_DATE
) as
select business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
from (
SELECT business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) as sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
FROM 
(
SELECT 
   'BOS' AS FN_SYSTEM_CATEGORY_CODE
  , d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.business_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.cal_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between current_date - 120 and current_date) d
  FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between '2023-01-01' AND '2023-04-02') d
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'sonic' and business_date between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'BOS' 
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.cal_date = dsd.business_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          business_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
	 ,Fisc_Wk_end_date
) vw;

create or replace view BOS_WEEKLY_SALES_MEASURE_BWW(
	FISC_WK_END_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE
) as
select 
      Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
    
from (
SELECT Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
FROM 
(
SELECT 
   'BOS' AS FN_SYSTEM_CATEGORY_CODE
  --, d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
  FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between '2023-01-01' AND '2023-04-01') d
--TRUE-- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  BRAND_ID = 'bww' and Fisc_Wk_end_date between current_date - 120 and current_date)   r
  INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  BRAND_ID = 'bww' and Fisc_Wk_end_date between '2023-01-01' AND '2023-04-02')   r
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'BOS' 
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw;

create or replace view BOS_WEEKLY_SALES_MEASURE_SONIC(
	FISC_WK_END_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE
) as
select 
      Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
    
from (
SELECT Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
FROM 
(
SELECT 
   'BOS' AS FN_SYSTEM_CATEGORY_CODE
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE -- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
  FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between '2023-01-01' AND '2023-04-01') d
--TRUE -- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  BRAND_ID = 'sonic' and Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  BRAND_ID = 'sonic' and Fisc_Wk_end_date between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'BOS' 
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw;

create or replace view GL_WEEKLY_SALES_MEASURE_BWW(
	FISC_WK_END_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE
) as
select 
      Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
    
from (
SELECT
        Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
FROM 
(
SELECT 
   'GL' AS FN_SYSTEM_CATEGORY_CODE
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between '2023-01-01' AND '2023-04-01') d
--TRUE-- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and Fisc_Wk_end_date between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'GL' 
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw;

create or replace view GL_WEEKLY_SALES_MEASURE_SONIC(
	FISC_WK_END_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE
) as
select 
      Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
    
from (
SELECT
        Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
FROM 
(
SELECT 
   'GL' AS FN_SYSTEM_CATEGORY_CODE
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between '2023-01-01' AND '2023-04-01') d
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'sonic' and Fisc_Wk_end_date between '2023-01-01' AND '2023-04-02')   r
--TRUE-- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'sonic' and Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'GL' 
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw;

create or replace view POS_DAILY_SALES_MEASURE_BWW(
	BUSINESS_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE,
	FISC_WK_END_DATE
) as
select business_date
       ,brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
from (
SELECT business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) as sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
FROM 
(
SELECT 
   'POS' AS FN_SYSTEM_CATEGORY_CODE
  , d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.business_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.cal_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between '2023-01-01' AND '2023-04-02') d
--TRUE-- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and business_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and business_date between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'POS'
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.cal_date = dsd.business_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          business_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       --,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
	 ,Fisc_Wk_end_date
) vw;

create or replace view POS_DAILY_SALES_MEASURE_SONIC(
	BUSINESS_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE,
	FISC_WK_END_DATE
) as
select business_date
       ,brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
from (
SELECT business_date
       ,brand_id
       ,rest_id
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) as sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , Fisc_Wk_end_date
FROM 
(
SELECT 
   'POS' AS FN_SYSTEM_CATEGORY_CODE
  , d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.business_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.cal_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE  cal_date between '2023-01-01' AND '2023-04-02') d
--TRUE-- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'sonic' and business_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  brand_id = 'sonic' and business_date between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'POS'
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.cal_date = dsd.business_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          business_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
	 ,Fisc_Wk_end_date
) vw;

create or replace view POS_WEEKLY_SALES_MEASURE_BWW(
	FISC_WK_END_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE
) as
select 
      Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
    
from (
SELECT Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
FROM 
(
SELECT 
   'POS' AS FN_SYSTEM_CATEGORY_CODE
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between '2023-01-01' AND '2023-04-02') d
--INNER JOIN  IDH_QA.D_LOC.REST r
-- on d.Fisc_Wk_end_date >=  r.open_date  and d.Fisc_Wk_start_date <= coalesce(r.closure_date,to_date('9999-12-31'))
--TRUE-- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'bww' and Fisc_Wk_end_date between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'POS'
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw;

create or replace view POS_WEEKLY_SALES_MEASURE_SONIC(
	FISC_WK_END_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	IMPUTED_IND,
	SALE_USD_AMOUNT,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE
) as
select 
      Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
    
from (
SELECT Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
       ,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
FROM 
(
SELECT 
   'POS' AS FN_SYSTEM_CATEGORY_CODE
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
--TRUE-- FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
FROM  (SELECT * FROM IDH_DEV."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between '2023-01-01' AND '2023-04-02') d
--TRUE-- INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'sonic' and Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  brand_id = 'sonic' and Fisc_Wk_end_date between '2023-01-01' AND '2023-04-02')   r
INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m on 1 = 1
  LEFT OUTER JOIN   (
    SELECT dsd.* FROM IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
   INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on s.FN_SYSTEM_CATEGORY_CODE = 'POS'
  AND s.fn_system_id = dsd.fn_system_id
 ) dsd
  ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 group by 
          Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
       ,imputed_ind  -- Indicates no record found so row generated with 0 amount.
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw;