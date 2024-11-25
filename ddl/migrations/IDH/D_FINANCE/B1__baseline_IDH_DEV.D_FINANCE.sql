create view IF NOT EXISTS BOS_DAILY_SALES_MEASURE(
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
   S.FN_SYSTEM_CATEGORY_CODE
  , d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.business_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by  S.FN_SYSTEM_CATEGORY_CODE, d.cal_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
FROM  (SELECT * FROM "IDH_DEV"."D_REF"."DATE" WHERE  cal_date between current_date - 120 and current_date) d
--INNER JOIN  IDH_DEV.D_LOC.REST r
-- on d.Fisc_Wk_end_date >=  r.open_date  and d.Fisc_Wk_start_date <= coalesce(r.closure_date,to_date('9999-12-31'))
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  business_date between current_date - 120 and current_date)   r
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on r.brand_id = s.brand_id
  AND s.FN_SYSTEM_CATEGORY_CODE = 'BOS'
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on 1 = 1
LEFT OUTER JOIN IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
ON d.cal_date = dsd.business_date
  -- and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  AND s.fn_system_id = dsd.fn_system_id
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
) vw
;
create view IF NOT EXISTS BOS_WEEKLY_SALES_MEASURE(
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
   S.FN_SYSTEM_CATEGORY_CODE
  --, d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by S.FN_SYSTEM_CATEGORY_CODE, d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
FROM  (SELECT * FROM "IDH_DEV"."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
--INNER JOIN  IDH_DEV.D_LOC.REST r
-- on d.Fisc_Wk_end_date >=  r.open_date  and d.Fisc_Wk_start_date <= coalesce(r.closure_date,to_date('9999-12-31'))
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on r.brand_id = s.brand_id
  and s.FN_SYSTEM_CATEGORY_CODE = 'BOS'
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on 1 = 1
LEFT OUTER JOIN IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  -- and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  AND s.fn_system_id = dsd.fn_system_id
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
       --,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw
;
create view IF NOT EXISTS DAILY_SALES_MEASURE(
	SYSTEM_CATEGORY_CODE,
	SYSTEM_NAME,
	BUSINESS_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	GL_ACCOUNT_CODE,
	GL_COST_CTR,
	SALE_USD_AMOUNT,
	SALE_AMOUNT,
	SALE_COUNT,
	COUNTRY_CODE,
	CURRENCY_CODE,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE,
	FISC_WK_END_DATE,
	UPDATE_DTTM
) as
select 
   FN_SYSTEM_CATEGORY_CODE as SYSTEM_CATEGORY_CODE
   ,system_name
      ,business_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
	   ,gl_account_code
       ,gl_cost_ctr
       ,sale_usd_amount
	   ,sale_amount
       ,sale_count
	   ,country_code
	   ,currency_code
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
	 ,Fisc_Wk_end_date
     , update_dttm
FROM 
(
SELECT 
   S.FN_SYSTEM_CATEGORY_CODE
   ,s.system_name
 , d.cal_date as business_date
  , dsd.brand_id brand_id
  , dsd.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , dsd.gl_account_code
  , dsd.gl_cost_ctr
  , dsd.sale_usd_amount as  sale_usd_amount
  , dsd.sale_amount
  , dsd.sale_count
  , dsd.country_code
  , dsd.currency_code
  , row_number() over (partition by S.FN_SYSTEM_CATEGORY_CODE, dsd.business_date, dsd.brand_id, dsd.rest_id, dsd.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
 , dsd.update_dttm
FROM IDS_DEV.TXN_BV.FN_daily_REV_MEASURE_BV dsd
INNER JOIN  IDH_DEV.D_REF.DATE  d
ON dsd.business_date = d.cal_date
INNER JOIN  IDH_DEV.D_LOC.REST r
 on r.rest_id = dsd.rest_id
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on dsd.fn_system_id = s.fn_system_id
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on dsd.fn_measure_id = m.fn_measure_id
) dy
where dy.row_rank_id = 1
;
create view IF NOT EXISTS GL_WEEKLY_SALES_MEASURE(
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
   S.FN_SYSTEM_CATEGORY_CODE
  --, d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by S.FN_SYSTEM_CATEGORY_CODE, d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
FROM  (SELECT * FROM "IDH_DEV"."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
--INNER JOIN  IDH_DEV.D_LOC.REST r
-- on d.Fisc_Wk_end_date >=  r.open_date  and d.Fisc_Wk_start_date <= coalesce(r.closure_date,to_date('9999-12-31'))
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on r.brand_id = s.brand_id
  and s.FN_SYSTEM_CATEGORY_CODE = 'GL'
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on 1 = 1
LEFT OUTER JOIN IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  -- and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  AND s.fn_system_id = dsd.fn_system_id
  and r.rest_id = dsd.rest_id
) dy
where dy.row_rank_id = 1
 --and dy.FN_SYSTEM_CATEGORY_CODE = 'GL'
 group by 
          Fisc_Wk_end_date
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
) vw
--where Fisc_Wk_end_date = '2022-07-10'
--and brand_id = 'arbys'
;
create view IF NOT EXISTS POS_BOS_VARIANCE(
	BUSINESS_DATE,
	REST_ID,
	MEASURE_NAME,
	POS_SALE_AMT,
	BOS_SALE_AMT,
	POS_BOS_DIFFERENCE
) as select P.BUSINESS_DATE,P.REST_ID,P.MEASURE_NAME, P.SALE_USD_AMOUNT as POS_SALE_AMT,B.SALE_USD_AMOUNT as BOS_SALE_AMT, (P.SALE_USD_AMOUNT-B.SALE_USD_AMOUNT) as POS_BOS_Difference   from "IDH_DEV"."D_FINANCE"."POS_DAILY_SALES_MEASURE" P inner join "IDH_DEV"."D_FINANCE"."BOS_DAILY_SALES_MEASURE" B  on P.BUSINESS_DATE =B.BUSINESS_DATE and P.REST_ID= B.REST_ID and p.MEASURE_NAME=B.MEASURE_NAME WHERE B.FISC_WK_END_DATE = '2022-07-17' and (P.SALE_USD_AMOUNT <>0 and B.SALE_USD_AMOUNT <>0);
create view IF NOT EXISTS POS_DAILY_SALES_MEASURE(
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
   S.FN_SYSTEM_CATEGORY_CODE
  , d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.business_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by  S.FN_SYSTEM_CATEGORY_CODE, d.cal_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
FROM  (SELECT * FROM "IDH_DEV"."D_REF"."DATE" WHERE  cal_date between current_date - 120 and current_date) d
--INNER JOIN  IDH_DEV.D_LOC.REST r
-- on d.Fisc_Wk_end_date >=  r.open_date  and d.Fisc_Wk_start_date <= coalesce(r.closure_date,to_date('9999-12-31'))
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV  WHERE  business_date between current_date - 120 and current_date)   r
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on r.brand_id = s.brand_id
  and s.FN_SYSTEM_CATEGORY_CODE = 'POS'
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on 1 = 1
LEFT OUTER JOIN IDS_DEV.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
ON d.cal_date = dsd.business_date
  -- and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  AND s.fn_system_id = dsd.fn_system_id
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
) vw
;
create view IF NOT EXISTS POS_WEEKLY_SALES_MEASURE(
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
   S.FN_SYSTEM_CATEGORY_CODE
  --, d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by S.FN_SYSTEM_CATEGORY_CODE, d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
FROM  (SELECT * FROM "IDH_DEV"."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
--INNER JOIN  IDH_DEV.D_LOC.REST r
-- on d.Fisc_Wk_end_date >=  r.open_date  and d.Fisc_Wk_start_date <= coalesce(r.closure_date,to_date('9999-12-31'))
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on r.brand_id = s.brand_id
  and s.FN_SYSTEM_CATEGORY_CODE = 'POS'
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on 1 = 1
LEFT OUTER JOIN IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  -- and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  AND s.fn_system_id = dsd.fn_system_id
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
       --,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw
;
create view IF NOT EXISTS SS_WEEKLY_SALES_MEASURE(
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
SELECT   Fisc_Wk_end_date
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
   S.FN_SYSTEM_CATEGORY_CODE
  --, d.cal_date as business_date
  , r.brand_id brand_id
  , r.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , case when dsd.Fisc_Wk_end_date is null then TRUE else FALSE end imputed_ind
  , coalesce(dsd.sale_usd_amount,0) as  sale_usd_amount
  , row_number() over (partition by S.FN_SYSTEM_CATEGORY_CODE, d.Fisc_Wk_end_date, r.brand_id, r.rest_id, m.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
FROM  (SELECT * FROM "IDH_DEV"."D_REF"."DATE" WHERE CAL_DATE = Fisc_Wk_end_date AND cal_date between current_date - 120 and current_date) d
--INNER JOIN  IDH_DEV.D_LOC.REST r
-- on d.Fisc_Wk_end_date >=  r.open_date  and d.Fisc_Wk_start_date <= coalesce(r.closure_date,to_date('9999-12-31'))
INNER JOIN  (select distinct brand_id, rest_id from IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV  WHERE  Fisc_Wk_end_date between current_date - 120 and current_date)   r
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on r.brand_id = s.brand_id
  and s.FN_SYSTEM_CATEGORY_CODE = 'SS'
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on 1 = 1
LEFT OUTER JOIN IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date
  -- and r.brand_id = dsd.brand_id -- system id is associated with brand id
  and m.fn_measure_id = dsd.fn_measure_id
  AND s.fn_system_id = dsd.fn_system_id
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
       --,sum(sale_usd_amount) sale_usd_amount
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
) vw
;
create view IF NOT EXISTS WEEKLY_SALES_MEASURE(
	SYSTEM_CATEGORY_CODE,
	SYSTEM_NAME,
	FISC_WK_END_DATE,
	BRAND_ID,
	REST_ID,
	MEASURE_NAME,
	MEASURE_SORT_ID,
	MEASURE_CATEGORY_NAME,
	GL_ACCOUNT_CODE,
	GL_COST_CTR,
	SALE_USD_AMOUNT,
	SALE_AMOUNT,
	SALE_COUNT,
	COUNTRY_CODE,
	CURRENCY_CODE,
	FISC_YR_NBR,
	FISC_YR_START_DATE,
	FISC_YR_END_DATE,
	FISC_PERIOD_NBR,
	FISC_PERIOD_START_DATE,
	FISC_PERIOD_END_DATE,
	FISC_WK_NBR,
	FISC_WK_START_DATE,
	UPDATE_DTTM
) as
select 
   FN_SYSTEM_CATEGORY_CODE as SYSTEM_CATEGORY_CODE
   ,system_name
      ,Fisc_Wk_end_date
	 , brand_id
       ,rest_id 
       ,measure_name
       ,measure_sort_id
       ,measure_category_name
	   ,gl_account_code
       ,gl_cost_ctr
       ,sale_usd_amount
	   ,sale_amount
       ,sale_count
	   ,country_code
	   ,currency_code
     , Fisc_yr_nbr
     , Fisc_yr_start_date
     , Fisc_yr_End_date
     , Fisc_period_nbr
     , Fisc_period_start_date
     , Fisc_period_end_date
     , Fisc_wk_nbr
     , Fisc_Wk_start_date
     , update_dttm
FROM 
(
SELECT 
   S.FN_SYSTEM_CATEGORY_CODE
   ,s.system_name
  --, d.cal_date as business_date
  , dsd.brand_id brand_id
  , dsd.rest_id as rest_id
  , m.fn_measure_id
  , m.measure_name
  , m.default_sort_id as measure_sort_id
  , m.measure_category_name
  , dsd.gl_account_code
  , dsd.gl_cost_ctr
  , dsd.sale_usd_amount as  sale_usd_amount
  , dsd.sale_amount
  , dsd.sale_count
  , dsd.country_code
  , dsd.currency_code
  , row_number() over (partition by S.FN_SYSTEM_CATEGORY_CODE, dsd.Fisc_Wk_end_date, dsd.brand_id, dsd.rest_id, dsd.fn_measure_id, dsd.gl_account_code, dsd.gl_cost_ctr order by dsd.load_dttm desc)     row_rank_id -- need rank to pull latest record
 , d.Fisc_yr_nbr
 , d.Fisc_yr_start_date
 , d.Fisc_yr_End_date
 , d.Fisc_period_nbr
 , d.Fisc_period_start_date
 , d.Fisc_period_end_date
 , d.Fisc_wk_nbr
 , d.Fisc_Wk_end_date
 , d.Fisc_Wk_start_date
 , dsd.update_dttm
FROM IDS_DEV.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
INNER JOIN  IDH_DEV.D_REF.DATE  d
ON dsd.Fisc_Wk_end_date = d.cal_date
INNER JOIN  IDH_DEV.D_LOC.REST r
 on r.rest_id = dsd.rest_id
INNER JOIN IDS_DEV.TXN_BV.FN_SYSTEM_BV s
  on dsd.fn_system_id = s.fn_system_id
 INNER JOIN  IDS_DEV.TXN_BV.FN_MEASURE_BV m
 on dsd.fn_measure_id = m.fn_measure_id
) dy
where dy.row_rank_id = 1
;