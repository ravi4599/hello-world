--BWW BOS Daily view

create or replace view IDH_{{params.env}}.D_FINANCE.BOS_DAILY_SALES_MEASURE_BWW(
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
WITH CTE_PARAMETERS as
(
    SELECT 'bww' brand_id, CURRENT_DATE - 120 ST_DATE, CURRENT_DATE END_DATE 
)
, CTE_REST as
(
    --to make sure we are reading all rest id irrespective of System type
    SELECT Distinct dsd.brand_id, rest_id
    FROM IDS_{{params.env}}.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
    INNER JOIN CTE_PARAMETERS p
    WHERE dsd.brand_id =p.brand_id and dsd.BUSINESS_DATE BETWEEN p.ST_DATE AND p.END_DATE
)
, CTE_VIEW_DATA AS 
(
    SELECT dsd.brand_id, business_date , rest_id , fn_measure_id, SALE_USD_AMOUNT 
    FROM IDS_{{params.env}}.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
    INNER JOIN IDS_{{params.env}}.TXN_BV.FN_SYSTEM_BV s 
    ON s.FN_SYSTEM_CATEGORY_CODE = 'BOS'	AND s.fn_system_id = dsd.fn_system_id
    inner join CTE_PARAMETERS p 
    on  p.Brand_id = dsd.brand_id and  dsd.BUSINESS_DATE  BETWEEN p.ST_DATE   AND p.END_DATE
)
, CTE_CALENDAR AS
(
    SELECT  p.brand_id, r.rest_id, d.CAL_DATE,  FN_MEASURE_ID , measure_name, default_sort_id, MEASURE_CATEGORY_NAME
   , Fisc_yr_nbr,Fisc_yr_start_date,Fisc_yr_End_date,Fisc_period_nbr,Fisc_period_start_date
    ,Fisc_period_end_date,Fisc_wk_nbr,Fisc_Wk_end_date,Fisc_Wk_start_date
    FROM "IDH_{{params.env}}"."D_REF"."DATE" d
    JOIN CTE_PARAMETERS p on  d.CAL_DATE  BETWEEN p.ST_DATE   AND p.END_DATE
    JOIN CTE_REST r
    JOIN IDS_{{params.env}}.TXN.FN_BRAND_MEASURE BM 
    on p.brand_id = bm.brand_id and bm.SALES_RECON_REPORT_DISPLAY_IND = true
    JOIN IDS_{{params.env}}.TXN_BV.FN_MEASURE_BV USING (FN_MEASURE_ID )
)

    SELECT d.cal_date AS business_date
    ,d.brand_id brand_id
    ,d.rest_id AS rest_id
    ,d.measure_name
    ,MAX(d.default_sort_id) AS measure_sort_id
    ,MAX(d.measure_category_name) as measure_category_name
    ,MAX(CASE WHEN dsd.business_date IS NULL THEN TRUE ELSE FALSE	END) imputed_ind
    ,sum(coalesce(dsd.sale_usd_amount, 0)) AS sale_usd_amount
    ,MAX(Fisc_yr_nbr              ) AS  Fisc_yr_nbr            
    ,MAX(Fisc_yr_start_date       ) AS  Fisc_yr_start_date
    ,MAX(Fisc_yr_End_date         ) AS  Fisc_yr_End_date
    ,MAX(Fisc_period_nbr          ) AS  Fisc_period_nbr
    ,MAX(Fisc_period_start_date   ) AS  Fisc_period_start_date    
    ,MAX(Fisc_period_end_date     ) AS  Fisc_period_end_date    
    ,MAX(Fisc_wk_nbr              ) AS  Fisc_wk_nbr
    ,MAX(Fisc_Wk_start_date       ) AS  Fisc_Wk_start_date
    ,MAX(Fisc_Wk_end_date         ) AS  Fisc_Wk_end_date
    FROM CTE_CALENDAR d 
    LEFT OUTER JOIN CTE_VIEW_DATA  dsd ON d.cal_date = dsd.business_date  
    AND d.fn_measure_id = dsd.fn_measure_id   
    AND d.rest_id = dsd.rest_id
    GROUP BY d.cal_date ,d.brand_id	, d.rest_id	,measure_name;	

-- BWW BOS Weekly view
	
create or replace view IDH_{{params.env}}.D_FINANCE.BOS_WEEKLY_SALES_MEASURE_BWW(
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
WITH CTE_PARAMETERS as
(
    SELECT 'bww' brand_id, CURRENT_DATE - 120 ST_DATE, CURRENT_DATE END_DATE 
)
, CTE_REST as
(
    --to make sure we are reading all rest id irrespective of System type
    SELECT Distinct dsd.brand_id, rest_id
    FROM IDS_{{params.env}}.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
    INNER JOIN CTE_PARAMETERS p
    WHERE dsd.brand_id =p.brand_id and dsd.Fisc_Wk_end_date BETWEEN p.ST_DATE AND p.END_DATE
)
, CTE_VIEW_DATA AS 
(
    SELECT dsd.brand_id, Fisc_Wk_end_date , rest_id , fn_measure_id, SALE_USD_AMOUNT 
    FROM IDS_{{params.env}}.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
    INNER JOIN IDS_{{params.env}}.TXN_BV.FN_SYSTEM_BV s 
    ON s.FN_SYSTEM_CATEGORY_CODE = 'BOS'	AND s.fn_system_id = dsd.fn_system_id
    inner join CTE_PARAMETERS p 
    on  p.Brand_id = dsd.brand_id and  dsd.Fisc_Wk_end_date  BETWEEN p.ST_DATE   AND p.END_DATE
)
, CTE_CALENDAR AS
(
SELECT  p.brand_id, r.rest_id, d.CAL_DATE,  FN_MEASURE_ID , measure_name, default_sort_id, MEASURE_CATEGORY_NAME
   , Fisc_yr_nbr,Fisc_yr_start_date,Fisc_yr_End_date,Fisc_period_nbr,Fisc_period_start_date
    ,Fisc_period_end_date,Fisc_wk_nbr,Fisc_Wk_end_date,Fisc_Wk_start_date
    FROM "IDH_{{params.env}}"."D_REF"."DATE" d 
    JOIN CTE_PARAMETERS p on  d.Fisc_Wk_end_date  BETWEEN p.ST_DATE   AND p.END_DATE
    JOIN CTE_REST r
    JOIN IDS_{{params.env}}.TXN.FN_BRAND_MEASURE BM 
    on p.brand_id = bm.brand_id and bm.SALES_RECON_REPORT_DISPLAY_IND = true
    JOIN IDS_{{params.env}}.TXN_BV.FN_MEASURE_BV USING (FN_MEASURE_ID )
 WHERE CAL_DATE = Fisc_Wk_end_date
)
SELECT MAX(d.Fisc_Wk_end_date) AS  Fisc_Wk_end_date
    ,d.brand_id brand_id
    ,d.rest_id AS rest_id
    ,d.measure_name
    ,MAX(d.default_sort_id) AS measure_sort_id
    ,MAX(d.measure_category_name) as measure_category_name
    ,MAX(CASE WHEN dsd.Fisc_Wk_end_date IS NULL THEN TRUE ELSE FALSE	END) imputed_ind
    ,sum(coalesce(dsd.sale_usd_amount, 0)) AS sale_usd_amount
    ,MAX(d.Fisc_yr_nbr              ) AS  Fisc_yr_nbr            
    ,MAX(d.Fisc_yr_start_date       ) AS  Fisc_yr_start_date
    ,MAX(d.Fisc_yr_End_date         ) AS  Fisc_yr_End_date
    ,MAX(d.Fisc_period_nbr          ) AS  Fisc_period_nbr
    ,MAX(d.Fisc_period_start_date   ) AS  Fisc_period_start_date    
    ,MAX(d.Fisc_period_end_date     ) AS  Fisc_period_end_date    
    ,MAX(d.Fisc_wk_nbr              ) AS  Fisc_wk_nbr
    ,MAX(d.Fisc_Wk_start_date       ) AS  Fisc_Wk_start_date
    FROM CTE_CALENDAR d 
    LEFT OUTER JOIN CTE_VIEW_DATA  dsd ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date  
    AND d.fn_measure_id = dsd.fn_measure_id   
    AND d.rest_id = dsd.rest_id
    GROUP BY d.Fisc_Wk_end_date ,d.brand_id	, d.rest_id	,measure_name;

-- BWW GL Daily view	
create or replace view IDH_{{params.env}}.D_FINANCE.GL_DAILY_SALES_MEASURE_BWW(
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
WITH CTE_PARAMETERS as
(
    SELECT 'bww' brand_id, CURRENT_DATE - 120 ST_DATE, CURRENT_DATE END_DATE 
)
, CTE_REST as
(
    --to make sure we are reading all rest id irrespective of System type
    SELECT Distinct dsd.brand_id, rest_id
    FROM IDS_{{params.env}}.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
    INNER JOIN CTE_PARAMETERS p
    WHERE dsd.brand_id =p.brand_id and dsd.BUSINESS_DATE BETWEEN p.ST_DATE AND p.END_DATE
)
, CTE_VIEW_DATA AS 
(
    SELECT dsd.brand_id, business_date , rest_id , fn_measure_id, SALE_USD_AMOUNT 
    FROM IDS_{{params.env}}.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
    INNER JOIN IDS_{{params.env}}.TXN_BV.FN_SYSTEM_BV s 
    ON s.FN_SYSTEM_CATEGORY_CODE = 'GL'	AND s.fn_system_id = dsd.fn_system_id
    inner join CTE_PARAMETERS p 
    on  p.Brand_id = dsd.brand_id and  dsd.BUSINESS_DATE  BETWEEN p.ST_DATE   AND p.END_DATE
)
, CTE_CALENDAR AS
(
    SELECT  p.brand_id, r.rest_id, d.CAL_DATE,  FN_MEASURE_ID , measure_name, default_sort_id, MEASURE_CATEGORY_NAME
   , Fisc_yr_nbr,Fisc_yr_start_date,Fisc_yr_End_date,Fisc_period_nbr,Fisc_period_start_date
    ,Fisc_period_end_date,Fisc_wk_nbr,Fisc_Wk_end_date,Fisc_Wk_start_date
    FROM "IDH_{{params.env}}"."D_REF"."DATE" d
    JOIN CTE_PARAMETERS p on  d.CAL_DATE  BETWEEN p.ST_DATE   AND p.END_DATE
    JOIN CTE_REST r
    JOIN IDS_{{params.env}}.TXN.FN_BRAND_MEASURE BM 
    on p.brand_id = bm.brand_id and bm.SALES_RECON_REPORT_DISPLAY_IND = true
    JOIN IDS_{{params.env}}.TXN_BV.FN_MEASURE_BV USING (FN_MEASURE_ID )
)
    SELECT d.cal_date AS business_date
    ,d.brand_id brand_id
    ,d.rest_id AS rest_id
    ,d.measure_name
    ,MAX(d.default_sort_id) AS measure_sort_id
    ,MAX(d.measure_category_name) as measure_category_name
    ,MAX(CASE WHEN dsd.business_date IS NULL THEN TRUE ELSE FALSE	END) imputed_ind
    ,sum(coalesce(dsd.sale_usd_amount, 0)) AS sale_usd_amount
    ,MAX(Fisc_yr_nbr              ) AS  Fisc_yr_nbr            
    ,MAX(Fisc_yr_start_date       ) AS  Fisc_yr_start_date
    ,MAX(Fisc_yr_End_date         ) AS  Fisc_yr_End_date
    ,MAX(Fisc_period_nbr          ) AS  Fisc_period_nbr
    ,MAX(Fisc_period_start_date   ) AS  Fisc_period_start_date    
    ,MAX(Fisc_period_end_date     ) AS  Fisc_period_end_date    
    ,MAX(Fisc_wk_nbr              ) AS  Fisc_wk_nbr
    ,MAX(Fisc_Wk_start_date       ) AS  Fisc_Wk_start_date
    ,MAX(Fisc_Wk_end_date         ) AS  Fisc_Wk_end_date
    FROM CTE_CALENDAR d 
    LEFT OUTER JOIN CTE_VIEW_DATA  dsd ON d.cal_date = dsd.business_date  
    AND d.fn_measure_id = dsd.fn_measure_id   
    AND d.rest_id = dsd.rest_id
    GROUP BY d.cal_date ,d.brand_id	, d.rest_id	,measure_name;	

--BWW GL WEEKLY view	
create or replace view IDH_{{params.env}}.D_FINANCE.GL_WEEKLY_SALES_MEASURE_BWW(
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
WITH CTE_PARAMETERS as
(
    SELECT 'bww' brand_id, CURRENT_DATE - 120 ST_DATE, CURRENT_DATE END_DATE 
)
, CTE_REST as
(
    --to make sure we are reading all rest id irrespective of System type
    SELECT Distinct dsd.brand_id, rest_id
    FROM IDS_{{params.env}}.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
    INNER JOIN CTE_PARAMETERS p
    WHERE dsd.brand_id =p.brand_id and dsd.Fisc_Wk_end_date BETWEEN p.ST_DATE AND p.END_DATE
)
, CTE_VIEW_DATA AS 
(
    SELECT dsd.brand_id, Fisc_Wk_end_date , rest_id , fn_measure_id, SALE_USD_AMOUNT 
    FROM IDS_{{params.env}}.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
    INNER JOIN IDS_{{params.env}}.TXN_BV.FN_SYSTEM_BV s 
    ON s.FN_SYSTEM_CATEGORY_CODE = 'GL'	AND s.fn_system_id = dsd.fn_system_id
    inner join CTE_PARAMETERS p 
    on  p.Brand_id = dsd.brand_id and  dsd.Fisc_Wk_end_date  BETWEEN p.ST_DATE   AND p.END_DATE
)
, CTE_CALENDAR AS
(
SELECT  p.brand_id, r.rest_id, d.CAL_DATE,  FN_MEASURE_ID , measure_name, default_sort_id, MEASURE_CATEGORY_NAME
   , Fisc_yr_nbr,Fisc_yr_start_date,Fisc_yr_End_date,Fisc_period_nbr,Fisc_period_start_date
    ,Fisc_period_end_date,Fisc_wk_nbr,Fisc_Wk_end_date,Fisc_Wk_start_date
    FROM "IDH_{{params.env}}"."D_REF"."DATE" d 
    JOIN CTE_PARAMETERS p on  d.Fisc_Wk_end_date  BETWEEN p.ST_DATE   AND p.END_DATE
    JOIN CTE_REST r
    JOIN IDS_{{params.env}}.TXN.FN_BRAND_MEASURE BM 
    on p.brand_id = bm.brand_id and bm.SALES_RECON_REPORT_DISPLAY_IND = true
    JOIN IDS_{{params.env}}.TXN_BV.FN_MEASURE_BV USING (FN_MEASURE_ID )
 WHERE CAL_DATE = Fisc_Wk_end_date
)
SELECT MAX(d.Fisc_Wk_end_date) AS  Fisc_Wk_end_date
    ,d.brand_id brand_id
    ,d.rest_id AS rest_id
    ,d.measure_name
    ,MAX(d.default_sort_id) AS measure_sort_id
    ,MAX(d.measure_category_name) as measure_category_name
    ,MAX(CASE WHEN dsd.Fisc_Wk_end_date IS NULL THEN TRUE ELSE FALSE	END) imputed_ind
    ,sum(coalesce(dsd.sale_usd_amount, 0)) AS sale_usd_amount
    ,MAX(d.Fisc_yr_nbr              ) AS  Fisc_yr_nbr            
    ,MAX(d.Fisc_yr_start_date       ) AS  Fisc_yr_start_date
    ,MAX(d.Fisc_yr_End_date         ) AS  Fisc_yr_End_date
    ,MAX(d.Fisc_period_nbr          ) AS  Fisc_period_nbr
    ,MAX(d.Fisc_period_start_date   ) AS  Fisc_period_start_date    
    ,MAX(d.Fisc_period_end_date     ) AS  Fisc_period_end_date    
    ,MAX(d.Fisc_wk_nbr              ) AS  Fisc_wk_nbr
    ,MAX(d.Fisc_Wk_start_date       ) AS  Fisc_Wk_start_date
    FROM CTE_CALENDAR d 
    LEFT OUTER JOIN CTE_VIEW_DATA  dsd ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date  
    AND d.fn_measure_id = dsd.fn_measure_id   
    AND d.rest_id = dsd.rest_id
    GROUP BY d.Fisc_Wk_end_date ,d.brand_id	, d.rest_id	,measure_name;

--BWW POS Daily view
create or replace view IDH_{{params.env}}.D_FINANCE.POS_DAILY_SALES_MEASURE_BWW(
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
WITH CTE_PARAMETERS as
(
    SELECT 'bww' brand_id, CURRENT_DATE - 120 ST_DATE, CURRENT_DATE END_DATE 
)
, CTE_REST as
(
    --to make sure we are reading all rest id irrespective of System type
    SELECT Distinct dsd.brand_id, rest_id
    FROM IDS_{{params.env}}.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
    INNER JOIN CTE_PARAMETERS p
    WHERE dsd.brand_id =p.brand_id and dsd.BUSINESS_DATE BETWEEN p.ST_DATE AND p.END_DATE
)
, CTE_VIEW_DATA AS 
(
    SELECT dsd.brand_id, business_date , rest_id , fn_measure_id, SALE_USD_AMOUNT 
    FROM IDS_{{params.env}}.TXN_BV.FN_DAILY_REV_MEASURE_BV dsd
    INNER JOIN IDS_{{params.env}}.TXN_BV.FN_SYSTEM_BV s 
    ON s.FN_SYSTEM_CATEGORY_CODE = 'POS'	AND s.fn_system_id = dsd.fn_system_id
    inner join CTE_PARAMETERS p 
    on  p.Brand_id = dsd.brand_id and  dsd.BUSINESS_DATE  BETWEEN p.ST_DATE   AND p.END_DATE
)
, CTE_CALENDAR AS
(
    SELECT  p.brand_id, r.rest_id, d.CAL_DATE,  FN_MEASURE_ID , measure_name, default_sort_id, MEASURE_CATEGORY_NAME
   , Fisc_yr_nbr,Fisc_yr_start_date,Fisc_yr_End_date,Fisc_period_nbr,Fisc_period_start_date
    ,Fisc_period_end_date,Fisc_wk_nbr,Fisc_Wk_end_date,Fisc_Wk_start_date
    FROM "IDH_{{params.env}}"."D_REF"."DATE" d
    JOIN CTE_PARAMETERS p on  d.CAL_DATE  BETWEEN p.ST_DATE   AND p.END_DATE
    JOIN CTE_REST r
    JOIN IDS_{{params.env}}.TXN.FN_BRAND_MEASURE BM 
    on p.brand_id = bm.brand_id and bm.SALES_RECON_REPORT_DISPLAY_IND = true
    JOIN IDS_{{params.env}}.TXN_BV.FN_MEASURE_BV USING (FN_MEASURE_ID )
)
    SELECT d.cal_date AS business_date
    ,d.brand_id brand_id
    ,d.rest_id AS rest_id
    ,d.measure_name
    ,MAX(d.default_sort_id) AS measure_sort_id
    ,MAX(d.measure_category_name) as measure_category_name
    ,MAX(CASE WHEN dsd.business_date IS NULL THEN TRUE ELSE FALSE	END) imputed_ind
    ,sum(coalesce(dsd.sale_usd_amount, 0)) AS sale_usd_amount
    ,MAX(Fisc_yr_nbr              ) AS  Fisc_yr_nbr            
    ,MAX(Fisc_yr_start_date       ) AS  Fisc_yr_start_date
    ,MAX(Fisc_yr_End_date         ) AS  Fisc_yr_End_date
    ,MAX(Fisc_period_nbr          ) AS  Fisc_period_nbr
    ,MAX(Fisc_period_start_date   ) AS  Fisc_period_start_date    
    ,MAX(Fisc_period_end_date     ) AS  Fisc_period_end_date    
    ,MAX(Fisc_wk_nbr              ) AS  Fisc_wk_nbr
    ,MAX(Fisc_Wk_start_date       ) AS  Fisc_Wk_start_date
    ,MAX(Fisc_Wk_end_date         ) AS  Fisc_Wk_end_date
    FROM CTE_CALENDAR d 
    LEFT OUTER JOIN CTE_VIEW_DATA  dsd ON d.cal_date = dsd.business_date  
    AND d.fn_measure_id = dsd.fn_measure_id   
    AND d.rest_id = dsd.rest_id
    GROUP BY d.cal_date ,d.brand_id	, d.rest_id	,measure_name;	

--BWW POS Weekly view
create or replace view IDH_{{params.env}}.D_FINANCE.POS_WEEKLY_SALES_MEASURE_BWW(
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
WITH CTE_PARAMETERS as
(
    SELECT 'bww' brand_id, CURRENT_DATE - 120 ST_DATE, CURRENT_DATE END_DATE 
)
, CTE_REST as
(
    --to make sure we are reading all rest id irrespective of System type
    SELECT Distinct dsd.brand_id, rest_id
    FROM IDS_{{params.env}}.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
    INNER JOIN CTE_PARAMETERS p
    WHERE dsd.brand_id =p.brand_id and dsd.Fisc_Wk_end_date BETWEEN p.ST_DATE AND p.END_DATE
)
, CTE_VIEW_DATA AS 
(
    SELECT dsd.brand_id, Fisc_Wk_end_date , rest_id , fn_measure_id, SALE_USD_AMOUNT 
    FROM IDS_{{params.env}}.TXN_BV.FN_WEEKLY_REV_MEASURE_BV dsd
    INNER JOIN IDS_{{params.env}}.TXN_BV.FN_SYSTEM_BV s 
    ON s.FN_SYSTEM_CATEGORY_CODE = 'POS'	AND s.fn_system_id = dsd.fn_system_id
    inner join CTE_PARAMETERS p 
    on  p.Brand_id = dsd.brand_id and  dsd.Fisc_Wk_end_date  BETWEEN p.ST_DATE   AND p.END_DATE
)
, CTE_CALENDAR AS
(
SELECT  p.brand_id, r.rest_id, d.CAL_DATE,  FN_MEASURE_ID , measure_name, default_sort_id, MEASURE_CATEGORY_NAME
   , Fisc_yr_nbr,Fisc_yr_start_date,Fisc_yr_End_date,Fisc_period_nbr,Fisc_period_start_date
    ,Fisc_period_end_date,Fisc_wk_nbr,Fisc_Wk_end_date,Fisc_Wk_start_date
    FROM "IDH_{{params.env}}"."D_REF"."DATE" d 
    JOIN CTE_PARAMETERS p on  d.Fisc_Wk_end_date  BETWEEN p.ST_DATE   AND p.END_DATE
    JOIN CTE_REST r
    JOIN IDS_{{params.env}}.TXN.FN_BRAND_MEASURE BM 
    on p.brand_id = bm.brand_id and bm.SALES_RECON_REPORT_DISPLAY_IND = true
    JOIN IDS_{{params.env}}.TXN_BV.FN_MEASURE_BV USING (FN_MEASURE_ID )
 WHERE CAL_DATE = Fisc_Wk_end_date
)
SELECT MAX(d.Fisc_Wk_end_date) AS  Fisc_Wk_end_date
    ,d.brand_id brand_id
    ,d.rest_id AS rest_id
    ,d.measure_name
    ,MAX(d.default_sort_id) AS measure_sort_id
    ,MAX(d.measure_category_name) as measure_category_name
    ,MAX(CASE WHEN dsd.Fisc_Wk_end_date IS NULL THEN TRUE ELSE FALSE	END) imputed_ind
    ,sum(coalesce(dsd.sale_usd_amount, 0)) AS sale_usd_amount
    ,MAX(d.Fisc_yr_nbr              ) AS  Fisc_yr_nbr            
    ,MAX(d.Fisc_yr_start_date       ) AS  Fisc_yr_start_date
    ,MAX(d.Fisc_yr_End_date         ) AS  Fisc_yr_End_date
    ,MAX(d.Fisc_period_nbr          ) AS  Fisc_period_nbr
    ,MAX(d.Fisc_period_start_date   ) AS  Fisc_period_start_date    
    ,MAX(d.Fisc_period_end_date     ) AS  Fisc_period_end_date    
    ,MAX(d.Fisc_wk_nbr              ) AS  Fisc_wk_nbr
    ,MAX(d.Fisc_Wk_start_date       ) AS  Fisc_Wk_start_date
    FROM CTE_CALENDAR d 
    LEFT OUTER JOIN CTE_VIEW_DATA  dsd ON d.Fisc_Wk_end_date = dsd.Fisc_Wk_end_date  
    AND d.fn_measure_id = dsd.fn_measure_id   
    AND d.rest_id = dsd.rest_id
    GROUP BY d.Fisc_Wk_end_date ,d.brand_id	, d.rest_id	,measure_name;