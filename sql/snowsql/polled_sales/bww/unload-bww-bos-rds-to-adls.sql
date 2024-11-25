USE WAREHOUSE {{params.warehouse}};
 
SET start_load_dt =  '{{params.load_start_dt}}';
 
COPY INTO @IDH_{{params.env}}.{{params.schema}}.stage_for_bww_polled_sales/{{params.file_name}} 
from 
(
SELECT 
'BWW' AS BRAND,
TO_CHAR(fiscal_week_end_dt,'MM-DD-YYYY') AS REPORTING_PERIOD,
OWNER_ID AS CUSTOMER_NUMBER, 
FRANCHISEE_NAME AS CUSTOMER_NAME,
LPAD(T.REST_ID,6,'0') AS UNIT,
'USD' AS LOCAL_CURRENCY,
SUM(DERIVED_NET_AMT) AS LOCAL_NET_SALES_RETAIL,
SUM(TRANS_CNT) AS TRANSACTION_COUNT_RETAIL,
SUM(DERIVED_GROSS_AMT) LOCAL_GROSS_SALES,
'' AS LOCAL_NET_SALES_WHOLESALE, 
'' AS TRANSACTION_COUNT_WHOLESALE,
'WEEKLY' AS REPORTING_FREQUENCY
FROM IDH_{{params.source_env}}.{{params.schema}}.TRANS T
INNER JOIN IDH_{{params.source_env}}.D_LOC.REST R 
ON T.REST_ID = R.REST_ID AND T.BRAND_ID = R.BRAND_ID
inner join IDS_{{params.source_env}}.INT_REF_BV."DATE_DIM_BV" cal_date on T.Business_date = cal_date.calendar_dt
WHERE T.BRAND_ID = 'bww' 
AND R.OWNERSHIP_TYPE = 'Franchised'
AND VOID_IND = 'FALSE'
AND
BUSINESS_DATE
BETWEEN
to_date(date_trunc('Month',(dateadd('Month',-1,$start_load_dt))))
- dayofweek(date_trunc('month',(dateadd('Month',-1,$start_load_dt)))) + 1 AND
to_date(last_day(dateadd('Month',-1,$start_load_dt))) - dayofweek(last_day(dateadd('Month',-1,$start_load_dt)))
AND DAY(TO_DATE($start_load_dt)) = 01
GROUP BY 
T.BRAND_ID,
REPORTING_PERIOD,
OWNER_ID, 
FRANCHISEE_NAME,
T.REST_ID
)
OVERWRITE = TRUE 
single = true
FILE_FORMAT = (FIELD_DELIMITER = '|' TYPE = csv NULL_IF = ('NULL', 'null') EMPTY_FIELD_AS_NULL = false  compression='NONE')
HEADER=true;
