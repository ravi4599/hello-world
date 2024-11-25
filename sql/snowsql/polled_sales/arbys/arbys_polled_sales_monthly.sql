BEGIN TRANSACTION;

delete  FROM IDS_{{target_env}}.FINANCE_ACCTG.FRANCHISEE_POLLED_SALES_SNAPSHOT WHERE snapshot_date < (CURRENT_DATE - INTERVAL '6 months') and BRAND_id='arbys';

SET load_dt =  CURRENT_TIMESTAMP;

insert into IDS_{{target_env}}.FINANCE_ACCTG.FRANCHISEE_POLLED_SALES_SNAPSHOT	   
SELECT 'arbys' AS BRAND_ID
	,TO_CHAR(LAST_DAY(BUSINESS_DATE), 'YYYY-MM-DD') AS REPORTING_PERIOD
	,OWNER_ID AS CUSTOMER_NUMBER
	,FRANCHISEE_NAME AS CUSTOMER_NAME
	,LPAD(T.REST_ID, 6, '0') AS REST_ID
	,'USD' LOCAL_CURRENCY
	,CAST(SUM(DERIVED_NET_AMT) AS STRING) LOCAL_NET_SALES_RETAIL
	,CAST(SUM(TRANS_CNT) AS STRING) TRANSACTION_COUNT_RETAIL
	,0 --CAST(SUM(DERIVED_GROSS_AMT) AS STRING) 
	LOCAL_GROSS_SALES
	,'' AS LOCAL_NET_SALES_WHOLESALE
	,'' AS TRANSACTION_COUNT_WHOLESALE
	,'MONTHLY' AS REPORTING_FREQUENCY,
 DAY(LAST_DAY(CURRENT_date))  expected_sales_days,
 count(DISTINCT 
  business_date) ACTUAL_SALES_DAYS,
  current_date snapshot_date,
  T.source_system_name,
  to_number(to_varchar($load_dt, 'yyyyMMddHHmmss')) AS LOAD_ID,$load_dt LOAD_DTTM,to_number(to_varchar($load_dt, 'yyyyMMddHHmmss')) AS UPDATE_ID,$load_dt UPDATE_DTTM
FROM IDH_{{source_env}}.D_TRANS.TRANS T
INNER JOIN IDH_{{source_env}}.D_LOC.REST R ON T.REST_ID = R.REST_ID
	AND T.BRAND_ID = R.BRAND_ID
WHERE T.BRAND_ID = 'arbys'
	AND R.FRAN_IND = true  AND (SALES_THRESHOLD_IND = TRUE OR SALES_THRESHOLD_IND IS NULL)
	AND BUSINESS_DATE BETWEEN TO_DATE(DATE_TRUNC('MONTH', ADD_MONTHS('{{ var.json.arbys_polled_sales_monthly.load_dt | default((macros.datetime.today() ).strftime('%Y-%m-%d') ) }}', - 1)))
		AND LAST_DAY(ADD_MONTHS('{{ var.json.arbys_polled_sales_monthly.load_dt | default((macros.datetime.today() ).strftime('%Y-%m-%d') ) }}', - 1))	GROUP BY T.BRAND_ID
	,LAST_DAY(BUSINESS_DATE)
	,OWNER_ID
	,FRANCHISEE_NAME
	,T.REST_ID,
	T.source_system_name,
  to_number(to_varchar($load_dt, 'yyyyMMddHHmmss')) ,$load_dt,to_number(to_varchar($load_dt, 'yyyyMMddHHmmss')),$load_dt;

	
	COPY INTO @IDH_{{target_env}}.D_FINANCE.STAGE_FOR_ARBYS_POLLED_SALES/ARBYS_UDP_Finance_PolledSales_{{ var.json.baskin_polled_sales_monthly.load_dt | default((macros.datetime.today() ).strftime('%Y%m%d') ) }}_MONTHLY.dat
FROM (
SELECT 'ARG' as brand,to_char(CAST(REPORTING_PERIOD AS date),'MM-DD-YYYY'), CUSTOMER_NUMBER, CUSTOMER_NAME, rest_id as UNIT, LOCAL_CURRENCY, LOCAL_NET_SALES_RETAIL, TRANSACTION_COUNT_RETAIL, LOCAL_GROSS_SALES, LOCAL_NET_SALES_WHOLESALE, TRANSACTION_COUNT_WHOLESALE, REPORTING_FREQUENCY FROM IDS_{{target_env}}.FINANCE_ACCTG.FRANCHISEE_POLLED_SALES_SNAPSHOT  WHERE SNAPSHOT_DATE = current_date AND BRAND_ID = 'arbys' AND REPORTING_FREQUENCY = 'MONTHLY')
OVERWRITE = TRUE 
single = true
FILE_FORMAT = (FIELD_DELIMITER = '|' TYPE = csv NULL_IF = ('NULL', 'null') EMPTY_FIELD_AS_NULL = false  compression='NONE')
HEADER=true;

COMMIT;