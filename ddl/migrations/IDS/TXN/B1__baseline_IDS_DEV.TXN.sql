create TABLE IF NOT EXISTS AUDITED_DAILY_SALES (
	STORE_ID VARCHAR(16777216) NOT NULL COMMENT 'Number that uniquely identifies the store',
	BUSINESS_DT DATE NOT NULL COMMENT 'Date of sales',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Identifier for store brand',
	TRANSACTION_CNT NUMBER(38,0) COMMENT 'Count of sales transactions',
	NET_SALES_AMT NUMBER(18,2) COMMENT 'Net amount of sales',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XPKAUDITED_DAILY_SALES primary key (STORE_ID, BUSINESS_DT, BRAND_ID)
)COMMENT='Summarized audited sales  and transaction counts by brand/restaurant/day/source'
;
create TABLE IF NOT EXISTS DAILY_AUDIT_SALES_MAPPING (
	STORE_ID VARCHAR(16777216) NOT NULL COMMENT 'Number that uniquely identifies the store',
	BUSINESS_DT DATE NOT NULL COMMENT 'Date of sales',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Identifier for store brand',
	TRANSACTION_CNT NUMBER(38,0) COMMENT 'Count of sales transactions',
	NET_SALES_AMT NUMBER(18,2) COMMENT 'Net amount of sales',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.'
)COMMENT='Summarized audited sales  and transaction counts by brand/restaurant/day/source'
;
create TABLE IF NOT EXISTS DAILY_DAYPART_FLASH_COMPARABLE_SALES (
	BUSINESS_DT DATE NOT NULL COMMENT 'Date of sales transaction',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand associated with store \n\n',
	STORE_NBR VARCHAR(16777216) NOT NULL COMMENT 'Number that uniquely identifies the Store',
	CLOSED_HOUR_TM TIME(9) NOT NULL COMMENT 'Hour in which the order was closed (completed) in military time\n',
	IRB_DAYPART_NM VARCHAR(16777216) COMMENT 'Inspire enterprise daypart associated with the CLOSED_HOUR\n',
	BRAND_DAYPART_NM VARCHAR(16777216) COMMENT 'Brand specific daypart associated with the CLOSED_HOUR\n',
	DMA_NM VARCHAR(16777216) COMMENT 'Designated marketing area code name for the store\n\n',
	DMA_CD VARCHAR(16777216) COMMENT 'Designated marketing area code for the store',
	LEVEL1_NM VARCHAR(16777216) COMMENT '\t\nOps hierarchy L1 name',
	LEVEL2_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L2 name',
	LEVEL3_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L3 name',
	LEVEL4_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L4 name',
	LEVEL5_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L5 name',
	OWNERSHIP_TYP VARCHAR(16777216) COMMENT 'Type of store ownership',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'Address line 1 for the store\n\n',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Address line 2 for the store',
	CITY_NM VARCHAR(16777216) COMMENT 'City for the store',
	ZIP_CD VARCHAR(16777216) COMMENT 'Zip code for the store',
	STATE_CD VARCHAR(16777216) COMMENT 'State of the store',
	COUNTRY_CD VARCHAR(16777216) COMMENT 'Country of the store',
	FISCAL_YEAR_NBR NUMBER(38,0) COMMENT 'Fiscal year number for business date',
	FISCAL_QUARTER_NBR NUMBER(38,0) COMMENT 'Fiscal quarter number for business date',
	FISCAL_PERIOD_NBR NUMBER(38,0) COMMENT 'Fiscal period number for business date\n',
	FISCAL_WEEK_NBR NUMBER(38,0) COMMENT 'Fiscal week number for business date',
	DAY_OF_WEEK_NM VARCHAR(16777216) COMMENT 'Name of day for business date',
	CURRENT_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for business date',
	CURRENT_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for business date',
	CURRENT_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Current year net sales amount for the aggregate level',
	CURRENT_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of transactions for the aggregate level',
	CURRENT_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for the current year business date',
	CURRENT_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for the current year business date',
	CURRENT_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Current year comp sales amount for the aggregate level',
	CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of comp transactions for the aggregate level',
	CURRENT_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales on the current year business date',
	CURRENT_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for the current year business date',
	FISCAL_LAST_YEAR_DT DATE COMMENT 'Last year fiscal comparable date associated with the current year business date for aggregate level.',
	FISCAL_LAST_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_AVERAGE_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year fiscal comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_DT DATE COMMENT 'Last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_DT DATE COMMENT '2 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_2YEAR_COMPARABLE_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_2YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_DT DATE COMMENT '3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKDAILY_DAYPART_FLASH_COMPARABLE_SALES primary key (BUSINESS_DT, BRAND_ID, STORE_NBR, CLOSED_HOUR_TM)
)COMMENT='The Inspire Daily Flash view is a materialized view populated from the Inspire Daily Flash function and supports daily flash sales analytics for a business date, by brand and store. This materialized view has been created to enhance performance of the queries against the dataset. '
;
create TABLE IF NOT EXISTS DAILY_FLASH_COMPARABLE_SALES (
	BUSINESS_DT DATE NOT NULL COMMENT 'Date of sales transaction',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand associated with store \n\n',
	STORE_NBR VARCHAR(16777216) NOT NULL COMMENT 'Number that uniquely identifies the Store',
	DMA_NM VARCHAR(16777216) COMMENT 'Designated marketing area code name for the store\n\n',
	DMA_CD VARCHAR(16777216) COMMENT 'Designated marketing area code for the store',
	LEVEL1_NM VARCHAR(16777216) COMMENT '\t\nOps hierarchy L1 name',
	LEVEL2_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L2 name',
	LEVEL3_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L3 name',
	LEVEL4_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L4 name',
	LEVEL5_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L5 name',
	OWNERSHIP_TYP VARCHAR(16777216) COMMENT 'Type of store ownership',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'Address line 1 for the store\n\n',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Address line 2 for the store',
	CITY_NM VARCHAR(16777216) COMMENT 'City for the store',
	ZIP_CD VARCHAR(16777216) COMMENT 'Zip code for the store',
	STATE_CD VARCHAR(16777216) COMMENT 'State of the store',
	COUNTRY_CD VARCHAR(16777216) COMMENT 'Country of the store',
	FISCAL_YEAR_NBR NUMBER(38,0) COMMENT 'Fiscal year number for business date',
	FISCAL_QUARTER_NBR NUMBER(38,0) COMMENT 'Fiscal quarter number for business date',
	FISCAL_PERIOD_NBR NUMBER(38,0) COMMENT 'Fiscal period number for business date\n',
	FISCAL_WEEK_NBR NUMBER(38,0) COMMENT 'Fiscal week number for business date',
	DAY_OF_WEEK_NM VARCHAR(16777216) COMMENT 'Name of day for business date',
	CURRENT_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for business date',
	CURRENT_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for business date',
	CURRENT_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Current year net sales amount for the aggregate level',
	CURRENT_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of transactions for the aggregate level',
	CURRENT_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for the current year business date',
	CURRENT_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for the current year business date',
	CURRENT_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Current year comp sales amount for the aggregate level',
	CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of comp transactions for the aggregate level',
	CURRENT_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales on the current year business date',
	CURRENT_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for the current year business date',
	FISCAL_LAST_YEAR_DT DATE COMMENT 'Last year fiscal comparable date associated with the current year business date for aggregate level.',
	FISCAL_LAST_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_AVERAGE_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year fiscal comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_DT DATE COMMENT 'Last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_DT DATE COMMENT '2 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_2YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_2YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_DT DATE COMMENT '3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKDAILY_FLASH_COMPARABLE_SALES primary key (BUSINESS_DT, BRAND_ID, STORE_NBR)
)COMMENT='The Inspire Daily Flash view is a materialized view populated from the Inspire Daily Flash function and supports daily flash sales analytics for a business date, by brand and store. This materialized view has been created to enhance performance of the queries against the dataset. '
;
create TABLE IF NOT EXISTS DAILY_ORDER_FULFILMENT_FLASH_COMPARABLE_SALES (
	BUSINESS_DT DATE NOT NULL COMMENT 'Date of sales transaction',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand associated with store \n\n',
	STORE_NBR VARCHAR(16777216) NOT NULL COMMENT 'Number that uniquely identifies the Store',
	CHANNEL_ID VARCHAR(16777216) NOT NULL COMMENT 'Unique order fulfillment channel identifier\n',
	ORDER_CHANNEL_HIERARCHY_LEVEL1_NM VARCHAR(16777216) COMMENT 'Level 1 channel hierarchy for an order at a restaurant.\n',
	ORDER_CHANNEL_HIERARCHY_LEVEL2_NM VARCHAR(16777216) COMMENT 'Level 2 channel hierarchy for an order at a restaurant.\n',
	ORDER_CHANNEL_HIERARCHY_LEVEL3_NM VARCHAR(16777216) COMMENT 'Level 3 channel hierarchy for an order at a restaurant.\n',
	FULFILMENT_CHANNEL_HIERARCHY_LEVEL1_NM VARCHAR(16777216) COMMENT 'Level 1 fulfillment channel hierarchy for an order at a restaurant.\n',
	FULFILMENT_CHANNEL_HIERARCHY_LEVEL2_NM VARCHAR(16777216) COMMENT 'Level 2 fulfillment channel hierarchy for an order at a restaurant.\n',
	FULFILMENT_CHANNEL_HIERARCHY_LEVEL3_NM VARCHAR(16777216) COMMENT 'Level 3 fulfillment channel hierarchy for an order at a restaurant.\n',
	DMA_NM VARCHAR(16777216) COMMENT 'Designated marketing area code name for the store\n\n',
	DMA_CD VARCHAR(16777216) COMMENT 'Designated marketing area code for the store',
	LEVEL1_NM VARCHAR(16777216) COMMENT '\t\nOps hierarchy L1 name',
	LEVEL2_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L2 name',
	LEVEL3_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L3 name',
	LEVEL4_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L4 name',
	LEVEL5_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L5 name',
	OWNERSHIP_TYP VARCHAR(16777216) COMMENT 'Type of store ownership',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'Address line 1 for the store\n\n',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Address line 2 for the store',
	CITY_NM VARCHAR(16777216) COMMENT 'City for the store',
	ZIP_CD VARCHAR(16777216) COMMENT 'Zip code for the store',
	STATE_CD VARCHAR(16777216) COMMENT 'State of the store',
	COUNTRY_CD VARCHAR(16777216) COMMENT 'Country of the store',
	FISCAL_YEAR_NBR NUMBER(38,0) COMMENT 'Fiscal year number for business date',
	FISCAL_QUARTER_NBR NUMBER(38,0) COMMENT 'Fiscal quarter number for business date',
	FISCAL_PERIOD_NBR NUMBER(38,0) COMMENT 'Fiscal period number for business date\n',
	FISCAL_WEEK_NBR NUMBER(38,0) COMMENT 'Fiscal week number for business date',
	DAY_OF_WEEK_NM VARCHAR(16777216) COMMENT 'Name of day for business date',
	CURRENT_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for business date',
	CURRENT_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for business date',
	CURRENT_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Current year net sales amount for the aggregate level',
	CURRENT_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of transactions for the aggregate level',
	CURRENT_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for the current year business date',
	CURRENT_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for the current year business date',
	CURRENT_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Current year comp sales amount for the aggregate level',
	CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of comp transactions for the aggregate level',
	CURRENT_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales on the current year business date',
	CURRENT_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for the current year business date',
	FISCAL_LAST_YEAR_DT DATE COMMENT 'Last year fiscal comparable date associated with the current year business date for aggregate level.',
	FISCAL_LAST_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_AVERAGE_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year fiscal comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_DT DATE COMMENT 'Last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_DT DATE COMMENT '2 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_2YEAR_COMPARABLE_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_2YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_DT DATE COMMENT '3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKDAILY_ORDER_FULFILMENT_FLASH_COMPARABLE_SALES primary key (BUSINESS_DT, BRAND_ID, STORE_NBR, CHANNEL_ID)
)COMMENT='The Inspire Daily Flash view is a materialized view populated from the Inspire Daily Flash function and supports daily flash sales analytics for a business date, by brand and store. This materialized view has been created to enhance performance of the queries against the dataset. '
;
create TABLE IF NOT EXISTS DIGITAL_ORDER (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BUSINESS_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Business Datetime is the day and time that location has received the order.',
	ORDER_CHANNEL_ID VARCHAR(16777216) COMMENT 'Order Channel Identifier is a unique identifier that specifies a unique brand and channel combination. The data is sourced from MDM. For example, Order Channel Id=cc9d59ae59ba8477b59b220163520c4f, Source Order Channel Code=curbside, Fulfillment Channel Code=curbside.',
	ORDER_CHANNEL_CD VARCHAR(16777216) NOT NULL COMMENT 'Source Order Channel Code specifies how an order was submitted to a location. For example, WEB for browser based orders or IOSAPP for order submitted via iphone or ipad.',
	FULFILLMENT_CHANNEL_CD VARCHAR(16777216) COMMENT 'Source Fulfillment Channel Code specifies how an orderfulfilled.For example, Curbside or Pickup.',
	FULFILLMENT_CHANNEL_SUB_CD VARCHAR(16777216) NOT NULL COMMENT 'Fulfillment Channel Type Code adds additional context on the delivery of an order. Sample values, ASAP, Now, and Future.',
	ORDER_IDEMPOTENT_ID VARCHAR(16777216) COMMENT 'Order Idempotent Identifier . Sample Value: c8880f45-0bd3-4198-bed0-49c18ec9d780.',
	ORDER_STATUS_CD VARCHAR(16777216) COMMENT 'Order Status Code . Sample Value: POS_SUBMITTED.',
	ORDER_DISPLAY_STATUS_TXT VARCHAR(16777216) COMMENT 'Order Display Status Text. Sample Value: Order Received.',
	ORDER_NM VARCHAR(16777216) COMMENT 'Order Name . Sample Value: PHONE NOT SPECIFIED .',
	ORDER_EXPECTED_PICKUP_DTTM TIMESTAMP_NTZ(9) COMMENT 'Order Expected Pickup Datetime. Sample Value: 2021-07-09T11:00:00-05:00.',
	ORDER_FULFILLMENT_DTTM TIMESTAMP_NTZ(9) COMMENT 'Order Fulfillment Datetime . Sample Value: 2021-07-09T11:00:00-05:00.',
	POS_ORDER_ID VARCHAR(16777216) COMMENT 'Pos Order Identifier is used to link a digitial order to the pos within a store. Sample Value: 3730.',
	POS_STATUS_CD VARCHAR(16777216) COMMENT 'Pos Status Code. Sample Value: Success.',
	POS_RESULT_CD VARCHAR(16777216) COMMENT 'Pos Result Code . Sample Value: 0.',
	POS_MESSAGE_TXT VARCHAR(16777216) COMMENT 'Pos Message Text. Sample Value: Success.',
	ORDER_SUB_TOTAL_AMT NUMBER(18,2) COMMENT 'Order Sub Total Amount. Sample Value: 9.99.',
	ORDER_TOTAL_TAX_AMT NUMBER(18,2) COMMENT 'Order Total Tax Amount. Sample Value: 0.71.',
	ORDER_TIP_AMT NUMBER(18,2) COMMENT 'Order Tip Amount. Sample Value: 0.',
	ORDER_DISCOUNT_AMT NUMBER(18,2) COMMENT 'Order Discount Amount. Sample Value: 2.79.',
	ORDER_DISCOUNT_PRICE_AMT NUMBER(18,2),
	ORDER_TOTAL_AMT NUMBER(18,2) COMMENT 'Order Total Price Amount is the summation of the Order Sub Total, Tax, and Tip Amount. Sample Value: 10.70.',
	ORDER_DETAIL_TXT VARCHAR(16777216) COMMENT 'Order Detail Text . Sample Value: Confirmed Order.',
	DELIVERY_FEE_AMOUUNT NUMBER(18,2),
	SERVER_EMPLOYEE_ID VARCHAR(16777216),
	TABLE_NBR VARCHAR(16777216),
	GUEST_CNT NUMBER(38,0),
	CHECK_NBR VARCHAR(16777216),
	DELIVERY_DTTM TIMESTAMP_NTZ(9),
	PAYMENT_URL VARCHAR(16777216),
	TALLY_TM TIME(9),
	IDP_CUST_ID VARCHAR(16777216) COMMENT 'Customer Identifier uniquely identifies on an order. Values may vary based on channel. Sample Value: ad90c54a-06cc-4dd9-b3cc-734e63a2b021.',
	CUST_MEMBERSHIP_NBR VARCHAR(16777216),
	CUST_FIRST_NM VARCHAR(16777216) COMMENT 'Customer First Name . Sample Value: Mikhail.',
	CUST_LAST_NM VARCHAR(16777216) COMMENT 'Customer Last Name . Sample Value: Krestelev.',
	CUST_PHONE_NBR VARCHAR(16777216) COMMENT 'Customer Phone Number. Sample Value: 2233222232.',
	CUST_EMAIL VARCHAR(16777216) COMMENT 'Customer Email . Sample Value: mkrestelev@inspirebrands.com.',
	CUST_ADR_LINE_1_TXT VARCHAR(16777216) COMMENT 'Customer Address Line 1 Text. Sample Value: 5500 Wayzata Blvd..',
	CUST_ADR_LINE_2_TXT VARCHAR(16777216) COMMENT 'Customer Address Line 2 Text. Sample Value: Ste 13.',
	CUST_CTY_NM VARCHAR(16777216) COMMENT 'Customer City Name. Sample Value: Minneapolis.',
	CUST_ST_CD VARCHAR(16777216) COMMENT 'Customer State Code. Sample Value: MN.',
	CUST_ZIP_CD VARCHAR(16777216) COMMENT 'Customer Zip Code . Sample Value: 55416.',
	LOC_NM VARCHAR(16777216) COMMENT 'Location Name . Sample Value: Buzztime QA 6.7.',
	LOC_PHONE_NBR VARCHAR(16777216) COMMENT 'Location Phone Number. Sample Value: 612-866-9316.',
	LOC_EMAIL VARCHAR(16777216) COMMENT 'Customer Email . Sample Value: mkrestelev@inspirebrands.com.',
	LOC_ADR_LINE_1_TXT VARCHAR(16777216) COMMENT 'Location Address Line 1 Text. Sample Value: 5500 Wayzata Blvd..',
	LOC_ADR_LINE_2_TXT VARCHAR(16777216) COMMENT 'Location Address Line 2 Text. Sample Value: Ste 13.',
	LOC_CTY_NM VARCHAR(16777216) COMMENT 'Location City Name. Sample Value: Minneapolis.',
	LOC_ST_CD VARCHAR(16777216) COMMENT 'Location State Code. Sample Value: MN.',
	LOC_ZIP_CD VARCHAR(16777216) COMMENT 'Location Zip Code . Sample Value: 55416.',
	LOC_FRAUD_CHECK_IND BOOLEAN,
	ORDER_CORRELATION_ID VARCHAR(16777216) COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	FULLFILLMENT_UNIQUE_IDENTIFER NUMBER(38,0),
	FULFILLMENT_ID NUMBER(38,0),
	FULFILLMENT_TYP_CD VARCHAR(16777216),
	FULFILLMENT_STATUS_URL VARCHAR(16777216),
	DELIVERY_CONTACT_FIRST_NM VARCHAR(16777216) COMMENT 'Delivery Contact First Name . Sample Value: John.',
	DELIVERY_CONTACT_LAST_NM VARCHAR(16777216) COMMENT 'Delivery Contact Last Name . Sample Value: Wick.',
	DELIVERY_CONTACT_PHONE_NBR VARCHAR(16777216) COMMENT 'Delivery Contact Phone Number. Sample Value: 15012323767.',
	DELIVERY_CONTACT_EMAIL VARCHAR(16777216) COMMENT 'Delivery Contact Email . Sample Value: mkrestelev@inspirebrands.com.',
	DELIVERY_ADR_LINE_1_TXT VARCHAR(16777216) COMMENT 'Delivery Address Line 1 Text. Sample Value: 5500 Wayzata Blvd..',
	DELIVERY_ADR_LINE_2_TXT VARCHAR(16777216) COMMENT 'Delivery Address Line 2 Text. Sample Value: Ste 13.',
	DELIVERY_CTY_NM VARCHAR(16777216) COMMENT 'Delivery City Name. Sample Value: Minneapolis.',
	DELIVERY_ST_CD VARCHAR(16777216) COMMENT 'Delivery State Code. Sample Value: MN.',
	DELIVERY_ZIP_CD VARCHAR(16777216) COMMENT 'Delivery Zip Code . Sample Value: 55416.',
	DELIVERY_PARTNER_NM VARCHAR(16777216) COMMENT 'Delivery Partner Name. Sample Value: DoorDash.',
	DELIVERY_VEHICLE_DESC VARCHAR(16777216),
	DELIVERY_STATUS_TXT VARCHAR(16777216),
	CUST_FULFILLMENT_INSTRUCTION_TXT VARCHAR(16777216),
	ORDER_METADATA VARCHAR(16777216),
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CUST_PICKUP_INSTRUCTION_TXT VARCHAR(16777216),
	constraint XPKDIGITAL_ORDER primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, SOURCE_SYSTEM_NM),
	constraint XAK1DIGITAL_ORDER unique (BRAND_ID, ORDER_CORRELATION_ID, SOURCE_SYSTEM_NM)
)COMMENT='Digital Order contains an order for a location that was placed via a digital means, such as phone or web browser.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_CHANNEL_MAPPING (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	SOURCE_BRAND_ID VARCHAR(16777216) NOT NULL,
	SOURCE_ORDER_CHANNEL_CD VARCHAR(16777216) NOT NULL COMMENT 'Source Order Channel Code specifies how an order was submitted to a location. For example, WEB for browser based orders or IOSAPP for order submitted via iphone or ipad.',
	SOURCE_FULFILLMENT_CHANNEL_CD VARCHAR(16777216) NOT NULL COMMENT 'Source Fulfillment Channel Code specifies how an orderfulfilled.For example, Curbside or Pickup.',
	TARGET_ORDER_CHANNEL_ID VARCHAR(16777216) COMMENT 'Order Channel Identifier is a unique identifier that specifies a unique brand and channel combination.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XAK1DIGITAL_ORDER_CHANNEL_MAPPING unique (SOURCE_BRAND_ID, SOURCE_ORDER_CHANNEL_CD),
	constraint XPKDIGITAL_ORDER_CHANNEL_MAPPING primary key (SOURCE_SYSTEM_NM, SOURCE_BRAND_ID, SOURCE_ORDER_CHANNEL_CD, SOURCE_FULFILLMENT_CHANNEL_CD)
)COMMENT='Digitanl Order Channel Mapping contains record to map from a digital order to a UDP Order Channel. A Order Channel is the combination of the Soure of the Order as well as the Fulfillment Type of an Order.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_DISCOUNT (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	OMS_OFFER_CD VARCHAR(16777216) COMMENT 'Oms Offer Code . Sample Value: ca6b31cc-4849-493d-a6a0-12abb89a0757.',
	DISCOUNT_CERTIFICATE_ID VARCHAR(16777216) COMMENT 'Certificate Identifier . Sample Value: null.',
	DISCOUNT_CD VARCHAR(16777216) COMMENT 'Discount Code . Sample Value: null.',
	COMPLIMENTARY_CD VARCHAR(16777216) COMMENT 'Compensation Code . Sample Value: null.',
	DISCOUNT_SKU VARCHAR(16777216) COMMENT 'Sku . Sample Value: null.',
	COMPLIMENTARY_APPLIED_IND BOOLEAN COMMENT 'Compensation Applied Indicator. Sample Value: null.',
	DISCOUNT_NM VARCHAR(16777216) COMMENT 'Discount Name . Sample Value: DEV TEST -- Free Small Shake.',
	DISCOUNT_ID VARCHAR(16777216) NOT NULL COMMENT 'Discount Identifier . Sample Value: 1018131.',
	LOYALTY_ID VARCHAR(16777216) COMMENT 'Loyalty Identifier . Sample Value: null.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	constraint XPKDIGITAL_ORDER_DISCOUNT primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, DISCOUNT_ID, SOURCE_SYSTEM_NM),
	constraint R_45 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, SOURCE_SYSTEM_NM) references DIGITAL_ORDER(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,SOURCE_SYSTEM_NM)
)COMMENT='Digital Order Discount specifies the different discounts applied to an order.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	DISCOUNT_ID VARCHAR(16777216) NOT NULL COMMENT 'Discount Identifier . Sample Value: 1018131.',
	ITEM_DISCOUNT_AMT VARCHAR(16777216) COMMENT 'Item Discount Amount . Sample Value: 0.',
	MENU_ITEM_ID VARCHAR(16777216) COMMENT 'Menu Item Identifier . Sample Value: arb-itm-003-085. This is also know as Product Identifier',
	DISCOUNT_ITEM_QTY NUMBER(38,0) COMMENT 'Quantity . Sample Value: 1.',
	LINE_ITEM_NBR VARCHAR(16777216) NOT NULL,
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	constraint XPKDIGITAL_ORDER_DISCOUNT_APPLIED_ITEM primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, DISCOUNT_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM),
	constraint R_468 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, DISCOUNT_ID, SOURCE_SYSTEM_NM) references DIGITAL_ORDER_DISCOUNT(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,DISCOUNT_ID,SOURCE_SYSTEM_NM)
)COMMENT='Digital Order Discount Applied Item specifies for each item how much discount was applicable.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_LINE (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR VARCHAR(16777216) NOT NULL COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LINE_ITEM_ID VARCHAR(16777216) COMMENT 'Line Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	LINE_ITEM_DESC VARCHAR(16777216) COMMENT 'Item Description . Sample Value: Large Cheddar Cheese Curds.',
	LINE_ITEM_PRICE_AMT NUMBER(18,2) COMMENT 'Item Price Amount. Sample Value: 9.99.',
	LINE_ITEM_DISCOUNTED_AMT NUMBER(18,2),
	LINE_ITEM_PRODUCT_ID VARCHAR(16777216) COMMENT 'Product Identifier . Sample Value: SalesItem-3032.',
	LINE_ITEM_DESTINATION_CD VARCHAR(16777216) COMMENT 'Order Destination Code. Sample Value: 1.',
	LINE_ITEM_QTY NUMBER(38,0) COMMENT 'Order Quantity. Sample Value: 1.',
	SUGGESTED_SELL_RECOMMENDATION_ID VARCHAR(16777216) COMMENT 'Suggested Sell Recommendation Identifier uniquely identifies the recommendation that determine a set of products. The recommendation is based on a backet of goods and a set of recommendations from a recommendation file. Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	LINE_ITEM_PRODUCT_CATEGORY_NM VARCHAR(16777216) COMMENT 'Line Item Product Category Name specifies a grouping for an item. For example, Shake is the categorization for a Small Vanilla Shake.',
	LINE_ITEM_POS_ID NUMBER(38,0) COMMENT 'Line Item POS Identifier.  Sample value 640243388.',
	LINE_ITEM_COMPONENT_ID NUMBER(38,0),
	LINE_ITEM_AVAILABLE_IND BOOLEAN,
	LINE_ITEM_AVAILABLE_POS_ID NUMBER(38,0),
	LINE_ITEM_AVAILABLE_QTY NUMBER(38,0),
	LINE_ITEM_NOTE_TXT VARCHAR(16777216),
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKDIGITAL_ORDER_LINE primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM),
	constraint R_41 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, SOURCE_SYSTEM_NM) references DIGITAL_ORDER(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,SOURCE_SYSTEM_NM)
)COMMENT='Digital Order Line specifies a particualr item for an order.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_LINE_CHILD (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR VARCHAR(16777216) NOT NULL COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CHILD_ITEM_ID VARCHAR(16777216) NOT NULL COMMENT 'Child Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	CHILD_ITEM_DESC VARCHAR(16777216) COMMENT 'Child Description . Sample Value: Large Cheddar Cheese Curds.',
	CHILD__ITEM_PRICE_AMT NUMBER(18,2) COMMENT 'Child Price Amount. Sample Value: 9.99.',
	CHILD_ITEM_PRODUCT_ID VARCHAR(16777216) COMMENT 'Child Product Identifier . Sample Value: SalesItem-3032.',
	CHILD_ITEM_DESTINATION_CD VARCHAR(16777216) COMMENT 'Child Item Destination Code. Sample Value: 1.',
	CHILD_ITEM_QTY NUMBER(38,0) COMMENT 'Child Item Quantity. Sample Value: 1.',
	CHILD_SUGGESTED_SELL_RECOMMENDATION_ID VARCHAR(16777216) COMMENT 'Child Suggested Sell Recommendation Identifier uniquely identifies the recommendation that determine a set of products. The recommendation is based on a backet of goods and a set of recommendations from a recommendation file. Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	CHILD_ITEM_PRODUCT_CATEGORY_NM VARCHAR(16777216) COMMENT 'Child Item Product Category Name specifies a grouping for an item. For example, Shake is the categorization for a Small Vanilla Shake.',
	CHILD_ITEM_POS_ID NUMBER(38,0) COMMENT 'Child Item POS Identifier.  Sample value 640243388.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CHILD_ITEM_DISCOUNTED_AMT NUMBER(18,2),
	CHILD_ITEM_COMPONENT_ID NUMBER(38,0),
	CHILD_ITEM_AVAILABLE_IND BOOLEAN,
	CHILD_ITEM_AVAILABLE_POS_ID NUMBER(38,0),
	CHILD_ITEM_AVAILABLE_QTY NUMBER(38,0),
	CHILD_ITEM_NOTE_TXT VARCHAR(16777216),
	CHILD_PARENT_ITEM_ID VARCHAR(16777216) COMMENT 'Child Parent Item Identifier identifies the parent Child Item of a nested Child Item. ',
	constraint XPKDIGITAL_ORDER_LINE_CHILD primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM, CHILD_ITEM_ID),
	constraint R_44 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM) references DIGITAL_ORDER_LINE(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,SOURCE_SYSTEM_NM)
);
create TABLE IF NOT EXISTS DIGITAL_ORDER_LINE_CHILD_MODIFIER (
	CHILD_MODIFIER_DESC VARCHAR(16777216) COMMENT 'Modifier Description . Sample Value: Southwestern Ranch.',
	CHILD_MODIFIER_PRODUCT_ID VARCHAR(16777216) COMMENT 'Modifier Product Identifier . Sample Value: Modifier-5161.',
	CHILD_MODIFIER_LINE_ITEM_NBR NUMBER(38,0) COMMENT 'Modifier Line Item Number specifies the line the modifier is displayed on',
	CHILD_MODIFIER_PRICE_AMT NUMBER(18,2) COMMENT 'Modifier Price Amount. Sample Value: 0.00.',
	CHILD_MODIFIER_DISCOUNTED_PRICE_AMT NUMBER(18,2) COMMENT 'Modifier Discounted Price Amount is the price of the item after it has been discounted.',
	CHILD_MODIFIER_QTY NUMBER(38,0) COMMENT 'Modifier Quantity. Sample Value: 1.',
	CHILD_MODIFIER_DISPLAY_NM VARCHAR(16777216) COMMENT 'Modifier Display Name . Sample Value: Southwestern Ranch.',
	CHILD_MODIFIER_POS_ID NUMBER(38,0) COMMENT 'Modifier Pos Identifier. Sample Value: 640212495.',
	CHILD_MODIFIER_AVAILABLE_IND BOOLEAN,
	CHILD_MODIFIER_AVAILABLE_POS_ID NUMBER(38,0),
	CHILD_MODIFIER_AVAILABLE_QTY NUMBER(38,0),
	CHILD_MODIFIER_ACTION_CD VARCHAR(16777216) COMMENT 'Modifier Action Code',
	CHILD_MODIFIER_ACTION_DESC VARCHAR(16777216) COMMENT 'Modifier Action Description',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR VARCHAR(16777216) NOT NULL COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CHILD_ITEM_ID VARCHAR(16777216) NOT NULL COMMENT 'Child Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	CHILD_MODIFIER_GROUP_ID VARCHAR(16777216) NOT NULL COMMENT 'Modifier Group Id . Sample Value: 21521.',
	CHILD_MODIFIER_ID VARCHAR(16777216) NOT NULL COMMENT 'Modifier Id . Sample Value: 21551.',
	constraint XPKDIGITAL_ORDER_LINE_CHILD_MODIFIER primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM, CHILD_ITEM_ID, CHILD_MODIFIER_GROUP_ID, CHILD_MODIFIER_ID),
	constraint R_502 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM, CHILD_ITEM_ID, CHILD_MODIFIER_GROUP_ID) references DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,SOURCE_SYSTEM_NM,CHILD_ITEM_ID,CHILD_MODIFIER_GROUP_ID)
)COMMENT='Digital Line Order Modifier specifies the specific modification to a line item. For example a ketchup packet.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP (
	CHILD_MODIFIER_GROUP_POS_ID NUMBER(38,0) COMMENT 'Modifier Group Pos Identifier . Sample Value: 8937.',
	CHILD_MODIFIER_GROUP_PRODUCT_ID VARCHAR(16777216) COMMENT 'Modifier Group Product Identifier . Sample Value: ModifierGroup-8937.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CHILD_MODIFIER_GROUP_DESC VARCHAR(16777216) COMMENT 'Modifier Group Description.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR VARCHAR(16777216) NOT NULL COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CHILD_ITEM_ID VARCHAR(16777216) NOT NULL COMMENT 'Child Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	CHILD_MODIFIER_GROUP_ID VARCHAR(16777216) NOT NULL COMMENT 'Modifier Group Id . Sample Value: 21521.',
	constraint XPKDIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM, CHILD_ITEM_ID, CHILD_MODIFIER_GROUP_ID),
	constraint R_501 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM, CHILD_ITEM_ID) references DIGITAL_ORDER_LINE_CHILD(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,SOURCE_SYSTEM_NM,CHILD_ITEM_ID)
)COMMENT='Digital Order Line Child  Modifier Group is a categorizations that may occurs to child items. Such as a categorization of condiments for an entree.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_LINE_MODIFIER (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR VARCHAR(16777216) NOT NULL COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	MODIFIER_GROUP_ID VARCHAR(16777216) NOT NULL COMMENT 'Modifier Group Id . Sample Value: 21521.',
	MODIFIER_ID VARCHAR(16777216) NOT NULL COMMENT 'Modifier Id . Sample Value: 21551.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	MODIFIER_DESC VARCHAR(16777216) COMMENT 'Modifier Description . Sample Value: Southwestern Ranch.',
	MODIFIER_PRODUCT_ID VARCHAR(16777216) COMMENT 'Modifier Product Identifier . Sample Value: Modifier-5161.',
	MODIFIER_LINE_ITEM_NBR NUMBER(38,0) COMMENT 'Modifier Line Item Number specifies the line the modifier is displayed on',
	MODIFIER_PRICE_AMT NUMBER(18,2) COMMENT 'Modifier Price Amount. Sample Value: 0.00.',
	MODIFIER_DISCOUNTED_PRICE_AMT NUMBER(18,2) COMMENT 'Modifier Discounted Price Amount is the price of the item after it has been discounted.',
	MODIFIER_QTY NUMBER(38,0) COMMENT 'Modifier Quantity. Sample Value: 1.',
	MODIFIER_DISPLAY_NM VARCHAR(16777216) COMMENT 'Modifier Display Name . Sample Value: Southwestern Ranch.',
	MODIFIER_POS_ID NUMBER(38,0) COMMENT 'Modifier Pos Identifier. Sample Value: 640212495.',
	MODIFIER_AVAILABLE_IND BOOLEAN,
	MODIFIER_AVAILABLE_POS_ID NUMBER(38,0),
	MODIFIER_AVAILABLE_QTY NUMBER(38,0),
	MODIFIER_ACTION_CD VARCHAR(16777216) COMMENT 'Modifier Action Code',
	MODIFIER_ACTION_DESC VARCHAR(16777216) COMMENT 'Modifier Action Description',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKDIGITAL_ORDER_LINE_MODIFIER primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, MODIFIER_GROUP_ID, MODIFIER_ID, SOURCE_SYSTEM_NM),
	constraint R_43 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, MODIFIER_GROUP_ID, SOURCE_SYSTEM_NM) references DIGITAL_ORDER_LINE_MODIFIER_GROUP(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,MODIFIER_GROUP_ID,SOURCE_SYSTEM_NM)
)COMMENT='Digital Line Order Modifier specifies the specific modification to a line item. For example a ketchup packet.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_LINE_MODIFIER_GROUP (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR VARCHAR(16777216) NOT NULL COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	MODIFIER_GROUP_ID VARCHAR(16777216) NOT NULL COMMENT 'Modifier Group Id . Sample Value: 21521.',
	MODIFIER_GROUP_POS_ID NUMBER(38,0) COMMENT 'Modifier Group Pos Identifier . Sample Value: 8937.',
	MODIFIER_GROUP_PRODUCT_ID VARCHAR(16777216) COMMENT 'Modifier Group Product Identifier . Sample Value: ModifierGroup-8937.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	MODIFIER_GROUP_DESC VARCHAR(16777216) COMMENT 'Modifier Group Description.',
	constraint XPKDIGITAL_ORDER_LINE_MODIFIER_GROUP primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, MODIFIER_GROUP_ID, SOURCE_SYSTEM_NM),
	constraint R_42 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, LINE_ITEM_NBR, SOURCE_SYSTEM_NM) references DIGITAL_ORDER_LINE(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,SOURCE_SYSTEM_NM)
)COMMENT='Digital Order Line Modifier Group is a categorizations that may occurs to a line item. Such as a categorization of condiments for an entree.'
;
create TABLE IF NOT EXISTS DIGITAL_ORDER_PAYMENT (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID VARCHAR(16777216) NOT NULL COMMENT 'Location Identifier . Sample Value: 13.',
	PAYMENT_SEQ_NBR NUMBER(38,0) NOT NULL COMMENT 'Payment Sequence Number is the order in which objects are listed in the message or source of the file. Values are 1, 2, 3, 4, etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	PAYMENT_TYP_CD VARCHAR(16777216) COMMENT 'Payment Type Code is a unique code specifying how a settlement occured. Examples: TOKEN -  a credit card transaction through a third party processor, CASH - monies were exchanged.',
	PAYMENT_CARD_ISSUER_NM VARCHAR(16777216) COMMENT 'Card Issuer Name. Sample Value: Visa.',
	PAYMENT_CARD_NBR VARCHAR(16777216) COMMENT 'Card Number . Sample Value: 411111xxxxxx1111.',
	PAYMENT_AMT NUMBER(18,2) COMMENT 'Payment Amount. Sample Value: 10.70.',
	PAYMENT_CARD_HOLDER_NM VARCHAR(16777216) COMMENT 'Payment Card Holder Name. Sample Value: Dasha.',
	PAYMENT_PROCESS_CD VARCHAR(16777216) COMMENT 'Payment Process Code is a unique code reflecting payment on order. Sample value: 01Z6JF4VB001U6A6VSG0M3LBSFTC8TGO.',
	PAYMENT_STATUS_TXT VARCHAR(16777216) COMMENT 'Payment Status Text',
	PAYMENT_AUTHORIZATION_CD VARCHAR(16777216),
	PAYMENT_PROCESS_MESSAGE_TXT VARCHAR(16777216),
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKDIGITAL_ORDER_PAYMENT primary key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, PAYMENT_SEQ_NBR, SOURCE_SYSTEM_NM),
	constraint R_46 foreign key (BRAND_ID, BUSINESS_DT, DIGITAL_ORDER_ID, LOC_ID, SOURCE_SYSTEM_NM) references DIGITAL_ORDER(BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,SOURCE_SYSTEM_NM)
)COMMENT='Digital Order Payment specifies the different types of payment for an order.'
;
create TABLE IF NOT EXISTS DIGITAL_SUGGESTED_SELL (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ORDER_CORRELATION_ID VARCHAR(16777216) NOT NULL COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	SUGGESTED_SELL_RECOMMENDATION_ID VARCHAR(16777216) NOT NULL,
	TARGET_SALE_SEGMENT_CD VARCHAR(16777216) COMMENT 'Target Sale Segment Code is used to determine which suggestions are offered to the customer. There are three segmenets: Original - control group where no suggestions are shown; this variant does not have the customerType cookie,  Learning - the Learning Challenger group which sets the customerType to �LC�, and Polaris - the Polaris Challenger group which sets the customerType to �PC�.  Individuals are dynamically assigned a segment for the transaction.',
	LOC_ID VARCHAR(16777216) COMMENT 'Location Identifier . Sample Value: 13.',
	RECOMMENDATION_DTTM TIMESTAMP_NTZ(9) COMMENT 'Recommendation Datetime  is the day, month, year and time of when a the recommendation occured.',
	BASKET_ID VARCHAR(16777216) COMMENT 'Basket Identifier specified a basket of goods based on its characteristics that an individual is looking to purchase. The characteristics that are used in deriving a basiket identifier are Day of Week, Part of Day(breakfast, lunch, dinner), and number of entres, sides and drinks. Basket Identifier is currently only applicable to a customer segement of Polaris Challenger (PC).',
	ITEM_RECOMMENDATION_FILENAME VARCHAR(16777216) COMMENT 'Item Recommendation Filename containes the set of recommnedations for items that were used to generate the recommendation set.',
	CUST_SOURCE_CHANNEL_CD VARCHAR(16777216) COMMENT 'SOURCE_CHANNEL: the selling channel where the Customer originated. Example values: [STORE|WEBOA|MOBILE|KIOSK|PHONE|�].',
	CUST_SOURCE_SUB_CHANNEL_CD VARCHAR(16777216) COMMENT 'provides additional/support information about the SOURCE_CHANNEL property. Example values: [PREFERENCES|�|.',
	EVENT_TYP_CD VARCHAR(16777216) COMMENT 'this property indicates the type of event, which corresponds to the IDP domain. Sample values: [CUSTOMER|�]',
	EVENT_SUB_TYP_CD VARCHAR(16777216) COMMENT 'this property provides additional information on the event type. For example, it could indicate that the event is about a change of the order status for an Order event. Sample values: [PREFERENCE_TYPE|�]',
	EVENT_STATUS_CD VARCHAR(16777216) COMMENT '(optional): this property may indicate the actual change that trigged this event. For example, it could include the actual value of the Customer Status of �EMAIL_SIGNUP� so that only the consumers that are interested in Customer email signup events will take actions on this event. This property may not apply to every Customer event and thus is optional. But Customer domain services encourage to include this property where applicable.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKDIGITAL_SUGGESTED_SELL primary key (BRAND_ID, SOURCE_SYSTEM_NM, ORDER_CORRELATION_ID, SUGGESTED_SELL_RECOMMENDATION_ID)
)COMMENT='Digital Suggested Sell provides recommendations for additional items to purchase. For a Polaris Challenger a basket of goods is used to provide a recommendation of additional items. For a Learning Challender it is based on day of week and time of day.'
;
create TABLE IF NOT EXISTS DIGITAL_SUGGESTED_SELL_CART (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ORDER_CORRELATION_ID VARCHAR(16777216) NOT NULL COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	SUGGESTED_SELL_RECOMMENDATION_ID VARCHAR(16777216) NOT NULL,
	CART_ITEM_ID VARCHAR(16777216) NOT NULL COMMENT 'Cart Item Identifier specifies a product that is in a cart prior to checkout and used in the Suggested Sell process. Sample Value(s): arb-itm-000-001, arb-itm-003-001.',
	CART_ITEM_SEQ_NBR NUMBER(38,0) COMMENT 'Cart Item Sequence Number is the order of the items listed in the cart from the Suggested Sell message.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKDIGITAL_SUGGESTED_SELL_CART primary key (BRAND_ID, SOURCE_SYSTEM_NM, ORDER_CORRELATION_ID, SUGGESTED_SELL_RECOMMENDATION_ID, CART_ITEM_ID),
	constraint R_490 foreign key (BRAND_ID, SOURCE_SYSTEM_NM, ORDER_CORRELATION_ID, SUGGESTED_SELL_RECOMMENDATION_ID) references DIGITAL_SUGGESTED_SELL(BRAND_ID,SOURCE_SYSTEM_NM,ORDER_CORRELATION_ID,SUGGESTED_SELL_RECOMMENDATION_ID)
)COMMENT='Digital Suggested Sell Cart is the list of items the user is looking to purchase that are part of the Suggested Sell process.'
;
create TABLE IF NOT EXISTS DIGITAL_SUGGESTED_SELL_RECOMMENDATION (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ORDER_CORRELATION_ID VARCHAR(16777216) NOT NULL COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	SUGGESTED_SELL_RECOMMENDATION_ID VARCHAR(16777216) NOT NULL,
	RECOMMENDATION_ITEM_ID VARCHAR(16777216) NOT NULL COMMENT 'Recommendation Item Identifier specifies a recommended product for an order as part of the Suggested Sell process . Sample Value(s): arb-itm-000-001, arb-itm-003-001.',
	RECOMMENDATION_ITEM_SEQ_NBR NUMBER(38,0) COMMENT 'Recommendation Item Sequence Number is the order of the recommended items listed in the suggested sell message.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKDIGITAL_SUGGESTED_SELL_RECOMMENDATION primary key (BRAND_ID, SOURCE_SYSTEM_NM, ORDER_CORRELATION_ID, SUGGESTED_SELL_RECOMMENDATION_ID, RECOMMENDATION_ITEM_ID),
	constraint R_491 foreign key (BRAND_ID, SOURCE_SYSTEM_NM, ORDER_CORRELATION_ID, SUGGESTED_SELL_RECOMMENDATION_ID) references DIGITAL_SUGGESTED_SELL(BRAND_ID,SOURCE_SYSTEM_NM,ORDER_CORRELATION_ID,SUGGESTED_SELL_RECOMMENDATION_ID)
)COMMENT='Digital Suggested Sell Recommendation is the list of items that are recommended to the user for purchase as part of the Suggested Sell process.'
;
create TABLE IF NOT EXISTS FLYWAY_SCHEMA_HISTORY (
	INSTALLED_RANK NUMBER(38,0) NOT NULL,
	VERSION VARCHAR(50),
	DESCRIPTION VARCHAR(200),
	TYPE VARCHAR(20) NOT NULL,
	SCRIPT VARCHAR(1000) NOT NULL,
	CHECKSUM NUMBER(38,0),
	INSTALLED_BY VARCHAR(100) NOT NULL,
	INSTALLED_ON TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	EXECUTION_TIME NUMBER(38,0) NOT NULL,
	SUCCESS BOOLEAN NOT NULL,
	primary key (INSTALLED_RANK)
);
create TABLE IF NOT EXISTS FN_DAILY_REV_MEASURE (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DATE DATE NOT NULL COMMENT 'Business Date specifies the day of the year the value is valid for.',
	REST_ID VARCHAR(16777216) NOT NULL COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE VARCHAR(16777216) NOT NULL COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR VARCHAR(16777216) NOT NULL COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT NUMBER(18,2) COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT NUMBER(22,6) COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT NUMBER(38,0) COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE VARCHAR(16777216) COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_DAILY_REVENUE_MEASURE primary key (BRAND_ID, BUSINESS_DATE, REST_ID, FN_SYSTEM_ID, FN_MEASURE_ID, GL_ACCOUNT_CODE, GL_COST_CTR),
	constraint R_3 foreign key (FN_SYSTEM_ID) references FN_SYSTEM(FN_SYSTEM_ID),
	constraint R_10 foreign key (FN_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID)
)COMMENT='FN Daily Revenue Measure contains data from different sources that has been related to a common measure at the daily level.'
;
create TABLE IF NOT EXISTS FN_DAILY_REV_MEASURE_PARBRINKS (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DATE DATE NOT NULL COMMENT 'Business Date specifies the day of the year the value is valid for.',
	REST_ID VARCHAR(16777216) NOT NULL COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE VARCHAR(16777216) NOT NULL COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR VARCHAR(16777216) NOT NULL COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT NUMBER(18,2) COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT NUMBER(22,6) COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT NUMBER(38,0) COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE VARCHAR(16777216) COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_DAILY_REVENUE_MEASURE primary key (BRAND_ID, BUSINESS_DATE, REST_ID, FN_SYSTEM_ID, FN_MEASURE_ID, GL_ACCOUNT_CODE, GL_COST_CTR)
)COMMENT='FN Daily Revenue Measure contains data from different sources that has been related to a common measure at the daily level.'
;
create TABLE IF NOT EXISTS FN_DAILY_SYSTEM_DATA_BKUP (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DATE DATE NOT NULL COMMENT 'Business Date specifies the day of the year the value is valid for.',
	REST_ID VARCHAR(16777216) NOT NULL COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE VARCHAR(16777216) NOT NULL COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR VARCHAR(16777216) NOT NULL COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT NUMBER(18,2) COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT NUMBER(22,6) COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT NUMBER(38,0) COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE VARCHAR(16777216) COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_DAILY_SYSTEM_DATA primary key (BRAND_ID, BUSINESS_DATE, REST_ID, FN_SYSTEM_ID, FN_MEASURE_ID, GL_ACCOUNT_CODE, GL_COST_CTR),
	constraint R_3 foreign key (FN_SYSTEM_ID) references FN_SYSTEM(FN_SYSTEM_ID),
	constraint R_10 foreign key (FN_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID)
)COMMENT='FN Daily System Data contains data from different sources that has been related to a common measure at the daily level.'
;
create TABLE IF NOT EXISTS FN_MEASURE (
	FN_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	MEASURE_NAME VARCHAR(16777216) NOT NULL COMMENT 'Measure Name is a label utilized to specify a value. Sample values Tax Exempt Sales, Non-Taxable Sales, Paid Outs, and Over Short.\n',
	MEASURE_DESC VARCHAR(16777216) COMMENT 'Measure Description provides additional information about a measure. For example, Total Absolute Variance is taking the absolute value of a variance and aggregating it with other variances to provide the scope of the variance.',
	CREDIT_DEBIT_CODE VARCHAR(16777216) COMMENT 'Credit Debit Code specifies if a measure is typically a credit or debit. Sample values, D or C.',
	DEFAULT_SORT_ID NUMBER(38,0) COMMENT 'Default Sort Identifier is a reporting sort order for measures when they appear in a report.',
	MEASURE_CATEGORY_NAME VARCHAR(16777216) COMMENT 'Measure Category Name allows for a higher level grouping of measures. Samle values, Net Sales, Sales Tax,  Deposit, and Credit Cards.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_MEASURE primary key (FN_MEASURE_ID)
)COMMENT='FN Measure contains the measure that can be utilized for reconciliation analysis. For example: Net Sales, Sales Tax, Cash Deposit, and Visa Card.'
;
create TABLE IF NOT EXISTS FN_MEASURE_CALCULATION (
	FN_MEASURE_CALCULATION_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Calculation Identifier uniquely identifies a calculation for a measure.',
	FN_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	FN_MEASURE_CALCULATION_CATEGORY_CODE VARCHAR(16777216) NOT NULL COMMENT 'FN Measure Calculation Category Code uniquely identifies a calculation type that can be performed to create a new measure.For example, AGG or ABSAGG.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_MEASURE_CALCULATION primary key (FN_MEASURE_CALCULATION_ID),
	constraint R_18 foreign key (FN_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID),
	constraint R_19 foreign key (FN_MEASURE_CALCULATION_CATEGORY_CODE) references FN_MEASURE_CALCULATION_CATEGORY(FN_MEASURE_CALCULATION_CATEGORY_CODE)
)COMMENT='FN Measure Calculation specificies for a given measure what type of calculation will be utilized such as Aggregation or Absolute Aggregation.'
;
create TABLE IF NOT EXISTS FN_MEASURE_CALCULATION_CATEGORY (
	FN_MEASURE_CALCULATION_CATEGORY_CODE VARCHAR(16777216) NOT NULL COMMENT 'FN Measure Calculation Category Code uniquely identifies a calculation type that can be performed to create a new measure.For example, AGG or ABSAGG.',
	MEASURE_CALCULATION_CATEGORY_NAME VARCHAR(16777216) NOT NULL COMMENT 'FN Measure Calculation Category Code specifies a calculation type that can be performed to create a new measure.For example, Aggregation or Absolute Aggregation.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_MEASURE_CALCULATION_CATEGORY primary key (FN_MEASURE_CALCULATION_CATEGORY_CODE)
)COMMENT='FN Measure Calculation Category specificies a method to derive a measure. For example, Aggregation or Absolute Aggregation.'
;
create TABLE IF NOT EXISTS FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION (
	FN_MEASURE_CALCULATION_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Calculation Identifier uniquely identifies a calculation for a measure.',
	FN_INPUT_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_MEASURE_CALCULATION_MEASURE_ASSOCIATION primary key (FN_MEASURE_CALCULATION_ID, FN_INPUT_MEASURE_ID),
	constraint R_16 foreign key (FN_INPUT_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID),
	constraint R_20 foreign key (FN_MEASURE_CALCULATION_ID) references FN_MEASURE_CALCULATION(FN_MEASURE_CALCULATION_ID)
)COMMENT='FN Measure Calculation Measure Association specificies which measures are utilized in the calculation of another measure.'
;
create TABLE IF NOT EXISTS FN_SYSTEM (
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_SYSTEM_CATEGORY_CODE VARCHAR(16777216) COMMENT 'FN System Category Code uniquely identifies a grouping for multiple systems in order to be analyzed by category. Sample values are POS, BO, SS and GL.',
	SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'System Name is a label for which data is sourced. For example,  PAR Brink Data, Oracle General Ledger, Sales System, and Altametrics. ',
	SYSTEM_DESC VARCHAR(16777216) NOT NULL COMMENT 'System Description allows for additional information about a system, such as if it is only for a particular data elements.',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_SYSTEM primary key (FN_SYSTEM_ID),
	constraint R_9 foreign key (FN_SYSTEM_CATEGORY_CODE) references FN_SYSTEM_CATEGORY(FN_SYSTEM_CATEGORY_CODE)
)COMMENT='FN Systems specifies the different systems that are utilized for reconciliation analysis. For example: PAR Brink Data, Oracle General Ledger, Sales System, and Altametrics. '
;
create TABLE IF NOT EXISTS FN_SYSTEM_CATEGORY (
	FN_SYSTEM_CATEGORY_CODE VARCHAR(16777216) NOT NULL COMMENT 'FN System Category Code uniquely identifies a grouping for multiple systems in order to be analyzed by category. Sample values are POS, BO, SS and GL.',
	FN_SYSTEM_CATEGORY_NAME VARCHAR(16777216) NOT NULL COMMENT 'FN System Category Name is a label for a grouping for multiple systems in order to be analyzed by category. Sample values are Point of Sale, Back Office, Sales System, and General Ledger.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_SYSTEM_CATEGORY primary key (FN_SYSTEM_CATEGORY_CODE)
)COMMENT='FN System Category allows for aggregation of data across multiple sources of similar data to a general system category.  For example: POS - Point of Sale, BO - Back Office, SS - Sales System, and GL - General Ledger. '
;
create TABLE IF NOT EXISTS FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE (
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	GL_ACCOUNT_CODE VARCHAR(16777216) NOT NULL COMMENT 'GL Account Code represents an account used in the general ledger.',
	FN_MEASURE_ID NUMBER(38,0) COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_DEFAULT_MEASURE_IND BOOLEAN COMMENT 'GL Account Default Measure Indicator specifies if there are multiple measures for a GL account which one should be utilized if no additional information can be provided in selecting a measure. Valid values are true/false.',
	GL_ACCOUNT_PATTERN_IND BOOLEAN COMMENT 'GL Account Pattern Indicator specifes if a pattern for a GL account is specified. For example 7% matches to Paid Outs. Valid values are true/false.',
	ACTIVE_IND BOOLEAN COMMENT 'Active Indicator specifies if the mapping is currently in use. Valid value TRUE and FALSE.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE primary key (FN_SYSTEM_ID, BRAND_ID, SOURCE_SYSTEM_NAME, GL_ACCOUNT_CODE),
	constraint R_31 foreign key (FN_SYSTEM_ID) references FN_SYSTEM(FN_SYSTEM_ID),
	constraint R_32 foreign key (FN_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID)
)COMMENT='FN System GL Account To Measure Reference is utilized to map an account from source date to a measure.'
;
create TABLE IF NOT EXISTS FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BACKUP (
	FN_SYSTEM_ID NUMBER(38,0),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	GL_ACCOUNT_CODE VARCHAR(16777216),
	FN_MEASURE_ID NUMBER(38,0),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS FN_SYSTEM_TO_MEASURE_REFERENCE (
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	SOURCE_SALES_SYSTEM_MEASURE_TEXT VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	FN_MEASURE_ID NUMBER(38,0) COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	VALUE_ADJUSTMENT_AMOUNT FLOAT COMMENT 'Value Adjustment Amount is a factor that is applied to an incoming value in order to translate it. If Value Adjustment Amount is null then no factor is being applied to the value such as multiplying by 1. To adjust the sign of a value the factor would be -1. If dollar and cents are coming in as an integer than the factor of .01 could be utilized to convert the amount into a decimal number.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_SYSTEM_TO_MEASURE_REFERENCE primary key (FN_SYSTEM_ID, SOURCE_SALES_SYSTEM_MEASURE_TEXT, BRAND_ID, SOURCE_SYSTEM_NAME),
	constraint R_5 foreign key (FN_SYSTEM_ID) references FN_SYSTEM(FN_SYSTEM_ID),
	constraint R_11 foreign key (FN_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID)
)COMMENT='FN System To Measure Reference is a generic mapping table from source data to measure.'
;
create TABLE IF NOT EXISTS FN_SYSTEM_TO_MEASURE_REFERENCE_BKUP (
	FN_SYSTEM_ID NUMBER(38,0),
	SOURCE_SALES_SYSTEM_MEASURE_TEXT VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	FN_MEASURE_ID NUMBER(38,0),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS FN_WEEKLY_REV_MEASURE (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	FISC_WK_END_DATE DATE NOT NULL COMMENT 'Fiscal Week End Date specifies the day at the end of a fiscal week the value is valid for.',
	REST_ID VARCHAR(16777216) NOT NULL COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE VARCHAR(16777216) NOT NULL COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR VARCHAR(16777216) NOT NULL COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT NUMBER(18,2) COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT NUMBER(22,6) COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT NUMBER(38,0) COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE VARCHAR(16777216) COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_WEEKLY_REVENUE_MEASURE primary key (BRAND_ID, FISC_WK_END_DATE, REST_ID, FN_SYSTEM_ID, FN_MEASURE_ID, GL_ACCOUNT_CODE, GL_COST_CTR),
	constraint R_13 foreign key (FN_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID),
	constraint R_15 foreign key (FN_SYSTEM_ID) references FN_SYSTEM(FN_SYSTEM_ID)
);
create TABLE IF NOT EXISTS FN_WEEKLY_SYSTEM_DATA_BKUP (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	FISC_WK_END_DATE DATE NOT NULL COMMENT 'Fiscal Week End Date specifies the day at the end of a fiscal week the value is valid for.',
	REST_ID VARCHAR(16777216) NOT NULL COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID NUMBER(38,0) NOT NULL COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID NUMBER(38,0) NOT NULL COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE VARCHAR(16777216) NOT NULL COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR VARCHAR(16777216) NOT NULL COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT NUMBER(18,2) COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT NUMBER(22,6) COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT NUMBER(38,0) COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE VARCHAR(16777216) COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKFN_WEEKLY_SYSTEM_DATA primary key (BRAND_ID, FISC_WK_END_DATE, REST_ID, FN_SYSTEM_ID, FN_MEASURE_ID, GL_ACCOUNT_CODE, GL_COST_CTR),
	constraint R_13 foreign key (FN_MEASURE_ID) references FN_MEASURE(FN_MEASURE_ID),
	constraint R_15 foreign key (FN_SYSTEM_ID) references FN_SYSTEM(FN_SYSTEM_ID)
);
create TABLE IF NOT EXISTS INSPIRE_COMPARABLE_SALES (
	AGG_LEVEL_TYP VARCHAR(16777216) COMMENT 'Aggregation level for sales summarization',
	BUSINESS_DT DATE COMMENT 'Date of sales transaction',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand associated with store',
	STORE_NBR VARCHAR(16777216) COMMENT 'Number that uniquely identifies the Store',
	DMA_NM VARCHAR(16777216) COMMENT 'Designated marketing area code for the store',
	DMA_CD VARCHAR(16777216) COMMENT 'Designated marketing area code name for the store',
	LEVEL1_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L1 name',
	LEVEL2_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L2 name',
	LEVEL3_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L3 name',
	LEVEL4_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L4 name',
	LEVEL5_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L5 name',
	OWNERSHIP_TYP VARCHAR(16777216) COMMENT 'Type of store ownership',
	STATE_CD VARCHAR(16777216) COMMENT 'State of the store',
	COUNTRY_CD VARCHAR(16777216) COMMENT 'Country of the store',
	DAY_OF_WEEK_NM VARCHAR(16777216) COMMENT 'Name of day for business date',
	CURRENT_YEAR_START_DT DATE COMMENT 'Start date for the reporting period of the aggregate level',
	CURRENT_YEAR_END_DT DATE COMMENT 'The end date for the reporting period of the aggregate level',
	CURRENT_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Current year net sales amount for the  aggregate level',
	CURRENT_YEAR_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Current year count of transactions for the aggregate level',
	CURRENT_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Current year comp sales amount for the aggregate level',
	CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Current year count of comp transactions for the aggregate level',
	FISCAL_LAST_YEAR_DT DATE COMMENT 'Last year fiscal comparable date associated with the current year business date for aggregate level.',
	FISCAL_LAST_YEAR_START_DT DATE COMMENT 'Start date for the reporting period for the fiscal comparable date associated with the business date or aggregate level',
	FISCAL_LAST_YEAR_END_DT DATE COMMENT 'End date for the reporting period for the fiscal comparable date associated with the business date or aggregate level',
	FISCAL_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level count of transactions for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level comp transaction count for last year fiscal comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_DT DATE COMMENT 'Last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_START_DT DATE COMMENT 'Start date for the reporting period for the calendar comparable date associated with the business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_END_DT DATE COMMENT 'End date for the reporting period for the calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level count of transactions for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level comp transaction count for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_2YEAR_COMPARABLE_DT DATE COMMENT '2 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_2YEAR_COMPARABLE_START_DT DATE COMMENT 'Start date for the reporting period for the calendar comparable date 2 years ago associated with the business date',
	CALENDAR_2YEAR_COMPARABLE_END_DT DATE COMMENT 'End date for the reporting period for the calendar comparable date 2 years ago associated with the business date',
	CALENDAR_2YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level count of transactions for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level comp transaction count for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_3YEAR_COMPARABLE_DT DATE COMMENT '3 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_3YEAR_COMPARABLE_START_DT DATE COMMENT 'Start date for the reporting period for the calendar comparable date 3 years ago associated with the current year business date for aggregate level.',
	CALENDAR_3YEAR_COMPARABLE_END_DT DATE COMMENT 'End date for the reporting period for the calendar comparable date 3 years ago associated with the current year business date for aggregate level.',
	CALENDAR_3YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_3YEAR_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level count of transactions for 3 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_3YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(18,0) COMMENT 'Aggregate level comp transaction count for 3 year calendar comparable date associated with the current year business date for aggregate level.',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_ID NUMBER(38,0) COMMENT ' Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT '\nThe Date/Datetime the Record was Inserted',
	UPDATE_ID NUMBER(38,0) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated'
);
create TABLE IF NOT EXISTS "ORDER" (
	ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'OrderID uniquely defines each order\n',
	EMPLOYEE_ID VARCHAR(16777216) COMMENT 'Employee responsible for order\n',
	BUSINESS_DT DATE COMMENT 'Date of the Order \n',
	STORE_ID VARCHAR(16777216) COMMENT 'Number that uniquely identifies the Store\n',
	CHECK_NBR VARCHAR(16777216) COMMENT 'Daily incremental ticket number\n',
	OPENED_TM TIMESTAMP_NTZ(9) COMMENT 'time the ticket was opened\n',
	CLOSED_TM TIMESTAMP_NTZ(9) COMMENT 'time the ticket was closed\n',
	ORDER_NM VARCHAR(16777216) COMMENT 'Name on the order\n',
	ITEM_CNT NUMBER(38,0) COMMENT 'Count of DerivedGrossQuantity from Orderline table where orderlinetype=''item''\n',
	TRANSACTION_CNT NUMBER(38,0) COMMENT 'Records whether order has been voided or not. (previously called GrossQuantity)\n',
	SOURCE_GROSS_AMT NUMBER(18,2) COMMENT 'Gross Amount for an order provided by source\n',
	SOURCE_DISCOUNT_AMT NUMBER(18,2) COMMENT 'Discount Amount for an order provided by source\n',
	SOURCE_NET_AMT NUMBER(18,2) COMMENT 'Net Amount for an order provided by source\n',
	SURCHARGE_AMT NUMBER(18,2) COMMENT 'Delivery fee for the order\n',
	TAX_AMT NUMBER(18,2) COMMENT 'Total amount of taxes charged for the order\n',
	SOURCE_PAYMENT_AMT NUMBER(18,2) COMMENT 'Total amount that customer paid for their order (a.k.a. subticket a.k.a. orderid)',
	GRATUITY_AMT NUMBER(18,2) COMMENT 'This would be the tip paid via credit card to the carhop.\n',
	CUSTOMER_NM VARCHAR(16777216) COMMENT 'This would be the customer’s name that an online order was placed under.\n',
	CUSTOMER_ID VARCHAR(16777216) COMMENT 'This would be the customer id that an online order was placed under.\n',
	LOYALTY_NBR VARCHAR(16777216) COMMENT 'This would be the number assigned to a customer for loyalty purposes.\n',
	FIRST_SEND_TM TIMESTAMP_NTZ(9) COMMENT 'This would be approximately the time when the ticket was opened.\n',
	CLOSED_IND BOOLEAN COMMENT 'Indicates whether the existing ticket has been closed or not.\n',
	FUTURE_ORDER_IND BOOLEAN COMMENT 'This would indicate whether the customer had requested a specific time to pick up an online order.\n',
	VOID_IND BOOLEAN COMMENT 'Indicates whether the order was cancelled \n',
	COMBO_TRANSACTION_IND BOOLEAN COMMENT 'Combo Transaction  Indicator.This column will have value as C when we identify that a transaction is a combo transaction, which will help us to split those orders in IDSIn CDM DE will add value c whenever we have a combo transaction, in IDS we will create a view and use this column to identify which all orders to split and put in view',
	REFUND_IND BOOLEAN COMMENT 'Indicate if the Customer order was refunded for their order\n',
	TAX_EXEMPT_IND BOOLEAN COMMENT 'Indicates whether the customer has the tax exempt status \n',
	GUEST_CNT NUMBER(38,0) COMMENT 'Number of customer per ticket \n',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand for the Order\n',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'The POS System used to load the transaction  information\n',
	TIME_KEY NUMBER(38,0) COMMENT 'Time sec  at which an order was created.\n',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	DERIVED_GROSS_AMT NUMBER(18,2) COMMENT 'Pre tax and pre discounted amount.\n',
	DERIVED_DISCOUNT_AMT NUMBER(18,2) COMMENT 'This is total amount of discounts that were applied to an order.\n',
	DERIVED_NET_AMT NUMBER(18,2) COMMENT 'Gross Amount - Discount Amount +Delivery Fee\n',
	DERIVED_PAYMENT_AMT NUMBER(18,2) COMMENT 'Total amount that customer paid for their order (a.k.a. subticket a.k.a. orderid),Derived Payment Amount = Net Amount +  Taxes+ Gratuity + Misc Charge Amt',
	MISC_CHARGE_AMT NUMBER(18,2) COMMENT 'sum of any amount not directly linked to Net sales from orderline\n',
	CHANNEL_ID VARCHAR(16777216) COMMENT 'Unique channel id to identify order and fulfullmint mode\n',
	ALTERNATE1_ORDER_ID VARCHAR(16777216) COMMENT 'Captures any other unique id that is provided by source and could be used for estabilishing links to other systems.\n',
	LOAD_ID NUMBER(38,0) COMMENT ' Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT '\nThe Date/Datetime the Record was Inserted',
	UPDATE_ID NUMBER(38,0) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKORDER primary key (ORDER_ID)
)COMMENT='This entity refers to a particular order placed by a customer. \n\ndate key : The date in which the order was sold. \ntime key : It refers to the time of the order.\nrestaurant key : It is essentially a combination of restaurant number and a particular version.\npos key : It refers to a pos name\nemployee key : Employee who sold the order.\npop key : This is point of purchase key.Point of Purchase could be a channel as well. E.g, Drive thru,Mobile,Online etc.\norder id : This is a surrogate key from ParBrink side \norder number : actual Order number (System generated)\norder name : It''s essentially a customer name\ntaxexempt id : This is applicable for an organization which has tax exempt such as non-profit,govt etc.\nguest count : always 1\ngross quantity : Each item gets added to the header\ngross amount : It does not include tax.\npayment amount : Net amount - (discount + promo) + surcharge + tax\n\n '
;
create TABLE IF NOT EXISTS ORDER_COMBO (
	ORDER_ID VARCHAR(16777216),
	EMPLOYEE_ID VARCHAR(16777216),
	BUSINESS_DT DATE,
	STORE_ID VARCHAR(16777216),
	CHECK_NBR VARCHAR(16777216),
	OPENED_TM TIMESTAMP_NTZ(9),
	CLOSED_TM TIMESTAMP_NTZ(9),
	ORDER_NM VARCHAR(16777216),
	ITEM_CNT NUMBER(38,0),
	TRANSACTION_CNT NUMBER(38,0),
	SOURCE_GROSS_AMT NUMBER(18,2),
	SOURCE_DISCOUNT_AMT NUMBER(18,2),
	SOURCE_NET_AMT NUMBER(18,2),
	SURCHARGE_AMT NUMBER(18,2),
	TAX_AMT NUMBER(18,2),
	SOURCE_PAYMENT_AMT NUMBER(18,2),
	GRATUITY_AMT NUMBER(18,2),
	CUSTOMER_NM VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_NBR VARCHAR(16777216),
	FIRST_SEND_TM TIMESTAMP_NTZ(9),
	CLOSED_IND BOOLEAN,
	FUTURE_ORDER_IND BOOLEAN,
	VOID_IND BOOLEAN,
	REFUND_IND BOOLEAN,
	TAX_EXEMPT_IND BOOLEAN,
	GUEST_CNT NUMBER(38,0),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	TIME_KEY NUMBER(38,0),
	LOAD_TYP VARCHAR(16777216),
	DERIVED_GROSS_AMT NUMBER(18,2),
	DERIVED_DISCOUNT_AMT NUMBER(18,2),
	DERIVED_NET_AMT NUMBER(18,2),
	DERIVED_PAYMENT_AMT NUMBER(18,2),
	MISC_CHARGE_AMT NUMBER(18,2),
	CHANNEL_ID VARCHAR(16777216),
	ALTERNATE1_ORDER_ID VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	COMBO_TRANSACTION_IND VARCHAR(16777216)
);
create TABLE IF NOT EXISTS ORDER_LINE (
	ORDER_LINE_ID VARCHAR(16777216) NOT NULL COMMENT 'Unique ID for each line item in an order',
	ORDER_ID VARCHAR(16777216) COMMENT 'OrderID uniquely defines each order\n',
	ORDER_LINE_TYP VARCHAR(16777216) COMMENT 'Indicates if the line is linked to an item sale, discount or other type.\n',
	CHANNEL_ID VARCHAR(16777216) COMMENT 'Unique channel id to identify order and fulfullmint mode',
	MDM_ITEM_ID NUMBER(38,0) COMMENT 'MDM item ID for the item ordered',
	MDM_PARENT_ITEM_ID NUMBER(38,0) COMMENT 'MDM item ID for the parent item',
	BUSINESS_DT DATE COMMENT 'Date of the Order',
	TIME_KEY NUMBER(38,0) COMMENT 'Time sec  at which an order was created.',
	STORE_ID VARCHAR(16777216) COMMENT 'Store id at which the order was placed',
	EMPLOYEE_ID VARCHAR(16777216) COMMENT 'Employee responsible for taking the order',
	REGISTER_ID VARCHAR(16777216) COMMENT 'The register that received payment for the order',
	TAX_ID VARCHAR(16777216) COMMENT 'Rate at which customer is taxed for an item.',
	SEAT_NBR VARCHAR(16777216) COMMENT 'Table/seat number where the customer is seated',
	SOURCE_GROSS_QTY NUMBER(38,0) COMMENT 'Quantity of an item placed in an order provided by the source',
	PRICE_AMT NUMBER(18,2) COMMENT 'Price of the item',
	SOURCE_GROSS_AMT NUMBER(18,2) COMMENT 'Gross Amount of the Line Item provided by source',
	SOURCE_NET_AMT NUMBER(18,2) COMMENT 'Net Amount of the Line Item provided by source',
	TAX_AMT NUMBER(18,2) COMMENT 'Tax Amount for the line item',
	CLEARED_IND BOOLEAN COMMENT 'Indicates if the item was removed before subtotal or payment\n',
	DELETED_IND BOOLEAN COMMENT 'Deleting an item occurs if it is removed after the cashier “subtotals” the order but before payment is made.\n',
	VOID_IND BOOLEAN COMMENT 'Indicates whether the order was cancelled \n',
	INVENTORY_IND BOOLEAN COMMENT '”IsInventory” essentially means does the item have to be thrown in the trash once it’s voided (food) or can it be re-sold (coffee mug).\n',
	MODIFIER_IND BOOLEAN COMMENT 'Indicates if the items was a modifier for a given order \n',
	TAX_EXEMPT_ID VARCHAR(16777216) COMMENT 'This is the customer Tax Exempt Identifier\n',
	MANAGER_ID VARCHAR(16777216) COMMENT 'The ID number of the manager authorizing a void, refund etc.\n',
	DERIVED_GROSS_QTY NUMBER(38,0) COMMENT 'Quantity for Pre tax and pre discounted amount.\n',
	DERIVED_GROSS_AMT NUMBER(18,2) COMMENT 'Pre tax and pre discounted amount.\n',
	DERIVED_DISCOUNT_AMT NUMBER(18,2) COMMENT 'Amount of discount applied to a line item.\n',
	DERIVED_NET_AMT NUMBER(18,2) COMMENT 'Pre tax and after discount amount \n',
	SOURCE_ITEM_ID VARCHAR(16777216) COMMENT 'Unique Id for an item name \n',
	SOURCE_ITEM_PLU_ID VARCHAR(16777216) COMMENT 'This is the PLU number from the POS \n',
	SOURCE_ITEM_DESC VARCHAR(16777216) COMMENT 'Name of the item \n',
	SOURCE_PARENT_ITEM_ID VARCHAR(16777216) COMMENT 'This is used to establish Parent - child relation for the items \n',
	DISCOUNT_CD VARCHAR(16777216) COMMENT 'A key/code linked to the discount\n',
	DISCOUNT_NM VARCHAR(16777216) COMMENT 'Name of the discount that is applied\n',
	SOURCE_DISCOUNT_AMT NUMBER(18,2) COMMENT 'Discount Amount for an order provided by source\n',
	SOURCE_DISCOUNT_TYP VARCHAR(16777216) COMMENT 'Captures the type of discount provided by source.\n',
	ORDER_LINE_PARENT_ID VARCHAR(16777216) COMMENT 'Parent Line Id for the item that was discounted.\n',
	MISC_CHARGE_AMT NUMBER(18,2) COMMENT 'Amount linked to purchase of items that don’t contribute to net sales\n',
	ITEM_TYPE_CD VARCHAR(16777216) COMMENT 'The type code to indicate the type of miscellaneous charge. Will be referencing lookup table\n',
	ALTERNATE1_ORDER_ID VARCHAR(16777216) COMMENT 'Captures any other unique id that is provided by source and could be used for estabilishing links to other systems.\n',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand for the Order\n',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'The POS System used to load the transaction  information\n',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_ID NUMBER(38,0) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID NUMBER(38,0) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKORDER_LINE primary key (ORDER_LINE_ID)
)COMMENT='\n\nregister id : this is generated from the terminal\nline item id :\nparent line item id :\n'
;
create TABLE IF NOT EXISTS ORDER_LINE_COMBO (
	ORDER_LINE_ID VARCHAR(16777216),
	ORDER_ID VARCHAR(16777216),
	ORDER_LINE_TYP VARCHAR(16777216),
	CHANNEL_ID VARCHAR(16777216),
	MDM_ITEM_ID NUMBER(38,0),
	MDM_PARENT_ITEM_ID NUMBER(38,0),
	BUSINESS_DT DATE,
	TIME_KEY NUMBER(38,0),
	STORE_ID VARCHAR(16777216),
	EMPLOYEE_ID VARCHAR(16777216),
	REGISTER_ID VARCHAR(16777216),
	TAX_ID VARCHAR(16777216),
	SEAT_NBR VARCHAR(16777216),
	SOURCE_GROSS_QTY NUMBER(38,0),
	PRICE_AMT NUMBER(18,2),
	SOURCE_GROSS_AMT NUMBER(18,2),
	SOURCE_NET_AMT NUMBER(18,2),
	TAX_AMT NUMBER(18,2),
	CLEARED_IND BOOLEAN,
	DELETED_IND BOOLEAN,
	VOID_IND BOOLEAN,
	INVENTORY_IND BOOLEAN,
	MODIFIER_IND BOOLEAN,
	TAX_EXEMPT_ID VARCHAR(16777216),
	MANAGER_ID VARCHAR(16777216),
	DERIVED_GROSS_QTY NUMBER(38,0),
	DERIVED_GROSS_AMT NUMBER(18,2),
	DERIVED_DISCOUNT_AMT NUMBER(18,2),
	DERIVED_NET_AMT NUMBER(18,2),
	SOURCE_ITEM_ID VARCHAR(16777216),
	SOURCE_ITEM_PLU_ID VARCHAR(16777216),
	SOURCE_ITEM_DESC VARCHAR(16777216),
	SOURCE_PARENT_ITEM_ID VARCHAR(16777216),
	DISCOUNT_CD VARCHAR(16777216),
	DISCOUNT_NM VARCHAR(16777216),
	SOURCE_DISCOUNT_AMT NUMBER(18,2),
	SOURCE_DISCOUNT_TYP VARCHAR(16777216),
	ORDER_LINE_PARENT_ID VARCHAR(16777216),
	MISC_CHARGE_AMT NUMBER(18,2),
	ITEM_TYPE_CD VARCHAR(16777216),
	ALTERNATE1_ORDER_ID VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_TYP VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS ORDER_PAYMENT (
	PAYMENT_ID VARCHAR(16777216) NOT NULL COMMENT 'This column creates a unique payment record',
	ORDER_ID VARCHAR(16777216) NOT NULL COMMENT 'OrderID associates each unique paymentID record with the order that it paid for',
	EMPLOYEE_ID VARCHAR(16777216) COMMENT 'The ID of Employee taking the customer’s order',
	PAYMENT_TYP VARCHAR(16777216) COMMENT 'Card issuer, exactly as reported by the source system. May be full name e.g. “Visa” or abbreviation e.g. “VS”',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand for the Order',
	STORE_ID VARCHAR(16777216) COMMENT 'Unique identifier for the store where the purchase was made',
	BUSINESS_DT DATE COMMENT 'BusinessDate is the date when the order was taken. The exception is for 24-hour stores that may define the beginning of a new “business day” as being at 2:00AM. So an order at 1:30AM on 11/11/20 would be counted towards the sales of BusinessDate 11/10/20.\n\n',
	VOID_IND BOOLEAN COMMENT 'Indicates whether the order was cancelled \n',
	INSPIRE_ID VARCHAR(16777216) COMMENT 'Masked card number used to pay for order. Concatenates first six card digits, six “x”''s, last four card digits, and cardholder name.',
	ACCOUNT_NM VARCHAR(16777216) COMMENT 'Cardholder’s name',
	CARD_TYP VARCHAR(16777216) COMMENT 'Specifies whether payment was by credit card, cash, debit card, Sonic gift card, EBT card or delivery service (e.g. Door Dash, GrubHub etc.). Card Issuer (e.g. Visa, Mastercard etc.) is not specified.',
	CARD_ISSUER_NM VARCHAR(16777216) COMMENT 'Name of card issuer or delivery service.',
	PAYMENT_FIRST6_NBR VARCHAR(16777216) COMMENT 'First six digits of the payment card for the order.',
	PAYMENT_LAST4_NBR VARCHAR(16777216) COMMENT 'Last four digits of the payment card for the order.',
	PAYMENT_AUTH_CD VARCHAR(16777216) COMMENT 'This is the 6-character authorization code provided by the card issuer',
	EXPIRATION_DT DATE COMMENT 'Expiration date of the payment card used to purchase the order.',
	PAYMENT_AMT NUMBER(38,8) COMMENT 'Payment Amount reported by the credit card processor. Includes tip and delivery charge.',
	AMT NUMBER(38,8) COMMENT 'Payment Amount as reported by the Point Of Sale system. Includes everything but the tip (Includes delivery charge)',
	TRANSACTION_TM VARCHAR(16777216) COMMENT 'Time when the card transaction was made.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'The POS System used to load the transaction  information\n',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_ID VARCHAR(16777216) COMMENT ' Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.) \n',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT '\nThe Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKORDER_PAYMENT primary key (PAYMENT_ID, ORDER_ID)
)COMMENT='This entity referes to the payment made towards a particular order.\n\npayment type id : \npayment desc : This describes method of payment being used such as mastercard,visa,gift card etc.\npayment auth id : It refers to the authentication id after transaction is approved.\npayment name : same as customer name'
;
create TABLE IF NOT EXISTS PERIOD_COMP_SALES_TEST (
	BRAND VARCHAR(16777216),
	FISC_YEAR_NBR NUMBER(38,0),
	FISC_QUARTER_NBR NUMBER(38,0),
	FISC_PERIOD_NBR NUMBER(38,0),
	STORE_NBR VARCHAR(16777216),
	STORE_STATUS_TYPE VARCHAR(16777216),
	CURRENT_OWN VARCHAR(16777216),
	OWNER_AT_SALES_TIME VARCHAR(16777216),
	ORDER_CHANNEL VARCHAR(16777216),
	NET_SALES NUMBER(38,2),
	COMP_SALES_TY_CY NUMBER(38,2),
	COMP_SALES_LY_CY NUMBER(38,2),
	COMP_SALES_TY_PY NUMBER(38,2),
	COMP_SALES_LY_PY NUMBER(38,2),
	COMP_SALES_TY_PPY NUMBER(38,2),
	COMP_SALES_LY_PPY NUMBER(38,2),
	COMP_TRANS_TY_CY NUMBER(38,0),
	COMP_TRANS_LY_CY NUMBER(38,0),
	COMP_TRANS_TY_PY NUMBER(38,0),
	COMP_TRANS_LY_PY NUMBER(38,0),
	COMP_TRANS_TY_PPY NUMBER(38,0),
	COMP_TRANS_LY_PPY NUMBER(38,0)
);
create TABLE IF NOT EXISTS PERIOD_FLASH_COMPARABLE_SALES (
	BRAND_ID VARCHAR(16777216),
	FISC_YR_NBR NUMBER(38,0),
	FISC_QTR_NBR NUMBER(38,0),
	FISC_PERIOD_NBR NUMBER(38,0),
	WEEKS_IN_PERIOD NUMBER(38,0),
	STORE_ID VARCHAR(16777216),
	SALES_TIME_OWNER VARCHAR(16777216),
	ORDER_CHANNEL_TYPE VARCHAR(16777216),
	NET_SALES NUMBER(38,2),
	COMP_SALES_THIS_YR NUMBER(38,2),
	COMP_SALES_LAST_YR_CURR_YR NUMBER(38,2),
	COMP_SALES_THIS_YR_PREV_YR NUMBER(38,2),
	COMP_SALES_LAST_YR_PREV_YR NUMBER(38,2),
	COMP_SALES_THIS_YR_PREV_2_YR NUMBER(38,2),
	COMP_SALES_LAST_YR_PREV_2_YR NUMBER(38,2),
	COMP_TRANS_THIS_YR NUMBER(38,0),
	COMP_TRANS_LAST_YR_CURR_YR NUMBER(38,0),
	COMP_TRANS_THIS_YR_PREV_YR NUMBER(38,0),
	COMP_TRANS_LAST_YR_PREV_YR NUMBER(38,0),
	COMP_TRANS_THIS_YR_PREV_2_YR NUMBER(38,0),
	COMP_TRANS_LAST_YR_PREV_2_YR NUMBER(38,0),
	LOAD_TYPE VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)\n',
	LOAD_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated'
);
create TABLE IF NOT EXISTS WEEKLY_FLASH_COMPARABLE_SALES (
	FISCAL_WEEK_NBR NUMBER(38,8) NOT NULL COMMENT 'Fiscal week number for business date',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand associated with store \n\n',
	STORE_NBR VARCHAR(16777216) NOT NULL COMMENT 'Number that uniquely identifies the Store',
	FISCAL_YEAR_NBR NUMBER(38,0) NOT NULL COMMENT 'Fiscal year number for business date',
	FISCAL_WEEK_START_DT DATE COMMENT 'Current year fiscal week start date of sales transaction\n',
	FISCAL_WEEK_END_DT DATE COMMENT 'Current year fiscal week start date of sales transaction\n',
	DMA_NM VARCHAR(16777216) COMMENT 'Designated marketing area code name for the store\n\n',
	DMA_CD VARCHAR(16777216) COMMENT 'Designated marketing area code for the store',
	LEVEL1_NM VARCHAR(16777216) COMMENT '\t\nOps hierarchy L1 name',
	LEVEL2_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L2 name',
	LEVEL3_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L3 name',
	LEVEL4_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L4 name',
	LEVEL5_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L5 name',
	OWNERSHIP_TYP VARCHAR(16777216) COMMENT 'Type of store ownership',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'Address line 1 for the store\n\n',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Address line 2 for the store',
	CITY_NM VARCHAR(16777216) COMMENT 'City for the store',
	ZIP_CD VARCHAR(16777216) COMMENT 'Zip code for the store',
	STATE_CD VARCHAR(16777216) COMMENT 'State of the store',
	COUNTRY_CD VARCHAR(16777216) COMMENT 'Country of the store',
	FISCAL_QUARTER_NBR NUMBER(38,0) COMMENT 'Fiscal quarter number for business date',
	FISCAL_PERIOD_NBR NUMBER(38,0) COMMENT 'Fiscal period number for business date\n',
	CURRENT_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Current year net sales amount for the aggregate level',
	CURRENT_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of transactions for the aggregate level',
	CURRENT_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for the current year business date',
	CURRENT_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for the current year business date',
	CURRENT_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Current year comp sales amount for the aggregate level',
	CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of comp transactions for the aggregate level',
	CURRENT_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales on the current year business date',
	CURRENT_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for the current year business date',
	FISCAL_LAST_YEAR_WEEK_START_DT DATE COMMENT 'Last year fiscal week start date associated with the current year fiscal week',
	FISCAL_LAST_YEAR_WEEK_END_DT DATE COMMENT 'Last Year fiscal week end date associated with current year fiscal week',
	FISCAL_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_AVERAGE_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year fiscal comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_WEEK_START_DT DATE COMMENT 'Last year calendar comparable week start date associated with current year fiscal week',
	CALENDAR_LAST_YEAR_WEEK_END_DT DATE COMMENT 'Last year calendar comparable week end date associated with current year fiscal week',
	CALENDAR_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_WEEK_START_DT DATE COMMENT '2 Year calendar week start date for current year fiscal week',
	CALENDAR_2YEAR_WEEK_END_DT DATE COMMENT '2 Year calendar week end date associated with current year fiscal week',
	CALENDAR_2YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_2YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_WEEK_START_DT DATE COMMENT '3 year calendar comparable week start week date associated with the current year fiscal week.',
	CALENDAR_3YEAR_WEEK_END_DT DATE COMMENT '3 Year calendar comparable week end date associated with current year fiscal week',
	CALENDAR_3YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKWEEKLY_FLASH_COMPARABLE_SALES primary key (FISCAL_WEEK_NBR, BRAND_ID, STORE_NBR, FISCAL_YEAR_NBR)
)COMMENT='The Inspire Daily Flash view is a materialized view populated from the Inspire Daily Flash function and supports daily flash sales analytics for a business date, by brand and store. This materialized view has been created to enhance performance of the queries against the dataset. '
;
create TABLE IF NOT EXISTS WEEKLY_FLASH_DAYPART_FULFILMENT_COMPARABLE_SALES (
	CHANNEL_ID VARCHAR(16777216) NOT NULL COMMENT 'Unique order fulfillment channel identifier\n',
	FISCAL_WEEK_NBR NUMBER(38,0) NOT NULL COMMENT 'Fiscal week number for business date',
	CLOSED_HOUR_TM TIME(9) NOT NULL COMMENT 'Hour in which the order was closed (completed) in military time\n',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand associated with store \n\n',
	STORE_NBR VARCHAR(16777216) COMMENT 'Number that uniquely identifies the Store',
	FISCAL_WEEK_START_DT DATE COMMENT 'Current year fiscal week start date of sales transaction\n',
	FISCAL_WEEK_END_DT DATE COMMENT 'Current year fiscal week start date of sales transaction\n',
	IRB_DAYPART_NM VARCHAR(16777216) COMMENT 'Inspire enterprise daypart associated with the CLOSED_HOUR\n',
	BRAND_DAYPART_NM VARCHAR(16777216) COMMENT 'Brand specific daypart associated with the CLOSED_HOUR\n',
	ORDER_CHANNEL_HIERARCHY_LEVEL1_NM VARCHAR(16777216) COMMENT 'Level 1 channel hierarchy for an order at a restaurant.\n',
	ORDER_CHANNEL_HIERARCHY_LEVEL2_NM VARCHAR(16777216) COMMENT 'Level 2 channel hierarchy for an order at a restaurant.\n',
	ORDER_CHANNEL_HIERARCHY_LEVEL3_NM VARCHAR(16777216) COMMENT 'Level 3 channel hierarchy for an order at a restaurant.\n',
	FULFILMENT_CHANNEL_HIERARCHY_LEVEL1_NM VARCHAR(16777216) COMMENT 'Level 1 fulfillment channel hierarchy for an order at a restaurant.\n',
	FULFILMENT_CHANNEL_HIERARCHY_LEVEL2_NM VARCHAR(16777216) COMMENT 'Level 2 fulfillment channel hierarchy for an order at a restaurant.\n',
	FULFILMENT_CHANNEL_HIERARCHY_LEVEL3_NM VARCHAR(16777216) COMMENT 'Level 3 fulfillment channel hierarchy for an order at a restaurant.\n',
	DMA_NM VARCHAR(16777216) COMMENT 'Designated marketing area code name for the store\n\n',
	DMA_CD VARCHAR(16777216) COMMENT 'Designated marketing area code for the store',
	LEVEL1_NM VARCHAR(16777216) COMMENT '\t\nOps hierarchy L1 name',
	LEVEL2_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L2 name',
	LEVEL3_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L3 name',
	LEVEL4_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L4 name',
	LEVEL5_NM VARCHAR(16777216) COMMENT 'Ops hierarchy L5 name',
	OWNERSHIP_TYP VARCHAR(16777216) COMMENT 'Type of store ownership',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'Address line 1 for the store\n\n',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Address line 2 for the store',
	CITY_NM VARCHAR(16777216) COMMENT 'City for the store',
	ZIP_CD VARCHAR(16777216) COMMENT 'Zip code for the store',
	STATE_CD VARCHAR(16777216) COMMENT 'State of the store',
	COUNTRY_CD VARCHAR(16777216) COMMENT 'Country of the store',
	FISCAL_YEAR_NBR NUMBER(38,0) COMMENT 'Fiscal year number for business date',
	FISCAL_PERIOD_NBR NUMBER(38,0) COMMENT 'Fiscal period number for business date\n',
	FISCAL_QUARTER_NBR NUMBER(38,0) COMMENT 'Fiscal quarter number for business date',
	DAY_OF_WEEK_NM VARCHAR(16777216) COMMENT 'Name of day for business date',
	CURRENT_YEAR_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for business date',
	CURRENT_YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for business date',
	CURRENT_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Current year net sales amount for the aggregate level',
	CURRENT_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of transactions for the aggregate level',
	CURRENT_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for the current year business date',
	CURRENT_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for the current year business date',
	CURRENT_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Current year comp sales amount for the aggregate level',
	CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Current year count of comp transactions for the aggregate level',
	CURRENT_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales on the current year business date',
	CURRENT_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for the current year business date',
	FISCAL_LAST_YEAR_WEEK_START_DT DATE COMMENT 'Last year fiscal week start date associated with the current year fiscal week',
	FISCAL_LAST_YEAR_WEEK_END_DT DATE COMMENT 'Last Year fiscal week end date associated with current year fiscal week',
	FISCAL_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_AVERAGE_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year fiscal comparable date associated with the current year business date',
	FISCAL_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year fiscal comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for last year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_LAST_YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for last year calendar comparable date associated with the current year business date',
	CALENDAR_LAST_YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for last year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_WEEK_START_DT DATE COMMENT '2 Year calendar week start date for current year fiscal week',
	CALENDAR_2YEAR_WEEK_END_DT DATE COMMENT '2 Year calendar week end date associated with current year fiscal week',
	CALENDAR_2YEAR_COMPARABLE_DT DATE COMMENT '2 year calendar comparable date associated with the current year business date for aggregate level.',
	CALENDAR_2YEAR_COMPARABLE_HOLIDAY_IND VARCHAR(16777216) COMMENT 'Holiday indicator for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_HOLIDAY_NM VARCHAR(16777216) COMMENT 'Holiday name for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 2 year calendar comparable date associated with the current year business date',
	CALENDAR_2YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_2YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Comp store counter for 2 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_WEEK_START_DT DATE COMMENT '3 year calendar comparable week start week date associated with the current year fiscal week.',
	CALENDAR_3YEAR_WEEK_END_DT DATE COMMENT '3 Year calendar comparable week end date associated with current year fiscal week',
	CALENDAR_3YEAR_NET_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level net sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level count of transactions for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_SALES_AMT NUMBER(18,2) COMMENT 'Aggregate level comp sales amount for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT NUMBER(38,0) COMMENT 'Aggregate level comp transaction count for 3 year calendar comparable date associated with the current year business date for aggregate level.\n',
	CALENDAR_3YEAR_COMPARABLE_AVERAGE_CHECK_AMT NUMBER(18,2) COMMENT 'Average check amount for comp sales for 3 year calendar comparable date associated with the current year business date\n',
	CALENDAR_3YEAR_COMPARABLE_STORE_CNT NUMBER(38,0) COMMENT 'Store counter based on sales for 3 year calendar comparable date associated with the current year business date\n',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKWEEKLY_FLASH_DAYPART_FULFILMENT_COMPARABLE_SALES primary key (CHANNEL_ID, FISCAL_WEEK_NBR, CLOSED_HOUR_TM)
)COMMENT='The Inspire Daily Flash view is a materialized view populated from the Inspire Daily Flash function and supports daily flash sales analytics for a business date, by brand and store. This materialized view has been created to enhance performance of the queries against the dataset. '
;
CREATE PROCEDURE IF NOT EXISTS INSERT_PEROD_COMP_FLASH_SALES("FROMDATE" DATE, "TODATE" DATE)
RETURNS VARCHAR(16777216)
LANGUAGE SCALA
RUNTIME_VERSION = '2.12'
PACKAGES = ('com.snowflake:snowpark:1.4.0')
HANDLER = 'INSERT_PEROD_COMP_FLASH_SALES.run'
EXECUTE AS CALLER
AS '
object INSERT_PEROD_COMP_FLASH_SALES {
  import java.sql.Date

  def run(session: com.snowflake.snowpark.Session, FromDate: Date, ToDate: Date): String = {
    import com.snowflake.snowpark.TableFunction
    import com.snowflake.snowpark.{Row,DataFrame, Column}
    import com.snowflake.snowpark.functions._


    def toCol(colName: String): DataFrame => Column = df => df(colName)
    def toColConst(const: Any): DataFrame => Column = _ => lit(const)
    def toColSQLExpr(expr: String): DataFrame => Column = _ => sqlExpr(expr)

    val load_type = if (FromDate != null && ToDate != null) "incremental" else "historical"

    lazy val mapping: Map[String, DataFrame => Column] = Map(
      "BRAND_ID"  -> toCol("BRAND_ID"),
      "FISC_YR_NBR" -> toCol("FISC_YEAR_NBR"),
      "FISC_QTR_NBR"  -> toCol("FISC_QUARTER_NBR"),
      "FISC_PERIOD_NBR" -> toCol("FISC_PERIOD_NBR"),
      "WEEKS_IN_PERIOD" -> toCol("WEEKS_IN_PERIOD_NBR"),
      "STORE_ID" -> toCol("STORE_NBR"),
      "SALES_TIME_OWNER" -> toCol("owner_at_sales_time"),
      "ORDER_CHANNEL_TYPE"  -> toCol("order_channel"),
      "NET_SALES"  -> toCol("NET_SALES"),
      "COMP_SALES_THIS_YR" -> toCol("COMP_SALES_TY_CY"),
      "COMP_SALES_LAST_YR_CURR_YR" -> toCol("COMP_SALES_LY_CY"),
      "COMP_SALES_THIS_YR_PREV_YR"  -> toCol("COMP_SALES_TY_PY"),
      "COMP_SALES_LAST_YR_PREV_YR"  -> toCol("COMP_SALES_LY_PY"),
      "COMP_SALES_THIS_YR_PREV_2_YR" -> toCol("COMP_SALES_TY_PPY"),
      "COMP_SALES_LAST_YR_PREV_2_YR" -> toCol("COMP_SALES_LY_PPY"),
      "COMP_TRANS_THIS_YR" -> toCol("COMP_TRANS_TY_CY"),
      "COMP_TRANS_LAST_YR_CURR_YR" -> toCol("COMP_TRANS_LY_CY"),
      "COMP_TRANS_THIS_YR_PREV_YR"  -> toCol("COMP_TRANS_TY_PY"),
      "COMP_TRANS_LAST_YR_PREV_YR"  -> toCol("COMP_TRANS_LY_PY"),
      "COMP_TRANS_THIS_YR_PREV_2_YR" -> toCol("COMP_TRANS_TY_PPY"),
      "COMP_TRANS_LAST_YR_PREV_2_YR" -> toCol("COMP_TRANS_LY_PPY"),
      "LOAD_TYPE" -> toColConst(load_type),
      "LOAD_ID" -> toColSQLExpr("cast(to_varchar(SYSDATE(),''yyyymmddhh24missFF3'') as NUMBER(38,0))"),
      "LOAD_DTTM" -> toColSQLExpr("SYSDATE()"),
      "UPDATE_ID" -> toColSQLExpr("cast(to_varchar(SYSDATE(),''yyyymmddhh24missFF3'') as NUMBER(38,0))"),
      "UPDATE_DTTM" -> toColSQLExpr("SYSDATE()")
    )
    
    var ingestable_periods: Array[Row] = null

    if (FromDate != null && ToDate != null) {

      ingestable_periods = session.table("IDS_DEV.INT_REF.COMP_DATES")
        .filter(col("fiscal_period_start_dt")
          .between(lit(FromDate),
            iff(lit(ToDate) > current_date(), current_date(), lit(ToDate)))
          and col("fiscal_period_end_dt").between(lit(FromDate),
          iff(lit(ToDate) > current_date(), current_date(), lit(ToDate))))
        .select(col("fiscal_period_start_dt"), col("fiscal_period_end_dt"),col("FISCAL_YEAR_NBR"),col("FISCAL_PERIOD_NBR"))
        .distinct()
        .sort(col("fiscal_period_start_dt"))
        .collect()


    }
    else {
      val latest_calculated = session.sql("select " +
        "FISC_YR_NBR,FISC_QTR_NBR,FISC_PERIOD_NBR " +
        "from PERIOD_FLASH_COMPARABLE_SALES " +
        "qualify (row_number() over (order by FISC_YR_NBR desc, FISC_QTR_NBR desc, FISC_PERIOD_NBR desc))=1;")
        .collect().head

      val lc_year = latest_calculated.getInt(0)
      val lc_quarter = latest_calculated.getInt(1)
      val lc_period = latest_calculated.getInt(2)

      val lc_timeindicator = lc_year.toString + lc_quarter.toString + lc_period.toString

      ingestable_periods = session.table("IDS_DEV.INT_REF.COMP_DATES")
        .where(
          col("time_interval") === lit("CY") and
            current_date() > col("fiscal_period_end_dt")
            and concat(col("fiscal_year_nbr"),
            col("fiscal_quarter_nbr"),
            col("fiscal_period_nbr")
          ) > lit(lc_timeindicator))
        .select("fiscal_period_start_dt", "fiscal_period_end_dt","FISCAL_YEAR_NBR","FISCAL_PERIOD_NBR")
        .distinct()
        .sort(col("fiscal_period_start_dt"))
        .collect()

    }

    if (ingestable_periods != null) {
      ingestable_periods.foreach(row=> {

        val period_computation = session.tableFunction(
          TableFunction("PERIOD_FLASH_SUMMARY"), lit(row.getDate(0)), lit(row.getDate(1)))

        val tbDeleted = session.table("PERIOD_FLASH_COMPARABLE_SALES")
        val deleted = tbDeleted.delete(tbDeleted("FISC_YR_NBR")===lit(row.getInt(2)) and tbDeleted("FISC_PERIOD_NBR")===lit(row.getInt(3)))


        val target = session.table("PERIOD_FLASH_COMPARABLE_SALES")

        target.merge(period_computation,
          target("BRAND_ID") === mapping("BRAND_ID")(period_computation) and
            target("FISC_YR_NBR")=== mapping("FISC_YR_NBR")(period_computation) and
            target("FISC_QTR_NBR") === mapping("FISC_QTR_NBR")(period_computation) and
            target("FISC_PERIOD_NBR") === mapping("FISC_PERIOD_NBR")(period_computation) and
            target("STORE_ID") === mapping("STORE_ID")(period_computation) and
            coalesce(target("ORDER_CHANNEL_TYPE"),lit("")) === coalesce(mapping("ORDER_CHANNEL_TYPE")(period_computation),lit(""))
        ).whenNotMatched.insert(
          mapping.transform(
            (key, f) => f(period_computation)
          )
        ).collect()
      })
    }
    "success"
  }
}
  ';
CREATE FUNCTION IF NOT EXISTS PERIOD_FLASH_INIT("DATE_FROM" DATE, "DATE_TO" DATE, "TIME_INTERVAL" VARCHAR(16777216))
RETURNS TABLE ("CALENDAR_DT" DATE, "BRAND_ID" VARCHAR(16777216), "STORE_ID" VARCHAR(16777216), "FISCAL_YEAR_NBR" NUMBER(38,0), "FISCAL_QUARTER_NBR" NUMBER(38,0), "FISCAL_PERIOD_NBR" NUMBER(38,0), "WEEKS_IN_PERIOD_NBR" NUMBER(38,0), "NET_SALES" NUMBER(38,2), "TRANS_CNT" NUMBER(38,0), "COMP_SALES" NUMBER(38,2), "COMP_TRANS_CNT" NUMBER(38,0), "CHANNEL_ID" VARCHAR(16777216), "OPEN_15P_FLAG" BOOLEAN, "OPEN_AFTER_HONEYMOON_FLAG" BOOLEAN, "CLOSED_BEFORE_PERIOD_END_FLAG1" BOOLEAN, "TEMP_CLOSED_FLAG1" BOOLEAN, "FISCAL_PERIOD_START_DT" DATE, "STORE_CNT" NUMBER(38,0), "COMP_STORE_CNT" NUMBER(38,0))
LANGUAGE SQL
AS '

with a as
(  
    select
        DATE_FROM,
        DATE_TO,
        TIME_INTERVAL,
        CALENDAR_DT,
        MAX_OPEN_DT,
        MAX_HONEY_DT
    FROM table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_HONEYMOON(DATE_FROM, DATE_TO, TIME_INTERVAL)) as honeymoon
), chs as (
    SELECT DISTINCT CHANNEL_ID, STORE_ID, BRAND_ID
    FROM IDS_DEV.TXN_BV.DAILY_DAY_PART_FLASH_SALES_FACT_BV as dfsf
    WHERE TIME_INTERVAL = ''CY''
),
DT as (
    SELECT
        DT.CALENDAR_DT,
        DT.DATE_KEY,
        DT.FISCAL_YEAR_NBR,
        DT.FISCAL_QUARTER_NBR,
        DT.FISCAL_PERIOD_NBR,
        DT.FISCAL_WEEK_NBR,
        count(distinct DT.FISCAL_WEEK_NBR) over (partition by fiscal_year_nbr,FISCAL_QUARTER_NBR,fiscal_period_nbr) as WEEKS_IN_PERIOD_NBR,
        DT.CALENDAR_DAY_NM,
        DT.FISCAL_WEEK_START_DT,
        MAX_OPEN_DT,
        MAX_HONEY_DT,
        DT.FISCAL_PERIOD_START_DT,
        DT.FISCAL_PERIOD_END_DT
    FROM a
    INNER join IDS_DEV.INT_REF.COMP_DATES DT
        on DT.CALENDAR_DT = a.CALENDAR_DT
        and DT.TIME_INTERVAL = TIME_INTERVAL
),
ST as (
    SELECT distinct
        DT.CALENDAR_DT,
        DT.FISCAL_YEAR_NBR,
        DT.FISCAL_QUARTER_NBR,
        DT.FISCAL_PERIOD_NBR,
        DT.FISCAL_WEEK_NBR,
        DT.WEEKS_IN_PERIOD_NBR,
        DT.CALENDAR_DAY_NM,
        DT.FISCAL_WEEK_START_DT,
        DT.DATE_KEY,
        ST.STORE_ID,
        ST.BRAND_ID,
        nvl(ST.OPEN_DT, DT.MAX_OPEN_DT) <= DT.MAX_OPEN_DT as OPEN_15P_FLAG, -- ODI-53
        nvl(ST.OPEN_DT, DT.MAX_HONEY_DT) <= DT.MAX_HONEY_DT as OPEN_AFTER_HONEYMOON_FLAG, -- ODI-199,
        chs.CHANNEL_ID,
        NVL(ST.CLOSURE_DT, DT.FISCAL_PERIOD_END_DT) < DT.FISCAL_PERIOD_END_DT as CLOSED_BEFORE_PERIOD_END_FLAG,
        (least(nvl(ST.REOPEN_DT -1,DT.FISCAL_PERIOD_END_DT), DT.FISCAL_PERIOD_END_DT)
       - greatest(nvl(ST.TEMP_CLOSED_DT,DT.FISCAL_PERIOD_START_DT), DT.FISCAL_PERIOD_START_DT) >= 6 
        and ST.TEMP_CLOSED_DT is not null) AS TEMP_CLOSED_FLAG,
        DT.FISCAL_PERIOD_START_DT
    FROM DT
    INNER JOIN IDH_DEV.SHARED.STORE_SCD_V ST  
        ON CURRENT_IND = 1 AND IFNULL(st.COUNTRY_NM, '''') in (''US'', ''USA'')
    INNER JOIN (
        select BDT.FISCAL_YEAR_START_DT
        from IDS_DEV.INT_REF.COMP_DATES BDT
        where BDT.TIME_INTERVAL = ''CAL3Y''
        and BDT.BUSINESS_DT = DATE_FROM
    ) as c3y_dt
        ON nvl(ST.CLOSURE_DT, c3y_dt.FISCAL_YEAR_START_DT) >= c3y_dt.FISCAL_YEAR_START_DT
    INNER JOIN (select distinct STORE_ID, BRAND_ID from IDS_DEV.TXN_BV.DAILY_DAY_PART_FLASH_SALES_FACT_BV as f) as dpsf
        on ST.store_ID = dpsf.STORE_ID and ST.BRAND_ID = dpsf.BRAND_ID
    LEFT JOIN chs
        ON  chs.STORE_ID = ST.STORE_ID
        AND chs.BRAND_ID = ST.BRAND_ID
),
FINAL AS (
    SELECT
    ST.CALENDAR_DT,
    ST.BRAND_ID
    , ST.STORE_ID
    , ST.FISCAL_YEAR_NBR as FISCAL_YEAR_NBR
    , ST.FISCAL_QUARTER_NBR as FISCAL_QUARTER_NBR
    , ST.FISCAL_PERIOD_NBR as FISCAL_PERIOD_NBR
    , ST.WEEKS_IN_PERIOD_NBR as WEEKS_IN_PERIOD_NBR
    , sum(nvl(dpsf.NET_SALES_AMT, 0)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.CALENDAR_DT,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN  TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END) as Net_Sales
    , sum(iff(nvl(dpsf.NET_SALES_AMT,0) = 0,0, nvl(dpsf.TRX_CNT,0))) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,
        ST.CALENDAR_DT,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END ) as Trans_Cnt
    , sum(CASE WHEN ST.FISCAL_WEEK_NBR = 53  then 0 else nvl(dpsf.NET_SALES_AMT, 0) end) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.CALENDAR_DT,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN  TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END) as Comp_Sales
    , sum(iff(nvl(dpsf.NET_SALES_AMT,0) = 0,0, CASE WHEN ST.FISCAL_WEEK_NBR = 53  then 0 else nvl(dpsf.TRX_CNT,0)end)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,
        ST.CALENDAR_DT,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END ) as Comp_Trans_Cnt
    , CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
    ELSE null
    END AS CHANNEL_ID
    , ST.OPEN_15P_FLAG as OPEN_15P_FLAG1
    , ST.OPEN_AFTER_HONEYMOON_FLAG as OPEN_AFTER_HONEYMOON_FLAG1
    , ST.CLOSED_BEFORE_PERIOD_END_FLAG as CLOSED_BEFORE_PERIOD_END_FLAG1
    , ST.TEMP_CLOSED_FLAG as TEMP_CLOSED_FLAG1
    , ST.FISCAL_PERIOD_START_DT
    , CASE
        WHEN sum(nvl(dpsf.NET_SALES_AMT, 0)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.CALENDAR_DT) = 0
        THEN 0 ELSE 1 END as STORE_CNT
    , CASE
        WHEN sum(nvl(dpsf.NET_SALES_AMT, 0)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.CALENDAR_DT) = 0 THEN 0
        WHEN CLOSED_BEFORE_PERIOD_END_FLAG1 OR TEMP_CLOSED_FLAG1
        THEN 0
        WHEN TIME_INTERVAL = ''CY'' AND (NOT OPEN_AFTER_HONEYMOON_FLAG1 OR NOT OPEN_15P_FLAG1)
        THEN 0
        WHEN TIME_INTERVAL <> ''CY'' AND NOT OPEN_AFTER_HONEYMOON_FLAG1
        THEN 0
        ELSE 1
    END                                     as COMP_STORE_CNT
FROM
    IDS_DEV.TXN_BV.DAILY_DAY_PART_FLASH_SALES_FACT_BV as dpsf
RIGHT JOIN ST
    ON  ST.STORE_ID = dpsf.STORE_ID
    and ST.brand_id = dpsf.brand_id
    AND ST.CALENDAR_DT = dpsf.BUSINESS_DT
AND CASE
        WHEN TIME_INTERVAL = ''CY''
        THEN nvl(dpsf.CHANNEL_ID, ST.CHANNEL_ID) = ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY''
        THEN dpsf.CHANNEL_ID is not null
    ELSE true END
)
SELECT * FROM FINAL
QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_ID, BRAND_ID, CALENDAR_DT, CHANNEL_ID order by CALENDAR_DT) = 1
';
CREATE FUNCTION IF NOT EXISTS PERIOD_FLASH_SUMMARY("DATE_FROM" DATE, "DATE_TO" DATE, "TIME_INTERVAL" VARCHAR(16777216))
RETURNS TABLE ("BRAND_ID" VARCHAR(16777216), "STORE_ID" VARCHAR(16777216), "FISCAL_YEAR_NBR" NUMBER(38,0), "FISCAL_QUARTER_NBR" NUMBER(38,0), "FISCAL_PERIOD_NBR" NUMBER(38,0), "NET_SALES" NUMBER(38,2), "TRANS_CNT" NUMBER(38,0), "COMP_SALES" NUMBER(38,2), "COMP_TRANS_CNT" NUMBER(38,0), "CHANNEL_ID" VARCHAR(16777216), "OPEN_15P_FLAG" BOOLEAN, "OPEN_AFTER_HONEYMOON_FLAG" BOOLEAN, "CLOSED_BEFORE_PERIOD_END_FLAG1" BOOLEAN, "TEMP_CLOSED_FLAG1" BOOLEAN, "FISCAL_PERIOD_START_DT" DATE, "STORE_CNT" NUMBER(38,0), "COMP_STORE_CNT" NUMBER(38,0))
LANGUAGE SQL
AS '

with a as
(  
    select
        DATE_FROM,
        DATE_TO,
        TIME_INTERVAL,
        CALENDAR_DT,
        MAX_OPEN_DT,
        MAX_HONEY_DT
    FROM table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_HONEYMOON(DATE_FROM, DATE_TO, TIME_INTERVAL)) as honeymoon
), chs as (
    SELECT DISTINCT CHANNEL_ID, STORE_ID, BRAND_ID
    FROM IDS_DEV.DATASTORE.DAILY_DAY_PART_FLASH_SALES_FACT as dfsf
    WHERE TIME_INTERVAL = ''CY''
),
DT as (
    SELECT
        DT.CALENDAR_DT,
        DT.DATE_KEY,
        DT.FISCAL_YEAR_NBR,
        DT.FISCAL_QUARTER_NBR,
        DT.FISCAL_PERIOD_NBR,
        DT.FISCAL_WEEK_NBR,
        DT.CALENDAR_DAY_NM,
        DT.FISCAL_WEEK_START_DT,
        MAX_OPEN_DT,
        MAX_HONEY_DT,
        DT.FISCAL_PERIOD_START_DT,
        DT.FISCAL_PERIOD_END_DT
    FROM a
    INNER join IDS_DEV.INT_REF.COMP_DATES DT
        on DT.CALENDAR_DT = a.CALENDAR_DT
        and DT.TIME_INTERVAL = TIME_INTERVAL
),
ST as (
    SELECT distinct
        DT.CALENDAR_DT,
        DT.FISCAL_YEAR_NBR,
        DT.FISCAL_QUARTER_NBR,
        DT.FISCAL_PERIOD_NBR,
        DT.FISCAL_WEEK_NBR,
        DT.CALENDAR_DAY_NM,
        DT.FISCAL_WEEK_START_DT,
        DT.DATE_KEY,
        ST.STORE_ID,
        ST.BRAND_ID,
        nvl(ST.OPEN_DT, DT.MAX_OPEN_DT) <= DT.MAX_OPEN_DT as OPEN_15P_FLAG, -- ODI-53
        nvl(ST.OPEN_DT, DT.MAX_HONEY_DT) <= DT.MAX_HONEY_DT as OPEN_AFTER_HONEYMOON_FLAG, -- ODI-199,
        chs.CHANNEL_ID,
        NVL(ST.CLOSURE_DT, DT.FISCAL_PERIOD_END_DT) < DT.FISCAL_PERIOD_END_DT as CLOSED_BEFORE_PERIOD_END_FLAG,
        (least(nvl(ST.REOPEN_DT -1,DT.FISCAL_PERIOD_END_DT), DT.FISCAL_PERIOD_END_DT)
       - greatest(nvl(ST.TEMP_CLOSED_DT,DT.FISCAL_PERIOD_START_DT), DT.FISCAL_PERIOD_START_DT) >= 6 
        and ST.TEMP_CLOSED_DT is not null) AS TEMP_CLOSED_FLAG,
        DT.FISCAL_PERIOD_START_DT
    FROM DT
    INNER JOIN IDH_DEV.SHARED.STORE_SCD_V ST  
        ON CURRENT_IND = 1 AND IFNULL(st.COUNTRY_NM, '''') in (''US'', ''USA'')
    INNER JOIN (
        select BDT.FISCAL_YEAR_START_DT
        from IDS_DEV.INT_REF.COMP_DATES BDT
        where BDT.TIME_INTERVAL = ''CAL3Y''
        and BDT.BUSINESS_DT = DATE_FROM
    ) as c3y_dt
        ON nvl(ST.CLOSURE_DT, c3y_dt.FISCAL_YEAR_START_DT) >= c3y_dt.FISCAL_YEAR_START_DT
    INNER JOIN (select distinct STORE_ID, BRAND_ID from IDS_DEV.DATASTORE.DAILY_DAY_PART_FLASH_SALES_FACT as f) as dpsf
        on ST.store_ID = dpsf.STORE_ID and ST.BRAND_ID = dpsf.BRAND_ID
    LEFT JOIN chs
        ON  chs.STORE_ID = ST.STORE_ID
        AND chs.BRAND_ID = ST.BRAND_ID
),
FINAL AS (
    SELECT
    ST.BRAND_ID
    , ST.STORE_ID
    , ST.FISCAL_YEAR_NBR as FISCAL_YEAR_NBR
    , ST.FISCAL_QUARTER_NBR as FISCAL_QUARTER_NBR
    , ST.FISCAL_PERIOD_NBR as FISCAL_PERIOD_NBR
    , sum(nvl(dpsf.NET_SALES_AMT, 0)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.FISCAL_PERIOD_NBR,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN  TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END) as Net_Sales
    , sum(iff(nvl(dpsf.NET_SALES_AMT,0) = 0,0, nvl(dpsf.TRX_CNT,0))) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,
        ST.FISCAL_PERIOD_NBR,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END ) as Trans_Cnt
    , sum(CASE WHEN ST.FISCAL_WEEK_NBR = 53  then 0 else nvl(dpsf.NET_SALES_AMT, 0) end) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.FISCAL_PERIOD_NBR,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN  TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END) as Comp_Sales
    , sum(iff(nvl(dpsf.NET_SALES_AMT,0) = 0,0, CASE WHEN ST.FISCAL_WEEK_NBR = 53  then 0 else nvl(dpsf.TRX_CNT,0)end)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,
        ST.FISCAL_PERIOD_NBR,
        CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
        ELSE null END ) as Comp_Trans_Cnt
    , CASE
        WHEN TIME_INTERVAL = ''CY'' THEN ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY'' THEN dpsf.CHANNEL_ID
    ELSE null
    END AS CHANNEL_ID
    , ST.OPEN_15P_FLAG as OPEN_15P_FLAG1
    , ST.OPEN_AFTER_HONEYMOON_FLAG as OPEN_AFTER_HONEYMOON_FLAG1
    , ST.CLOSED_BEFORE_PERIOD_END_FLAG as CLOSED_BEFORE_PERIOD_END_FLAG1
    , ST.TEMP_CLOSED_FLAG as TEMP_CLOSED_FLAG1
    , ST.FISCAL_PERIOD_START_DT
    , CASE
        WHEN sum(nvl(dpsf.NET_SALES_AMT, 0)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.FISCAL_PERIOD_NBR) = 0
        THEN 0 ELSE 1 END as STORE_CNT
    , CASE
        WHEN sum(nvl(dpsf.NET_SALES_AMT, 0)) OVER (PARTITION BY ST.BRAND_ID, ST.STORE_ID,ST.FISCAL_PERIOD_NBR) = 0 THEN 0
        WHEN CLOSED_BEFORE_PERIOD_END_FLAG1 OR TEMP_CLOSED_FLAG1
        THEN 0
        WHEN TIME_INTERVAL = ''CY'' AND (NOT OPEN_AFTER_HONEYMOON_FLAG1 OR NOT OPEN_15P_FLAG1)
        THEN 0
        WHEN TIME_INTERVAL <> ''CY'' AND NOT OPEN_AFTER_HONEYMOON_FLAG1
        THEN 0
        ELSE 1
    END                                     as COMP_STORE_CNT
FROM
    IDS_DEV.DATASTORE.DAILY_DAY_PART_FLASH_SALES_FACT as dpsf
RIGHT JOIN ST
    ON  ST.STORE_ID = dpsf.STORE_ID
    and ST.brand_id = dpsf.brand_id
    AND ST.CALENDAR_DT = dpsf.BUSINESS_DT
AND CASE
        WHEN TIME_INTERVAL = ''CY''
        THEN nvl(dpsf.CHANNEL_ID, ST.CHANNEL_ID) = ST.CHANNEL_ID
        WHEN TIME_INTERVAL <> ''CY''
        THEN dpsf.CHANNEL_ID is not null
    ELSE true END
)
SELECT * FROM FINAL
QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_ID, BRAND_ID, FISCAL_PERIOD_NBR, CHANNEL_ID order by FISCAL_PERIOD_NBR) = 1
';
CREATE FUNCTION IF NOT EXISTS PERIOD_FLASH_SUMMARY("FROM_DATE" DATE, "TO_DATE" DATE)
RETURNS TABLE ("BRAND_ID" VARCHAR(16777216), "FISC_YEAR_NBR" NUMBER(38,0), "FISC_QUARTER_NBR" NUMBER(38,0), "FISC_PERIOD_NBR" NUMBER(38,0), "WEEKS_IN_PERIOD_NBR" NUMBER(38,0), "STORE_NBR" VARCHAR(16777216), "OWNER_AT_SALES_TIME" VARCHAR(16777216), "ORDER_CHANNEL" VARCHAR(16777216), "NET_SALES" NUMBER(38,2), "COMP_SALES_TY_CY" NUMBER(38,2), "COMP_SALES_LY_CY" NUMBER(38,2), "COMP_SALES_TY_PY" NUMBER(38,2), "COMP_SALES_LY_PY" NUMBER(38,2), "COMP_SALES_TY_PPY" NUMBER(38,2), "COMP_SALES_LY_PPY" NUMBER(38,2), "COMP_TRANS_TY_CY" NUMBER(38,0), "COMP_TRANS_LY_CY" NUMBER(38,0), "COMP_TRANS_TY_PY" NUMBER(38,0), "COMP_TRANS_LY_PY" NUMBER(38,0), "COMP_TRANS_TY_PPY" NUMBER(38,0), "COMP_TRANS_LY_PPY" NUMBER(38,0))
LANGUAGE SQL
AS '
with comps_channel_id as (
SELECT 
    coalesce(cy.BRAND_ID, cly.BRAND_ID, c2y.BRAND_ID, c3y.BRAND_ID) as BRAND_ID
    , coalesce(cy.STORE_ID, cly.STORE_ID, c2y.STORE_ID, c3y.STORE_ID)
                                                    as STORE_NBR
    , coalesce(cy.CHANNEL_ID, cly.CHANNEL_ID, c2y.CHANNEL_ID, c3y.CHANNEL_ID)
                                                    as CHANNEL_ID
    , coalesce(cy.FISCAL_YEAR_NBR, cly.FISCAL_YEAR_NBR + 1, c2y.FISCAL_YEAR_NBR + 2, c3y.FISCAL_YEAR_NBR + 3)
                                                    as FISC_YEAR_NBR
    , coalesce(cy.FISCAL_QUARTER_NBR, cly.FISCAL_QUARTER_NBR, c2y.FISCAL_QUARTER_NBR, c3y.FISCAL_QUARTER_NBR)
                                                    as FISC_QUARTER_NBR
    , coalesce(cy.FISCAL_PERIOD_NBR, cly.FISCAL_PERIOD_NBR, c2y.FISCAL_PERIOD_NBR, c3y.FISCAL_PERIOD_NBR)
                                                    as FISCAL_PERIOD_NBR
    , coalesce(cy.WEEKS_IN_PERIOD_NBR, cly.WEEKS_IN_PERIOD_NBR, c2y.WEEKS_IN_PERIOD_NBR, c3y.WEEKS_IN_PERIOD_NBR)
                                                    as WEEKS_IN_PERIOD_NBR
    , cy.NET_SALES                                  as NET_SALES
    , nvl(iff(cy.COMP_STORE_CNT = 1, cy.Comp_Sales, 0),0)   as COMP_SALES_TY_CY
    , nvl(iff(cly.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, cly.Comp_Sales, 0),0)  as  COMP_SALES_LY_CY
    , nvl(iff(cly.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, cly.Comp_Sales, 0),0)  as  COMP_SALES_TY_PY
    , nvl(iff(c2y.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, c2y.Comp_Sales, 0),0)  as  COMP_SALES_LY_PY
    , nvl(iff(c2y.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, c2y.Comp_Sales, 0),0)  as  COMP_SALES_TY_PPY
    , nvl(iff(c3y.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, c3y.Comp_Sales, 0),0)  as  COMP_SALES_LY_PPY

    , nvl(iff(cy.COMP_STORE_CNT = 1, cy.Comp_Trans_Cnt, 0),0)   as COMP_TRANS_TY_CY
    , nvl(iff(cly.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, cly.Comp_Trans_Cnt, 0),0)  as  COMP_TRANS_LY_CY
    , nvl(iff(cly.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, cly.Comp_Trans_Cnt, 0),0)  as  COMP_TRANS_TY_PY
    , nvl(iff(c2y.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, c2y.Comp_Trans_Cnt, 0),0)  as  COMP_TRANS_LY_PY
    , nvl(iff(c2y.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, c2y.Comp_Trans_Cnt, 0),0)  as  COMP_TRANS_TY_PPY
    , nvl(iff(c3y.COMP_STORE_CNT = 1 and cy.COMP_STORE_CNT = 1, c3y.Comp_Trans_Cnt, 0),0)  as  COMP_TRANS_LY_PPY

FROM IDS_DEV.INT_REF.COMP_CALENDAR dt
INNER JOIN table(PERIOD_FLASH_INIT(FROM_DATE, TO_DATE,''CY'')) as cy
    on dt.CALENDAR_DT = cy.CALENDAR_DT
LEFT JOIN table(PERIOD_FLASH_INIT(FROM_DATE, TO_DATE, ''CAL1Y'')) as cly
    on cly.BRAND_ID = cy.BRAND_ID
    and cly.STORE_ID = cy.STORE_ID
    and dt.CALENDAR_1YR_COMP_DT  = cly.CALENDAR_DT
    and cly.CHANNEL_ID = cy.CHANNEL_ID
LEFT JOIN table(PERIOD_FLASH_INIT(FROM_DATE, TO_DATE, ''CAL2Y'')) as c2y
    on c2y.BRAND_ID = cy.BRAND_ID
    and c2y.STORE_ID = cy.STORE_ID
    and dt.CALENDAR_2YR_COMP_DT  = c2y.CALENDAR_DT
    and c2y.CHANNEL_ID = cy.CHANNEL_ID
LEFT JOIN table(PERIOD_FLASH_INIT(FROM_DATE, TO_DATE, ''CAL3Y'')) as c3y
    on c3y.BRAND_ID = cy.BRAND_ID
    and c3y.STORE_ID = cy.STORE_ID
    and dt.CALENDAR_3YR_COMP_DT = c3y.CALENDAR_DT
    and c3y.CHANNEL_ID = cy.CHANNEL_ID
-- TO CHECK
-- where cy.CHANNEL_ID  is not null OR cy.ADD_NULL_WEEK
--     and (
--         dt.CALENDAR_DT = cy.FISCAL_WEEK_START_DT
--         or dt.FISCAL_1YR_COMP_WEEK_DT = fly.FISCAL_WEEK_START_DT
--         or dt.CALENDAR_1YR_COMP_WEEK_DT = cly.FISCAL_WEEK_START_DT
--         or dt.CALENDAR_2YR_COMP_WEEK_DT = c2y.FISCAL_WEEK_START_DT 
--         or dt.CALENDAR_3YR_COMP_WEEK_DT = c3y.FISCAL_WEEK_START_DT
--     )
),
ch_grouped as (
select
        cci.BRAND_ID,
        cci.STORE_NBR,
        cci.FISC_YEAR_NBR,
        cci.FISC_QUARTER_NBR,
        cci.FISCAL_PERIOD_NBR,
        cci.WEEKS_IN_PERIOD_NBR,
        c_V.ORDER_CHANNEL_HIERARCHY_LEVEL3_NM as order_channel,
        sum(cci.NET_SALES) as NET_SALES,
        sum(cci.COMP_SALES_TY_CY) as COMP_SALES_TY_CY,
        sum(cci.COMP_SALES_LY_CY) as COMP_SALES_LY_CY,
        sum(cci.COMP_SALES_TY_PY) as COMP_SALES_TY_PY,
        sum(cci.COMP_SALES_LY_PY) as COMP_SALES_LY_PY,
        sum(cci.COMP_SALES_TY_PPY) as COMP_SALES_TY_PPY,
        sum(cci.COMP_SALES_LY_PPY) as COMP_SALES_LY_PPY,
        sum(cci.COMP_TRANS_TY_CY) as COMP_TRANS_TY_CY,
        sum(cci.COMP_TRANS_LY_CY) as COMP_TRANS_LY_CY,
        sum(cci.COMP_TRANS_TY_PY) as COMP_TRANS_TY_PY,
        sum(cci.COMP_TRANS_LY_PY) as COMP_TRANS_LY_PY,
        sum(cci.COMP_TRANS_TY_PPY) as COMP_TRANS_TY_PPY,
        sum(cci.COMP_TRANS_LY_PPY) as COMP_TRANS_LY_PPY

    from comps_channel_id cci
    left join (select * from IDH_DEV.SALES.ORDER_CHANNEL_V chan_V where chan_V.CHANNEL_ID is not null) c_V
    on cci.CHANNEL_ID = c_V.CHANNEL_ID
    group by 
        cci.BRAND_ID,
        cci.STORE_NBR,
        cci.FISC_YEAR_NBR,
        cci.FISC_QUARTER_NBR,
        cci.FISCAL_PERIOD_NBR,
        cci.WEEKS_IN_PERIOD_NBR,
        c_V.ORDER_CHANNEL_HIERARCHY_LEVEL3_NM
)
select 
       cg.BRAND_ID,
       cg.FISC_YEAR_NBR,
       cg.FISC_QUARTER_NBR,
       cg.FISCAL_PERIOD_NBR,
       cg.WEEKS_IN_PERIOD_NBR,
       cg.STORE_NBR,
       old_re.FRANCHISEE_NM as owner_at_sales_time,
       cg.order_channel,
       cg.NET_SALES,
       cg.COMP_SALES_TY_CY,
       cg.COMP_SALES_LY_CY,
       cg.COMP_SALES_TY_PY,
       cg.COMP_SALES_LY_PY,
       cg.COMP_SALES_TY_PPY,
       cg.COMP_SALES_LY_PPY,
       cg.COMP_TRANS_TY_CY,
       cg.COMP_TRANS_LY_CY,
       cg.COMP_TRANS_TY_PY,
       cg.COMP_TRANS_LY_PY,
       cg.COMP_TRANS_TY_PPY,
       cg.COMP_TRANS_LY_PPY

from ch_grouped cg
left join IDM_DEV.COREDIM.RESTAURANT_SCD_DIM old_re
    on cg.brand_ID = old_re.brand_id
    and cg.store_nbr = old_re.store_id
    and TO_DATE BETWEEN old_re.EFFECTIVE_BEGIN_DT and old_re.EFFECTIVE_END_DT
';
CREATE FUNCTION IF NOT EXISTS SONIC_AMT_FORMAT("SOURCE_DATA" VARCHAR(16777216))
RETURNS FLOAT
LANGUAGE SQL
AS '
select case right(source_data,1)
when ''}'' then -CONCAT(substr(replace(source_data,''}'',''0''),1,3),''.'',substr(replace(source_data,''}'',''0''),4,2))
when ''J'' then -CONCAT(substr(replace(source_data,''J'',''1''),1,3),''.'',substr(replace(source_data,''J'',''1''),4,2))
when ''K'' then -CONCAT(substr(replace(source_data,''K'',''2''),1,3),''.'',substr(replace(source_data,''K'',''2''),4,2))
when ''L'' then -CONCAT(substr(replace(source_data,''L'',''3''),1,3),''.'',substr(replace(source_data,''L'',''3''),4,2))
when ''M'' then -CONCAT(substr(replace(source_data,''M'',''4''),1,3),''.'',substr(replace(source_data,''M'',''4''),4,2))
when ''N'' then -CONCAT(substr(replace(source_data,''N'',''5''),1,3),''.'',substr(replace(source_data,''N'',''5''),4,2))
when ''O'' then -CONCAT(substr(replace(source_data,''O'',''6''),1,3),''.'',substr(replace(source_data,''O'',''6''),4,2))
when ''P'' then -CONCAT(substr(replace(source_data,''P'',''7''),1,3),''.'',substr(replace(source_data,''P'',''7''),4,2))
when ''Q'' then -CONCAT(substr(replace(source_data,''Q'',''8''),1,3),''.'',substr(replace(source_data,''Q'',''8''),4,2))
when ''R'' then -CONCAT(substr(replace(source_data,''R'',''9''),1,3),''.'',substr(replace(source_data,''R'',''9''),4,2))
ELSE CONCAT(substr(source_data,1,3),''.'',substr(source_data,4,2))
end as Amt

';
CREATE PROCEDURE IF NOT EXISTS STR_PROC_MERGE_INTO_DIGITAL_SUGGESTED_SELL_DIM_TABLES("SOURCE_DB_PARAM" VARCHAR(16777216), "SOURCE_SCHEMA_PARAM" VARCHAR(16777216), "DESTINATION_DB_PARAM" VARCHAR(16777216), "DESTINATION_SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var insert_digital_suggested_sell_cart = `MERGE INTO ` +DESTINATION_DB_PARAM+ `.` +DESTINATION_SCHEMA_PARAM+ `.DIGITAL_SUGGESTED_SELL_CART D USING (
                                                    SELECT DISTINCT
                                                        IFF(upper(header_brand_id) = ''ARB'', ''arbys'', header_brand_id) as BRAND_ID,
                                                        ''IDP'' AS SOURCE_SYSTEM_NM,
                                                        correlationId AS ORDER_CORRELATION_ID,
                                                        recommendationId AS SUGGESTED_SELL_RECOMMENDATION_ID,
                                                        itemsincart_value AS CART_ITEM_ID,
                                                        itemsincart_index AS CART_ITEM_SEQ_NBR,
                                                        LOAD_ID,
                                                        LOAD_DTTM
                                                    FROM ` +SOURCE_DB_PARAM+ `.` +SOURCE_SCHEMA_PARAM+ `.IDP_SUGGESTED_SELL_FLAT_STREAM
                                                    WHERE metadata$action = ''INSERT'' AND CART_ITEM_ID IS NOT NULL
                                                ) S
                                                ON S.BRAND_ID = D.BRAND_ID AND
                                                S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM AND
                                                S.ORDER_CORRELATION_ID = D.ORDER_CORRELATION_ID AND
                                                S.SUGGESTED_SELL_RECOMMENDATION_ID = D.SUGGESTED_SELL_RECOMMENDATION_ID AND
                                                S.CART_ITEM_ID = D.CART_ITEM_ID AND
                                                S.CART_ITEM_SEQ_NBR = D.CART_ITEM_SEQ_NBR
                                                WHEN MATCHED THEN UPDATE SET
                                                    D.UPDATE_ID = S.LOAD_ID,
                                                    D.UPDATE_DTTM = S.LOAD_DTTM
                                                WHEN NOT matched THEN INSERT (
                                                    BRAND_ID,
                                                    SOURCE_SYSTEM_NM,
                                                    ORDER_CORRELATION_ID,
                                                    SUGGESTED_SELL_RECOMMENDATION_ID,
                                                    CART_ITEM_ID,
                                                    CART_ITEM_SEQ_NBR,
                                                    LOAD_ID,
                                                    LOAD_DTTM,
                                                    UPDATE_ID,
                                                    UPDATE_DTTM)
                                                VALUES (
                                                    S.BRAND_ID,
                                                    S.SOURCE_SYSTEM_NM,
                                                    S.ORDER_CORRELATION_ID,
                                                    S.SUGGESTED_SELL_RECOMMENDATION_ID,
                                                    S.CART_ITEM_ID,
                                                    S.CART_ITEM_SEQ_NBR,
                                                    S.LOAD_ID,
                                                    S.LOAD_DTTM,
                                                    S.LOAD_ID,
                                                    S.LOAD_DTTM);`

    var insert_digital_suggested_sell_recommendation = `MERGE INTO ` +DESTINATION_DB_PARAM+ `.` +DESTINATION_SCHEMA_PARAM+ `.DIGITAL_SUGGESTED_SELL_RECOMMENDATION D USING (
                                                            SELECT DISTINCT
                                                                IFF(upper(header_brand_id) = ''ARB'', ''arbys'', header_brand_id) as BRAND_ID,
                                                                ''IDP'' AS SOURCE_SYSTEM_NM,
                                                                correlationId AS ORDER_CORRELATION_ID,
                                                                recommendationId AS SUGGESTED_SELL_RECOMMENDATION_ID,
                                                                recommendeditems_value AS RECOMMENDATION_ITEM_ID,
                                                                recommendeditems_index AS RECOMMENDATION_ITEM_SEQ_NBR,
                                                                LOAD_ID,
                                                                LOAD_DTTM
                                                            FROM ` +SOURCE_DB_PARAM+ `.` +SOURCE_SCHEMA_PARAM+ `.IDP_SUGGESTED_SELL_FLAT_STREAM
                                                            WHERE metadata$action = ''INSERT'' AND RECOMMENDATION_ITEM_ID IS NOT NULL
                                                        ) S
                                                        ON S.BRAND_ID = D.BRAND_ID AND
                                                        S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM AND
                                                        S.ORDER_CORRELATION_ID = D.ORDER_CORRELATION_ID AND
                                                        S.SUGGESTED_SELL_RECOMMENDATION_ID = D.SUGGESTED_SELL_RECOMMENDATION_ID AND
                                                        S.RECOMMENDATION_ITEM_ID = D.RECOMMENDATION_ITEM_ID
                                                        WHEN MATCHED THEN UPDATE SET
                                                            D.RECOMMENDATION_ITEM_SEQ_NBR = S.RECOMMENDATION_ITEM_SEQ_NBR,
                                                            D.UPDATE_ID = S.LOAD_ID,
                                                            D.UPDATE_DTTM = S.LOAD_DTTM
                                                        WHEN NOT matched THEN INSERT (
                                                            BRAND_ID,
                                                            SOURCE_SYSTEM_NM,
                                                            ORDER_CORRELATION_ID,
                                                            SUGGESTED_SELL_RECOMMENDATION_ID,
                                                            RECOMMENDATION_ITEM_ID,
                                                            RECOMMENDATION_ITEM_SEQ_NBR,
                                                            LOAD_ID,
                                                            LOAD_DTTM,
                                                            UPDATE_ID,
                                                            UPDATE_DTTM)
                                                        VALUES (
                                                            S.BRAND_ID,
                                                            S.SOURCE_SYSTEM_NM,
                                                            S.ORDER_CORRELATION_ID,
                                                            S.SUGGESTED_SELL_RECOMMENDATION_ID,
                                                            S.RECOMMENDATION_ITEM_ID,
                                                            S.RECOMMENDATION_ITEM_SEQ_NBR,
                                                            S.LOAD_ID,
                                                            S.LOAD_DTTM,
                                                            S.LOAD_ID,
                                                            S.LOAD_DTTM);`

    var insert_digital_suggested_sell = `MERGE INTO ` +DESTINATION_DB_PARAM+ `.` +DESTINATION_SCHEMA_PARAM+ `.DIGITAL_SUGGESTED_SELL D USING (
                                            SELECT DISTINCT
                                                IFF(upper(header_brand_id) = ''ARB'', ''arbys'', header_brand_id) as BRAND_ID,
                                                ''IDP'' AS SOURCE_SYSTEM_NM,
                                                correlationId AS ORDER_CORRELATION_ID,
                                                recommendationId AS SUGGESTED_SELL_RECOMMENDATION_ID,
                                                customerSegment AS TARGET_SALE_SEGMENT_CD,
                                                locationId AS LOC_ID,
                                                event_enqueued_time AS RECOMMENDATION_DTTM,
                                                basketId AS BASKET_ID,
                                                recommendationFilename AS ITEM_RECOMMENDATION_FILENAME,
                                                header_source_channel AS CUST_SOURCE_CHANNEL_CD,
                                                header_source_sub_channel AS CUST_SOURCE_SUB_CHANNEL_CD,
                                                header_event_type AS EVENT_TYP_CD,
                                                header_event_sub_type AS EVENT_SUB_TYP_CD,
                                                header_event_status AS EVENT_STATUS_CD,
                                                LOAD_ID,
                                                LOAD_DTTM
                                            FROM ` +SOURCE_DB_PARAM+ `.` +SOURCE_SCHEMA_PARAM+ `.IDP_SUGGESTED_SELL_FLAT_STREAM
                                            WHERE metadata$action = ''INSERT''
                                        ) S
                                        ON S.BRAND_ID = D.BRAND_ID AND
                                        S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM AND
                                        S.ORDER_CORRELATION_ID = D.ORDER_CORRELATION_ID AND
                                        S.SUGGESTED_SELL_RECOMMENDATION_ID = D.SUGGESTED_SELL_RECOMMENDATION_ID
                                        WHEN MATCHED THEN UPDATE SET
                                            D.TARGET_SALE_SEGMENT_CD = S.TARGET_SALE_SEGMENT_CD,
                                            D.LOC_ID = S.LOC_ID,
                                            D.RECOMMENDATION_DTTM = S.RECOMMENDATION_DTTM,
                                            D.BASKET_ID = S.BASKET_ID,
                                            D.ITEM_RECOMMENDATION_FILENAME = S.ITEM_RECOMMENDATION_FILENAME,
                                            D.CUST_SOURCE_CHANNEL_CD = S.CUST_SOURCE_CHANNEL_CD,
                                            D.CUST_SOURCE_SUB_CHANNEL_CD = S.CUST_SOURCE_SUB_CHANNEL_CD,
                                            D.EVENT_TYP_CD = S.EVENT_TYP_CD,
                                            D.EVENT_SUB_TYP_CD = S.EVENT_SUB_TYP_CD,
                                            D.EVENT_STATUS_CD = S.EVENT_STATUS_CD,
                                            D.UPDATE_ID = S.LOAD_ID,
                                            D.UPDATE_DTTM = S.LOAD_DTTM
                                        WHEN NOT matched THEN INSERT (
                                            BRAND_ID,
                                            SOURCE_SYSTEM_NM,
                                            ORDER_CORRELATION_ID,
                                            SUGGESTED_SELL_RECOMMENDATION_ID,
                                            TARGET_SALE_SEGMENT_CD,
                                            LOC_ID,
                                            RECOMMENDATION_DTTM,
                                            BASKET_ID,
                                            ITEM_RECOMMENDATION_FILENAME,
                                            CUST_SOURCE_CHANNEL_CD,
                                            CUST_SOURCE_SUB_CHANNEL_CD,
                                            EVENT_TYP_CD,
                                            EVENT_SUB_TYP_CD,
                                            EVENT_STATUS_CD,
                                            LOAD_ID,
                                            LOAD_DTTM,
                                            UPDATE_ID,
                                            UPDATE_DTTM)
                                        VALUES (
                                            S.BRAND_ID,
                                            S.SOURCE_SYSTEM_NM,
                                            S.ORDER_CORRELATION_ID,
                                            S.SUGGESTED_SELL_RECOMMENDATION_ID,
                                            S.TARGET_SALE_SEGMENT_CD,
                                            S.LOC_ID,
                                            S.RECOMMENDATION_DTTM,
                                            S.BASKET_ID,
                                            S.ITEM_RECOMMENDATION_FILENAME,
                                            S.CUST_SOURCE_CHANNEL_CD,
                                            S.CUST_SOURCE_SUB_CHANNEL_CD,
                                            S.EVENT_TYP_CD,
                                            S.EVENT_SUB_TYP_CD,
                                            S.EVENT_STATUS_CD,
                                            S.LOAD_ID,
                                            S.LOAD_DTTM,
                                            S.LOAD_ID,
                                            S.LOAD_DTTM);`;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );
        snowflake.execute (
            {sqlText: insert_digital_suggested_sell_cart}
            );
        snowflake.execute (
            {sqlText: insert_digital_suggested_sell_recommendation}
            );
        snowflake.execute (
            {sqlText: insert_digital_suggested_sell}
            );
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
create or replace stream DIGITAL_ORDER_CIP_INTAKE_STREAM on table DIGITAL_ORDER;
create or replace task DIGITAL_SUGGESTED_SELL_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.IDP_SUGGESTED_SELL_FLAT_STREAM')
	as call str_proc_merge_into_digital_suggested_sell_dim_tables (
                                                                                'RDS_DEV',
                                                                                'ARB',
                                                                                'IDS_DEV',
                                                                                'TXN');
create or replace task IDP_ORDER_CHANNEL_MAPPING_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_channel_mapping_stream')
	as MERGE INTO DIGITAL_ORDER_CHANNEL_MAPPING D USING (
WITH newest_channels AS (
    SELECT
    'IDP' as SOURCE_SYSTEM_NM,
    IFF(upper(brandid) = 'ARB', 'arbys',brandid) as SOURCE_BRAND_ID,
    sourceChannel as SOURCE_ORDER_CHANNEL_CD,
    orderType as SOURCE_FULFILLMENT_CHANNEL_CD,
    null as TARGET_ORDER_CHANNEL_ID, -- needs research, TARGET_ORDER_CHANNEL_ID => "CDMSYNC_DEV"."IRB"."TRAN_CHANNEL_PLR" or "POLARIS_DEV"."SADM"."CHANNEL_DIM"
    LOAD_ID,
    LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_channel_mapping_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM_NM,SOURCE_BRAND_ID,SOURCE_ORDER_CHANNEL_CD,SOURCE_FULFILLMENT_CHANNEL_CD ORDER BY header_epoch_time desc) = 1
)
    SELECT
    SOURCE_SYSTEM_NM,
    SOURCE_BRAND_ID,
    SOURCE_ORDER_CHANNEL_CD,
    SOURCE_FULFILLMENT_CHANNEL_CD,
    TARGET_ORDER_CHANNEL_ID,
    LOAD_ID,
    LOAD_DTTM
    FROM newest_channels
)S
ON
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM AND
    S.SOURCE_BRAND_ID = D.SOURCE_BRAND_ID AND
    S.SOURCE_ORDER_CHANNEL_CD = D.SOURCE_ORDER_CHANNEL_CD AND
    S.SOURCE_FULFILLMENT_CHANNEL_CD = D.SOURCE_FULFILLMENT_CHANNEL_CD
WHEN MATCHED THEN UPDATE SET
    D.TARGET_ORDER_CHANNEL_ID = S.TARGET_ORDER_CHANNEL_ID,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    SOURCE_SYSTEM_NM,
    SOURCE_BRAND_ID,
    SOURCE_ORDER_CHANNEL_CD,
    SOURCE_FULFILLMENT_CHANNEL_CD,
    TARGET_ORDER_CHANNEL_ID,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.SOURCE_SYSTEM_NM,
    S.SOURCE_BRAND_ID,
    S.SOURCE_ORDER_CHANNEL_CD,
    S.SOURCE_FULFILLMENT_CHANNEL_CD,
    S.TARGET_ORDER_CHANNEL_ID,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_DIGITAL_ORDER_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_digital_order_stream')
	as MERGE INTO DIGITAL_ORDER D USING (
    WITH newest_orders AS (
        SELECT
        IFF(upper(brandid) = 'ARB', 'arbys',brandid)  as BRAND_ID,
        to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
        id as DIGITAL_ORDER_ID,
        location_locationId as LOC_ID,
        'IDP' as SOURCE_SYSTEM_NM,
        coalesce(orderPlacedDateTime,header_epoch_time) as BUSINESS_DTTM,
        null as ORDER_CHANNEL_ID, -- null until they implement mapping to the UDP Order Channel Id
        sourceChannel as ORDER_CHANNEL_CD,
        orderType as FULFILLMENT_CHANNEL_CD,
        orderSubType as FULFILLMENT_CHANNEL_SUB_CD,
        idempotentId as ORDER_IDEMPOTENT_ID,
        status as ORDER_STATUS_CD,
        displayStatus as ORDER_DISPLAY_STATUS_TXT,
        name as ORDER_NM,
        coalesce(try_to_timestamp_ntz(expectedPickupTime, 'YYYY-MM-DDTHH24:MI:SS.FF TZH:TZM'), try_to_timestamp_ntz(expectedPickupTime, 'YYYY-MM-DDTHH24:MI:SS TZH:TZM')) as ORDER_EXPECTED_PICKUP_DTTM,
        coalesce(try_to_timestamp_ntz(fulfillment_fulfillmentTime, 'YYYY-MM-DDTHH24:MI:SS.FF TZH:TZM'), try_to_timestamp_ntz(expectedPickupTime, 'YYYY-MM-DDTHH24:MI:SS TZH:TZM')) as ORDER_FULFILLMENT_DTTM,
        posOrderId as POS_ORDER_ID,
        posStatus as POS_STATUS_CD,
        posResultCode as POS_RESULT_CD,
        posMessage as POS_MESSAGE_TXT,
        subTotal as  ORDER_SUB_TOTAL_AMT,
        totalTax as ORDER_TOTAL_TAX_AMT,
        tip as ORDER_TIP_AMT,
        discount as ORDER_DISCOUNT_AMT,
        discountedPrice as ORDER_DISCOUNT_PRICE_AMT,
        totalPrice as ORDER_TOTAL_AMT,
        details as ORDER_DETAIL_TXT,
        deliveryFee as DELIVERY_FEE_AMOUUNT,
        server as SERVER_EMPLOYEE_ID,
        table_field as TABLE_NBR,
        numberOfGuest as GUEST_CNT,
        storeCheckNumber as CHECK_NBR,
        deliveryTime as DELIVERY_DTTM,
        paymentURL as PAYMENT_URL,
        tallyTime as TALLY_TM,
        customer_customerId as IDP_CUST_ID, -- requires a lookup for the Internal Inspire Customer Id
        customer_membershipNumber as CUST_MEMBERSHIP_NBR,
        customer_customerFirstName as CUST_FIRST_NM,
        customer_customerLastName as CUST_LAST_NM,
        customer_customerPhone as CUST_PHONE_NBR,
        customer_customerEmail as CUST_EMAIL,
        customer_customerAddress_addressLine1 as CUST_ADR_LINE_1_TXT,
        customer_customerAddress_addressLine2 as CUST_ADR_LINE_2_TXT,
        customer_customerAddress_city as CUST_CTY_NM,
        customer_customerAddress_state as CUST_ST_CD,
        customer_customerAddress_zipcode as CUST_ZIP_CD,
        location_locationName as LOC_NM,
        location_locationPhone as LOC_PHONE_NBR,
        location_locationEmail as LOC_EMAIL,
        location_locationAddress_addressLine1 as LOC_ADR_LINE_1_TXT,
        location_locationAddress_addressLine2 as LOC_ADR_LINE_2_TXT,
        location_locationAddress_city as LOC_CTY_NM,
        location_locationAddress_state as LOC_ST_CD,
        location_locationAddress_zipcode as LOC_ZIP_CD,
        location_fraudCheck as LOC_FRAUD_CHECK_IND,
        udpCorrelationId as ORDER_CORRELATION_ID,
        fulfillment_id as FULLFILLMENT_UNIQUE_IDENTIFER,
        fulfillment_fulfillmentId as FULFILLMENT_ID,
        fulfillment_fulfillmentType as FULFILLMENT_TYP_CD,
        fulfillment_statusUrl as FULFILLMENT_STATUS_URL,
        fulfillment_contactFirstName as DELIVERY_CONTACT_FIRST_NM,
        fulfillment_contactLastName as DELIVERY_CONTACT_LAST_NM,
        fulfillment_contactPhone as DELIVERY_CONTACT_PHONE_NBR,
        fulfillment_contactEmail as DELIVERY_CONTACT_EMAIL,
        fulfillment_deliveryAddress_addressLine1 as DELIVERY_ADR_LINE_1_TXT,
        fulfillment_deliveryAddress_addressLine2 as DELIVERY_ADR_LINE_2_TXT,
        fulfillment_deliveryAddress_city as DELIVERY_CTY_NM,
        fulfillment_deliveryAddress_state as DELIVERY_ST_CD,
        fulfillment_deliveryAddress_zipcode as DELIVERY_ZIP_CD,
        fulfillment_deliveryPartner as DELIVERY_PARTNER_NM,
        fulfillment_vehicleDescription as DELIVERY_VEHICLE_DESC,
        fulfillment_deliveryStatus as DELIVERY_STATUS_TXT,
        fulfillment_instructionsFromCustomer as CUST_FULFILLMENT_INSTRUCTION_TXT,
        metadata as ORDER_METADATA,
        fulfillment_pickupInstructions_value as CUST_PICKUP_INSTRUCTION_TXT,
        LOAD_ID,
        LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_digital_order_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
    select
    n.BRAND_ID,
    n.BUSINESS_DT,
    n.DIGITAL_ORDER_ID,
    n.LOC_ID,
    n.SOURCE_SYSTEM_NM,
    n.BUSINESS_DTTM,
    n.ORDER_CHANNEL_ID,
    n.ORDER_CHANNEL_CD,
    n.FULFILLMENT_CHANNEL_CD,
    n.FULFILLMENT_CHANNEL_SUB_CD,
    n.ORDER_IDEMPOTENT_ID,
    n.ORDER_STATUS_CD,
    n.ORDER_DISPLAY_STATUS_TXT,
    n.ORDER_NM,
    n.ORDER_EXPECTED_PICKUP_DTTM,
    n.ORDER_FULFILLMENT_DTTM,
    n.POS_ORDER_ID,
    n.POS_STATUS_CD,
    n.POS_RESULT_CD,
    n.POS_MESSAGE_TXT,
    n.ORDER_SUB_TOTAL_AMT,
    n.ORDER_TOTAL_TAX_AMT,
    n.ORDER_TIP_AMT,
    n.ORDER_DISCOUNT_AMT,
    n.ORDER_DISCOUNT_PRICE_AMT,
    n.ORDER_TOTAL_AMT,
    n.ORDER_DETAIL_TXT,
    n.DELIVERY_FEE_AMOUUNT,
    n.SERVER_EMPLOYEE_ID,
    n.TABLE_NBR,
    n.GUEST_CNT,
    n.CHECK_NBR,
    n.DELIVERY_DTTM,
    n.PAYMENT_URL,
    n.TALLY_TM,
    n.IDP_CUST_ID,
    n.CUST_MEMBERSHIP_NBR,
    n.CUST_FIRST_NM,
    n.CUST_LAST_NM,
    n.CUST_PHONE_NBR,
    n.CUST_EMAIL,
    n.CUST_ADR_LINE_1_TXT,
    n.CUST_ADR_LINE_2_TXT,
    n.CUST_CTY_NM,
    n.CUST_ST_CD,
    n.CUST_ZIP_CD,
    n.LOC_NM,
    coalesce(n.LOC_PHONE_NBR, sd.STORE_PHONE) as LOC_PHONE_NBR,
    coalesce(n.LOC_EMAIL, sd.STORE_EMAIL) as LOC_EMAIL,
    coalesce(n.LOC_ADR_LINE_1_TXT,ADDRESS_1) as LOC_ADR_LINE_1_TXT,
    coalesce(n.LOC_ADR_LINE_2_TXT,ADDRESS_2) as LOC_ADR_LINE_2_TXT, 
    coalesce(n.LOC_CTY_NM,CITY) as LOC_CTY_NM,
    coalesce(n.LOC_ST_CD, STATE) as LOC_ST_CD,
    coalesce(n.LOC_ZIP_CD, ZIP) as LOC_ZIP_CD,
    n.LOC_FRAUD_CHECK_IND,
    n.ORDER_CORRELATION_ID,
    n.FULLFILLMENT_UNIQUE_IDENTIFER,
    n.FULFILLMENT_ID,
    n.FULFILLMENT_TYP_CD,
    n.FULFILLMENT_STATUS_URL,
    n.DELIVERY_CONTACT_FIRST_NM,
    n.DELIVERY_CONTACT_LAST_NM,
    n.DELIVERY_CONTACT_PHONE_NBR,
    n.DELIVERY_CONTACT_EMAIL,
    n.DELIVERY_ADR_LINE_1_TXT,
    n.DELIVERY_ADR_LINE_2_TXT,
    n.DELIVERY_CTY_NM,
    n.DELIVERY_ST_CD,
    n.DELIVERY_ZIP_CD,
    n.DELIVERY_PARTNER_NM,
    n.DELIVERY_VEHICLE_DESC,
    n.DELIVERY_STATUS_TXT,
    n.CUST_FULFILLMENT_INSTRUCTION_TXT,
    n.ORDER_METADATA,
    n.CUST_PICKUP_INSTRUCTION_TXT,
    n.LOAD_ID,
    n.LOAD_DTTM
    FROM newest_orders n
    left join POLARIS_DEV.SHDM.STORE_DIM  sd on lpad(n.LOC_ID,5,0) = sd.store_id and sd.brand_id=n.BRAND_ID
)S
ON
    S.BRAND_ID = D.BRAND_ID AND
    S.BUSINESS_DT = D.BUSINESS_DT AND
    S.DIGITAL_ORDER_ID = D.DIGITAL_ORDER_ID AND
    S.LOC_ID = D.LOC_ID AND
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET
    D.BUSINESS_DTTM = S.BUSINESS_DTTM,
    D.ORDER_CHANNEL_ID = S.ORDER_CHANNEL_ID,
    D.ORDER_CHANNEL_CD = S.ORDER_CHANNEL_CD,
    D.FULFILLMENT_CHANNEL_CD = S.FULFILLMENT_CHANNEL_CD,
    D.FULFILLMENT_CHANNEL_SUB_CD = S.FULFILLMENT_CHANNEL_SUB_CD,
    D.ORDER_IDEMPOTENT_ID = S.ORDER_IDEMPOTENT_ID,
    D.ORDER_STATUS_CD = S.ORDER_STATUS_CD,
    D.ORDER_DISPLAY_STATUS_TXT = S.ORDER_DISPLAY_STATUS_TXT,
    D.ORDER_NM = S.ORDER_NM,
    D.ORDER_EXPECTED_PICKUP_DTTM = S.ORDER_EXPECTED_PICKUP_DTTM,
    D.ORDER_FULFILLMENT_DTTM = S.ORDER_FULFILLMENT_DTTM,
    D.POS_ORDER_ID = S.POS_ORDER_ID,
    D.POS_STATUS_CD = S.POS_STATUS_CD,
    D.POS_RESULT_CD = S.POS_RESULT_CD,
    D.POS_MESSAGE_TXT = S.POS_MESSAGE_TXT,
    D.ORDER_SUB_TOTAL_AMT = S.ORDER_SUB_TOTAL_AMT,
    D.ORDER_TOTAL_TAX_AMT = S.ORDER_TOTAL_TAX_AMT,
    D.ORDER_TIP_AMT = S.ORDER_TIP_AMT,
    D.ORDER_DISCOUNT_AMT = S.ORDER_DISCOUNT_AMT,
    D.ORDER_DISCOUNT_PRICE_AMT = S.ORDER_DISCOUNT_PRICE_AMT,
    D.ORDER_TOTAL_AMT = S.ORDER_TOTAL_AMT,
    D.ORDER_DETAIL_TXT = S.ORDER_DETAIL_TXT,
    D.DELIVERY_FEE_AMOUUNT = S.DELIVERY_FEE_AMOUUNT,
    D.SERVER_EMPLOYEE_ID = S.SERVER_EMPLOYEE_ID,
    D.TABLE_NBR = S.TABLE_NBR,
    D.GUEST_CNT = S.GUEST_CNT,
    D.CHECK_NBR = S.CHECK_NBR,
    D.DELIVERY_DTTM = S.DELIVERY_DTTM,
    D.PAYMENT_URL = S.PAYMENT_URL,
    D.TALLY_TM = S.TALLY_TM,
    D.IDP_CUST_ID = S.IDP_CUST_ID,
    D.CUST_MEMBERSHIP_NBR = S.CUST_MEMBERSHIP_NBR,
    D.CUST_FIRST_NM = S.CUST_FIRST_NM,
    D.CUST_LAST_NM = S.CUST_LAST_NM,
    D.CUST_PHONE_NBR = S.CUST_PHONE_NBR,
    D.CUST_EMAIL = S.CUST_EMAIL,
    D.CUST_ADR_LINE_1_TXT = S.CUST_ADR_LINE_1_TXT,
    D.CUST_ADR_LINE_2_TXT = S.CUST_ADR_LINE_2_TXT,
    D.CUST_CTY_NM = S.CUST_CTY_NM,
    D.CUST_ST_CD = S.CUST_ST_CD,
    D.CUST_ZIP_CD = S.CUST_ZIP_CD,
    D.LOC_NM = S.LOC_NM,
    D.LOC_PHONE_NBR = S.LOC_PHONE_NBR,
    D.LOC_EMAIL = S.LOC_EMAIL,
    D.LOC_ADR_LINE_1_TXT = S.LOC_ADR_LINE_1_TXT,
    D.LOC_ADR_LINE_2_TXT = S.LOC_ADR_LINE_2_TXT,
    D.LOC_CTY_NM = S.LOC_CTY_NM,
    D.LOC_ST_CD = S.LOC_ST_CD,
    D.LOC_ZIP_CD = S.LOC_ZIP_CD,
    D.LOC_FRAUD_CHECK_IND = S.LOC_FRAUD_CHECK_IND,
    D.ORDER_CORRELATION_ID = S.ORDER_CORRELATION_ID,
    D.FULLFILLMENT_UNIQUE_IDENTIFER = S.FULLFILLMENT_UNIQUE_IDENTIFER,
    D.FULFILLMENT_ID = S.FULFILLMENT_ID,
    D.FULFILLMENT_TYP_CD = S.FULFILLMENT_TYP_CD,
    D.FULFILLMENT_STATUS_URL = S.FULFILLMENT_STATUS_URL,
    D.DELIVERY_CONTACT_FIRST_NM = S.DELIVERY_CONTACT_FIRST_NM,
    D.DELIVERY_CONTACT_LAST_NM = S.DELIVERY_CONTACT_LAST_NM,
    D.DELIVERY_CONTACT_PHONE_NBR = S.DELIVERY_CONTACT_PHONE_NBR,
    D.DELIVERY_CONTACT_EMAIL = S.DELIVERY_CONTACT_EMAIL,
    D.DELIVERY_ADR_LINE_1_TXT = S.DELIVERY_ADR_LINE_1_TXT,
    D.DELIVERY_ADR_LINE_2_TXT = S.DELIVERY_ADR_LINE_2_TXT,
    D.DELIVERY_CTY_NM = S.DELIVERY_CTY_NM,
    D.DELIVERY_ST_CD = S.DELIVERY_ST_CD,
    D.DELIVERY_ZIP_CD = S.DELIVERY_ZIP_CD,
    D.DELIVERY_PARTNER_NM = S.DELIVERY_PARTNER_NM,
    D.DELIVERY_VEHICLE_DESC = S.DELIVERY_VEHICLE_DESC,
    D.DELIVERY_STATUS_TXT = S.DELIVERY_STATUS_TXT,
    D.CUST_FULFILLMENT_INSTRUCTION_TXT = S.CUST_FULFILLMENT_INSTRUCTION_TXT,
    D.ORDER_METADATA = S.ORDER_METADATA,
    D.CUST_PICKUP_INSTRUCTION_TXT = S.CUST_PICKUP_INSTRUCTION_TXT,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    SOURCE_SYSTEM_NM,
    BUSINESS_DTTM,
    ORDER_CHANNEL_ID,
    ORDER_CHANNEL_CD,
    FULFILLMENT_CHANNEL_CD,
    FULFILLMENT_CHANNEL_SUB_CD,
    ORDER_IDEMPOTENT_ID,
    ORDER_STATUS_CD,
    ORDER_DISPLAY_STATUS_TXT,
    ORDER_NM,
    ORDER_EXPECTED_PICKUP_DTTM,
    ORDER_FULFILLMENT_DTTM,
    POS_ORDER_ID,
    POS_STATUS_CD,
    POS_RESULT_CD,
    POS_MESSAGE_TXT,
    ORDER_SUB_TOTAL_AMT,
    ORDER_TOTAL_TAX_AMT,
    ORDER_TIP_AMT,
    ORDER_DISCOUNT_AMT,
    ORDER_DISCOUNT_PRICE_AMT,
    ORDER_TOTAL_AMT,
    ORDER_DETAIL_TXT,
    DELIVERY_FEE_AMOUUNT,
    SERVER_EMPLOYEE_ID,
    TABLE_NBR,
    GUEST_CNT,
    CHECK_NBR,
    DELIVERY_DTTM,
    PAYMENT_URL,
    TALLY_TM,
    IDP_CUST_ID,
    CUST_MEMBERSHIP_NBR,
    CUST_FIRST_NM,
    CUST_LAST_NM,
    CUST_PHONE_NBR,
    CUST_EMAIL,
    CUST_ADR_LINE_1_TXT,
    CUST_ADR_LINE_2_TXT,
    CUST_CTY_NM,
    CUST_ST_CD,
    CUST_ZIP_CD,
    LOC_NM,
    LOC_PHONE_NBR,
    LOC_EMAIL,
    LOC_ADR_LINE_1_TXT,
    LOC_ADR_LINE_2_TXT,
    LOC_CTY_NM,
    LOC_ST_CD,
    LOC_ZIP_CD,
    LOC_FRAUD_CHECK_IND,
    ORDER_CORRELATION_ID,
    FULLFILLMENT_UNIQUE_IDENTIFER,
    FULFILLMENT_ID,
    FULFILLMENT_TYP_CD,
    FULFILLMENT_STATUS_URL,
    DELIVERY_CONTACT_FIRST_NM,
    DELIVERY_CONTACT_LAST_NM,
    DELIVERY_CONTACT_PHONE_NBR,
    DELIVERY_CONTACT_EMAIL,
    DELIVERY_ADR_LINE_1_TXT,
    DELIVERY_ADR_LINE_2_TXT,
    DELIVERY_CTY_NM,
    DELIVERY_ST_CD,
    DELIVERY_ZIP_CD,
    DELIVERY_PARTNER_NM,
    DELIVERY_VEHICLE_DESC,
    DELIVERY_STATUS_TXT,
    CUST_FULFILLMENT_INSTRUCTION_TXT,
    ORDER_METADATA,
    CUST_PICKUP_INSTRUCTION_TXT,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.BRAND_ID,
    S.BUSINESS_DT,
    S.DIGITAL_ORDER_ID,
    S.LOC_ID,
    S.SOURCE_SYSTEM_NM,
    S.BUSINESS_DTTM,
    S.ORDER_CHANNEL_ID,
    S.ORDER_CHANNEL_CD,
    S.FULFILLMENT_CHANNEL_CD,
    S.FULFILLMENT_CHANNEL_SUB_CD,
    S.ORDER_IDEMPOTENT_ID,
    S.ORDER_STATUS_CD,
    S.ORDER_DISPLAY_STATUS_TXT,
    S.ORDER_NM,
    S.ORDER_EXPECTED_PICKUP_DTTM,
    S.ORDER_FULFILLMENT_DTTM,
    S.POS_ORDER_ID,
    S.POS_STATUS_CD,
    S.POS_RESULT_CD,
    S.POS_MESSAGE_TXT,
    S.ORDER_SUB_TOTAL_AMT,
    S.ORDER_TOTAL_TAX_AMT,
    S.ORDER_TIP_AMT,
    S.ORDER_DISCOUNT_AMT,
    S.ORDER_DISCOUNT_PRICE_AMT,
    S.ORDER_TOTAL_AMT,
    S.ORDER_DETAIL_TXT,
    S.DELIVERY_FEE_AMOUUNT,
    S.SERVER_EMPLOYEE_ID,
    S.TABLE_NBR,
    S.GUEST_CNT,
    S.CHECK_NBR,
    S.DELIVERY_DTTM,
    S.PAYMENT_URL,
    S.TALLY_TM,
    S.IDP_CUST_ID,
    S.CUST_MEMBERSHIP_NBR,
    S.CUST_FIRST_NM,
    S.CUST_LAST_NM,
    S.CUST_PHONE_NBR,
    S.CUST_EMAIL,
    S.CUST_ADR_LINE_1_TXT,
    S.CUST_ADR_LINE_2_TXT,
    S.CUST_CTY_NM,
    S.CUST_ST_CD,
    S.CUST_ZIP_CD,
    S.LOC_NM,
    S.LOC_PHONE_NBR,
    S.LOC_EMAIL,
    S.LOC_ADR_LINE_1_TXT,
    S.LOC_ADR_LINE_2_TXT,
    S.LOC_CTY_NM,
    S.LOC_ST_CD,
    S.LOC_ZIP_CD,
    S.LOC_FRAUD_CHECK_IND,
    S.ORDER_CORRELATION_ID,
    S.FULLFILLMENT_UNIQUE_IDENTIFER,
    S.FULFILLMENT_ID,
    S.FULFILLMENT_TYP_CD,
    S.FULFILLMENT_STATUS_URL,
    S.DELIVERY_CONTACT_FIRST_NM,
    S.DELIVERY_CONTACT_LAST_NM,
    S.DELIVERY_CONTACT_PHONE_NBR,
    S.DELIVERY_CONTACT_EMAIL,
    S.DELIVERY_ADR_LINE_1_TXT,
    S.DELIVERY_ADR_LINE_2_TXT,
    S.DELIVERY_CTY_NM,
    S.DELIVERY_ST_CD,
    S.DELIVERY_ZIP_CD,
    S.DELIVERY_PARTNER_NM,
    S.DELIVERY_VEHICLE_DESC,
    S.DELIVERY_STATUS_TXT,
    S.CUST_FULFILLMENT_INSTRUCTION_TXT,
    S.ORDER_METADATA,
    S.CUST_PICKUP_INSTRUCTION_TXT,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_DISCOUNT_APPLIED_ITEM_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_discount_applied_item_stream')
	as MERGE INTO digital_order_discount_applied_item d USING (
WITH newest_discount_items AS (
    SELECT
    IFF(upper(brandid) = 'ARB', 'arbys',brandid)  AS BRAND_ID,
    to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
    id AS DIGITAL_ORDER_ID,
    location_locationId AS LOC_ID,
    discounts_id AS DISCOUNT_ID,
    discounts_appliedItems_lineItemId AS LINE_ITEM_NBR,
    'IDP' AS SOURCE_SYSTEM_NM,
    discounts_appliedItems_amount AS ITEM_DISCOUNT_AMT,
    discounts_appliedItems_menuItemId AS MENU_ITEM_ID,
    discounts_appliedItems_quantity AS DISCOUNT_ITEM_QTY,
    load_id AS LOAD_ID,
    load_dttm AS LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_discount_applied_item_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND DISCOUNT_ID IS NOT NULL 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,DISCOUNT_ID,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
SELECT
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    DISCOUNT_ID,
    LINE_ITEM_NBR,
    SOURCE_SYSTEM_NM,
    ITEM_DISCOUNT_AMT,
    MENU_ITEM_ID,
    DISCOUNT_ITEM_QTY,
    LOAD_ID,
    LOAD_DTTM
FROM newest_discount_items
) s
ON s.BRAND_ID = d.BRAND_ID
  AND s.LOC_ID = d.LOC_ID
  AND s.BUSINESS_DT = d.BUSINESS_DT
  AND s.DIGITAL_ORDER_ID = d.DIGITAL_ORDER_ID
  AND s.DISCOUNT_ID = d.DISCOUNT_ID
  AND s.SOURCE_SYSTEM_NM = d.SOURCE_SYSTEM_NM
WHEN matched THEN UPDATE
SET
d.LINE_ITEM_NBR = s.LINE_ITEM_NBR,
d.ITEM_DISCOUNT_AMT = s.ITEM_DISCOUNT_AMT,
d.MENU_ITEM_ID = s.MENU_ITEM_ID,
d.DISCOUNT_ITEM_QTY = s.DISCOUNT_ITEM_QTY,
d.UPDATE_ID = s.LOAD_ID,
d.UPDATE_DTTM = s.LOAD_DTTM
WHEN NOT matched THEN
INSERT
(
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    DISCOUNT_ID,
    LINE_ITEM_NBR,
    SOURCE_SYSTEM_NM,
    ITEM_DISCOUNT_AMT,
    MENU_ITEM_ID,
    DISCOUNT_ITEM_QTY,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM
)
VALUES
(
    s.BRAND_ID,
    s.BUSINESS_DT,
    s.DIGITAL_ORDER_ID,
    s.LOC_ID,
    s.DISCOUNT_ID,
    s.LINE_ITEM_NBR,
    s.SOURCE_SYSTEM_NM,
    s.ITEM_DISCOUNT_AMT,
    s.MENU_ITEM_ID,
    s.DISCOUNT_ITEM_QTY,
    s.LOAD_ID,
    s.LOAD_DTTM,
    s.LOAD_ID,
    s.LOAD_DTTM
);
create or replace task IDP_ORDER_DISCOUNT_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_discount_stream')
	as MERGE INTO digital_order_discount d USING (
WITH newest_discounts AS (
    SELECT
    IFF(upper(brandid) = 'ARB', 'arbys',brandid)  AS BRAND_ID,
    to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
    id AS DIGITAL_ORDER_ID,
    location_locationid AS LOC_ID,
    discounts_omsOfferCode AS OMS_OFFER_CD,
    discounts_certificateId AS DISCOUNT_CERTIFICATE_ID,
    discounts_code AS DISCOUNT_CD,
    discounts_compCode AS COMPLIMENTARY_CD,
    discounts_sku AS DISCOUNT_SKU,
    discounts_isCompApplied AS COMPLIMENTARY_APPLIED_IND,
    discounts_name AS DISCOUNT_NM,
    discounts_id AS DISCOUNT_ID,
    discounts_loyaltyId AS LOYALTY_ID,
    'IDP' AS SOURCE_SYSTEM_NM,
    load_id AS LOAD_ID,
    load_dttm AS LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_discount_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND DISCOUNT_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,DISCOUNT_ID,SOURCE_SYSTEM_NM ORDER BY  header_epoch_time desc) = 1
)
    SELECT
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    OMS_OFFER_CD,
    DISCOUNT_CERTIFICATE_ID,
    DISCOUNT_CD,
    COMPLIMENTARY_CD,
    DISCOUNT_SKU,
    COMPLIMENTARY_APPLIED_IND,
    DISCOUNT_NM,
    DISCOUNT_ID,
    LOYALTY_ID,
    SOURCE_SYSTEM_NM,
    LOAD_ID,
    LOAD_DTTM
    FROM newest_discounts
)s
    ON s.BRAND_ID = d.BRAND_ID
    AND s.LOC_ID = d.LOC_ID
    AND s.BUSINESS_DT = d.BUSINESS_DT
    AND s.DIGITAL_ORDER_ID = d.DIGITAL_ORDER_ID
    AND s.DISCOUNT_ID = d.DISCOUNT_ID
    AND s.SOURCE_SYSTEM_NM = d.SOURCE_SYSTEM_NM
    WHEN matched THEN UPDATE
    SET
    d.OMS_OFFER_CD = s.OMS_OFFER_CD,
    d.DISCOUNT_CERTIFICATE_ID = s.DISCOUNT_CERTIFICATE_ID,
    d.DISCOUNT_CD = s.DISCOUNT_CD,
    d.COMPLIMENTARY_CD = s.COMPLIMENTARY_CD,
    d.DISCOUNT_SKU = s.DISCOUNT_SKU,
    d.COMPLIMENTARY_APPLIED_IND = s.COMPLIMENTARY_APPLIED_IND,
    d.DISCOUNT_NM = s.DISCOUNT_NM,
    d.LOYALTY_ID = s.LOYALTY_ID,
    d.UPDATE_ID = s.LOAD_ID,
    d.UPDATE_DTTM = s.LOAD_DTTM
    WHEN NOT matched THEN
INSERT
    (
        BRAND_ID,
        BUSINESS_DT,
        DIGITAL_ORDER_ID,
        LOC_ID,
        DISCOUNT_ID,
        SOURCE_SYSTEM_NM,
        OMS_OFFER_CD,
        DISCOUNT_CERTIFICATE_ID,
        DISCOUNT_CD,
        COMPLIMENTARY_CD,
        DISCOUNT_SKU,
        COMPLIMENTARY_APPLIED_IND,
        DISCOUNT_NM,
        LOYALTY_ID,
        LOAD_ID,
        LOAD_DTTM,
        UPDATE_ID,
        UPDATE_DTTM
    )
VALUES
    (
        s.BRAND_ID,
        s.BUSINESS_DT,
        s.DIGITAL_ORDER_ID,
        s.LOC_ID,
        s.DISCOUNT_ID,
        s.SOURCE_SYSTEM_NM,
        s.OMS_OFFER_CD,
        s.DISCOUNT_CERTIFICATE_ID,
        s.DISCOUNT_CD,
        s.COMPLIMENTARY_CD,
        s.DISCOUNT_SKU,
        s.COMPLIMENTARY_APPLIED_IND,
        s.DISCOUNT_NM,
        s.LOYALTY_ID,
        s.LOAD_ID,
        s.LOAD_DTTM,
        s.LOAD_ID,
        s.LOAD_DTTM
    );
create or replace task IDP_ORDER_LINE_CHILD_MODIFIER_GROUP_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_line_child_modifier_group_stream')
	as MERGE INTO DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP D USING (
    WITH newest_child_modifier_groups AS (
    SELECT
    IFF(upper(brandid) = 'ARB', 'arbys',brandid) as BRAND_ID,
    to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
    id as DIGITAL_ORDER_ID,
    location_locationId as LOC_ID,
    items_lineItemId as LINE_ITEM_NBR,
    ITEMS_CHILDITEMS_MODIFIERGROUPS_ID as CHILD_MODIFIER_GROUP_ID,
    'IDP' as SOURCE_SYSTEM_NM,
    ITEMS_CHILDITEMS_MODIFIERGROUPS_POSID as CHILD_MODIFIER_GROUP_POS_ID,
    ITEMS_CHILDITEMS_MODIFIERGROUPS_PRODUCTID as CHILD_MODIFIER_GROUP_PRODUCT_ID,
    ITEMS_CHILDITEMS_MODIFIERGROUPS_DESCRIPTION as CHILD_MODIFIER_GROUP_DESC,
    ITEMS_CHILDITEMS_ID as CHILD_ITEM_ID,
    LOAD_ID,
    LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_line_child_modifier_group_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND CHILD_MODIFIER_GROUP_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,CHILD_MODIFIER_GROUP_ID,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
    SELECT
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    CHILD_MODIFIER_GROUP_ID,
    SOURCE_SYSTEM_NM,
    CHILD_MODIFIER_GROUP_POS_ID,
    CHILD_MODIFIER_GROUP_PRODUCT_ID,
    CHILD_MODIFIER_GROUP_DESC,
    CHILD_ITEM_ID,
    LOAD_ID,
    LOAD_DTTM
    FROM newest_child_modifier_groups
)S
ON
    S.BRAND_ID = D.BRAND_ID AND
    S.BUSINESS_DT = D.BUSINESS_DT AND
    S.DIGITAL_ORDER_ID = D.DIGITAL_ORDER_ID AND
    S.LOC_ID = D.LOC_ID AND
    S.LINE_ITEM_NBR = D.LINE_ITEM_NBR AND
    S.CHILD_MODIFIER_GROUP_ID = D.CHILD_MODIFIER_GROUP_ID AND
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET
    D.CHILD_MODIFIER_GROUP_POS_ID = S.CHILD_MODIFIER_GROUP_POS_ID,
    D.CHILD_MODIFIER_GROUP_PRODUCT_ID = S.CHILD_MODIFIER_GROUP_PRODUCT_ID,
    D.CHILD_MODIFIER_GROUP_DESC = S.CHILD_MODIFIER_GROUP_DESC,
    D.CHILD_ITEM_ID = S.CHILD_ITEM_ID,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    CHILD_MODIFIER_GROUP_ID,
    SOURCE_SYSTEM_NM,
    CHILD_MODIFIER_GROUP_POS_ID,
    CHILD_MODIFIER_GROUP_PRODUCT_ID,
    CHILD_MODIFIER_GROUP_DESC,
    CHILD_ITEM_ID,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.BRAND_ID,
    S.BUSINESS_DT,
    S.DIGITAL_ORDER_ID,
    S.LOC_ID,
    S.LINE_ITEM_NBR,
    S.CHILD_MODIFIER_GROUP_ID,
    S.SOURCE_SYSTEM_NM,
    S.CHILD_MODIFIER_GROUP_POS_ID,
    S.CHILD_MODIFIER_GROUP_PRODUCT_ID,
    S.CHILD_MODIFIER_GROUP_DESC,
    S.CHILD_ITEM_ID,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_LINE_CHILD_MODIFIER_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_line_child_modifier_stream')
	as MERGE INTO DIGITAL_ORDER_LINE_CHILD_MODIFIER D USING (
WITH newest_child_modifiers AS (
    SELECT
        IFF(upper(brandid) = 'ARB', 'arbys',brandid) as BRAND_ID,
        to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
        id as DIGITAL_ORDER_ID,
        location_locationId as LOC_ID,
        ITEMS_CHILDITEMS_LINEITEMID as LINE_ITEM_NBR,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_ID as CHILD_MODIFIER_GROUP_ID,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_ID as CHILD_MODIFIER_ID,
        'IDP' as SOURCE_SYSTEM_NM,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_DESCRIPTION as CHILD_MODIFIER_DESC,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_PRODUCTID as CHILD_MODIFIER_PRODUCT_ID,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_LINEITEMID as CHILD_MODIFIER_LINE_ITEM_NBR,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_PRICE as CHILD_MODIFIER_PRICE_AMT,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_DISCOUNTEDPRICE as CHILD_MODIFIER_DISCOUNTED_PRICE_AMT,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_QUANTITY as CHILD_MODIFIER_QTY,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_DISPLAYNAME as CHILD_MODIFIER_DISPLAY_NM,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_POSID as CHILD_MODIFIER_POS_ID,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_AVAILABILITY_ISAVAILABLE as CHILD_MODIFIER_AVAILABLE_IND,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_AVAILABILITY_POSID as CHILD_MODIFIER_AVAILABLE_POS_ID,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_AVAILABILITY_QUANTITYAVALILABLE as CHILD_MODIFIER_AVAILABLE_QTY,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_MODIFIERACTIONCODE as CHILD_MODIFIER_ACTION_CD,
        ITEMS_CHILDITEMS_MODIFIERGROUPS_MODIFIERS_MODIFIERACTIONDESCRIPTION as CHILD_MODIFIER_ACTION_DESC,
        ITEMS_CHILDITEMS_ID as CHILD_ITEM_ID,
        LOAD_ID,
        LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_line_child_modifier_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND CHILD_MODIFIER_GROUP_ID IS NOT NULL AND CHILD_MODIFIER_ID IS NOT NULL 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,CHILD_MODIFIER_GROUP_ID,CHILD_MODIFIER_ID,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
    SELECT
        BRAND_ID,
        BUSINESS_DT,
        DIGITAL_ORDER_ID,
        LOC_ID,
        LINE_ITEM_NBR,
        CHILD_MODIFIER_GROUP_ID,
        CHILD_MODIFIER_ID,
        SOURCE_SYSTEM_NM,
        CHILD_MODIFIER_DESC,
        CHILD_MODIFIER_PRODUCT_ID,
        CHILD_MODIFIER_LINE_ITEM_NBR,
        CHILD_MODIFIER_PRICE_AMT,
        CHILD_MODIFIER_DISCOUNTED_PRICE_AMT,
        CHILD_MODIFIER_QTY,
        CHILD_MODIFIER_DISPLAY_NM,
        CHILD_MODIFIER_POS_ID,
        CHILD_MODIFIER_AVAILABLE_IND,
        CHILD_MODIFIER_AVAILABLE_POS_ID,
        CHILD_MODIFIER_AVAILABLE_QTY,
        CHILD_MODIFIER_ACTION_CD,
        CHILD_MODIFIER_ACTION_DESC,
        CHILD_ITEM_ID,
        LOAD_ID,
        LOAD_DTTM
    FROM newest_child_modifiers
)S
ON
    S.BRAND_ID = D.BRAND_ID AND
    S.BUSINESS_DT = D.BUSINESS_DT AND
    S.DIGITAL_ORDER_ID = D.DIGITAL_ORDER_ID AND
    S.LOC_ID = D.LOC_ID AND
    S.LINE_ITEM_NBR = D.LINE_ITEM_NBR AND
    S.CHILD_MODIFIER_GROUP_ID = D.CHILD_MODIFIER_GROUP_ID AND
    S.CHILD_MODIFIER_ID = D.CHILD_MODIFIER_ID AND
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET
    D.CHILD_MODIFIER_DESC = S.CHILD_MODIFIER_DESC,
    D.CHILD_MODIFIER_PRODUCT_ID = S.CHILD_MODIFIER_PRODUCT_ID,
    D.CHILD_MODIFIER_LINE_ITEM_NBR = S.CHILD_MODIFIER_LINE_ITEM_NBR,
    D.CHILD_MODIFIER_PRICE_AMT = S.CHILD_MODIFIER_PRICE_AMT,
    D.CHILD_MODIFIER_DISCOUNTED_PRICE_AMT = S.CHILD_MODIFIER_DISCOUNTED_PRICE_AMT,
    D.CHILD_MODIFIER_QTY = S.CHILD_MODIFIER_QTY,
    D.CHILD_MODIFIER_DISPLAY_NM = S.CHILD_MODIFIER_DISPLAY_NM,
    D.CHILD_MODIFIER_POS_ID = S.CHILD_MODIFIER_POS_ID,
    D.CHILD_MODIFIER_AVAILABLE_IND = S.CHILD_MODIFIER_AVAILABLE_IND,
    D.CHILD_MODIFIER_AVAILABLE_POS_ID = S.CHILD_MODIFIER_AVAILABLE_POS_ID,
    D.CHILD_MODIFIER_AVAILABLE_QTY = S.CHILD_MODIFIER_AVAILABLE_QTY,
    D.CHILD_MODIFIER_ACTION_CD = S.CHILD_MODIFIER_ACTION_CD,
    D.CHILD_MODIFIER_ACTION_DESC = S.CHILD_MODIFIER_ACTION_DESC,
    D.CHILD_ITEM_ID = S.CHILD_ITEM_ID,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    CHILD_MODIFIER_GROUP_ID,
    CHILD_MODIFIER_ID,
    SOURCE_SYSTEM_NM,
    CHILD_MODIFIER_DESC,
    CHILD_MODIFIER_PRODUCT_ID,
    CHILD_MODIFIER_LINE_ITEM_NBR,
    CHILD_MODIFIER_PRICE_AMT,
    CHILD_MODIFIER_DISCOUNTED_PRICE_AMT,
    CHILD_MODIFIER_QTY,
    CHILD_MODIFIER_DISPLAY_NM,
    CHILD_MODIFIER_POS_ID,
    CHILD_MODIFIER_AVAILABLE_IND,
    CHILD_MODIFIER_AVAILABLE_POS_ID,
    CHILD_MODIFIER_AVAILABLE_QTY,
    CHILD_MODIFIER_ACTION_CD,
    CHILD_MODIFIER_ACTION_DESC,
    CHILD_ITEM_ID,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.BRAND_ID,
    S.BUSINESS_DT,
    S.DIGITAL_ORDER_ID,
    S.LOC_ID,
    S.LINE_ITEM_NBR,
    S.CHILD_MODIFIER_GROUP_ID,
    S.CHILD_MODIFIER_ID,
    S.SOURCE_SYSTEM_NM,
    S.CHILD_MODIFIER_DESC,
    S.CHILD_MODIFIER_PRODUCT_ID,
    S.CHILD_MODIFIER_LINE_ITEM_NBR,
    S.CHILD_MODIFIER_PRICE_AMT,
    S.CHILD_MODIFIER_DISCOUNTED_PRICE_AMT,
    S.CHILD_MODIFIER_QTY,
    S.CHILD_MODIFIER_DISPLAY_NM,
    S.CHILD_MODIFIER_POS_ID,
    S.CHILD_MODIFIER_AVAILABLE_IND,
    S.CHILD_MODIFIER_AVAILABLE_POS_ID,
    S.CHILD_MODIFIER_AVAILABLE_QTY,
    S.CHILD_MODIFIER_ACTION_CD,
    S.CHILD_MODIFIER_ACTION_DESC,
    S.CHILD_ITEM_ID,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_LINE_CHILD_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_line_child_stream')
	as MERGE INTO DIGITAL_ORDER_LINE_CHILD D USING (
WITH newest_order_lines_child AS (
    SELECT
        IFF(upper(brandid) = 'ARB', 'arbys',brandid)  as BRAND_ID,
        to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
        id as DIGITAL_ORDER_ID,
        location_locationId as LOC_ID,
        ITEMS_CHILDITEMS_LINEITEMID as LINE_ITEM_NBR,
        'IDP' as SOURCE_SYSTEM_NM,
        ITEMS_CHILDITEMS_ID as  CHILD_ITEM_ID,
        ITEMS_CHILDITEMS_DESCRIPTION as CHILD_ITEM_DESC,
        ITEMS_CHILDITEMS_PRICE as CHILD__ITEM_PRICE_AMT,
        ITEMS_CHILDITEMS_PRODUCTID as CHILD_ITEM_PRODUCT_ID,
        ITEMS_CHILDITEMS_DESTINATION as CHILD_ITEM_DESTINATION_CD,
        ITEMS_CHILDITEMS_QUANTITY as CHILD_ITEM_QTY,
        ITEMS_CHILDITEMS_UDPRECOMMENDATIONID as CHILD_SUGGESTED_SELL_RECOMMENDATION_ID,
        ITEMS_CHILDITEMS_PRODUCTKIND as CHILD_ITEM_PRODUCT_CATEGORY_NM,
        ITEMS_CHILDITEMS_POSID as CHILD_ITEM_POS_ID,
        ITEMS_CHILDITEMS_DISCOUNTEDPRICE as CHILD_ITEM_DISCOUNTED_AMT,
        ITEMS_CHILDITEMS_COMPONENTID as CHILD_ITEM_COMPONENT_ID,
        ITEMS_CHILDITEMS_AVAILABILITY_ISAVAILABLE as CHILD_ITEM_AVAILABLE_IND,
        ITEMS_CHILDITEMS_AVAILABILITY_POSID as CHILD_ITEM_AVAILABLE_POS_ID,
        ITEMS_CHILDITEMS_AVAILABILITY_QUANTITYAVALILABLE as CHILD_ITEM_AVAILABLE_QTY,
        ITEMS_CHILDITEMS_NOTE as CHILD_ITEM_NOTE_TXT,
        ITEMS_CHILDITEMS_PARENT_ID as CHILD_PARENT_ITEM_ID,
        LOAD_ID,
        LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_line_child_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND CHILD_ITEM_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
    SELECT
        BRAND_ID,
        BUSINESS_DT,
        DIGITAL_ORDER_ID,
        LOC_ID,
        LINE_ITEM_NBR,
        SOURCE_SYSTEM_NM,
        CHILD_ITEM_ID,
        CHILD_ITEM_DESC,
        CHILD__ITEM_PRICE_AMT,
        CHILD_ITEM_PRODUCT_ID,
        CHILD_ITEM_DESTINATION_CD,
        CHILD_ITEM_QTY,
        CHILD_SUGGESTED_SELL_RECOMMENDATION_ID,
        CHILD_ITEM_PRODUCT_CATEGORY_NM,
        CHILD_ITEM_POS_ID,
        CHILD_ITEM_DISCOUNTED_AMT,
        CHILD_ITEM_COMPONENT_ID,
        CHILD_ITEM_AVAILABLE_IND,
        CHILD_ITEM_AVAILABLE_POS_ID,
        CHILD_ITEM_AVAILABLE_QTY,
        CHILD_ITEM_NOTE_TXT,
        CHILD_PARENT_ITEM_ID,
        LOAD_ID,
        LOAD_DTTM
    FROM newest_order_lines_child
)S
ON
    S.BRAND_ID = D.BRAND_ID AND
    S.BUSINESS_DT = D.BUSINESS_DT AND
    S.DIGITAL_ORDER_ID = D.DIGITAL_ORDER_ID AND
    S.LOC_ID = D.LOC_ID AND
    S.LINE_ITEM_NBR = D.LINE_ITEM_NBR AND
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET
    D.BRAND_ID = S.BRAND_ID,
    D.BUSINESS_DT = S.BUSINESS_DT,
    D.DIGITAL_ORDER_ID = S.DIGITAL_ORDER_ID,
    D.LOC_ID = S.LOC_ID,
    D.LINE_ITEM_NBR = S.LINE_ITEM_NBR,
    D.SOURCE_SYSTEM_NM = S.SOURCE_SYSTEM_NM,
    D.CHILD_ITEM_ID = S.CHILD_ITEM_ID,
    D.CHILD_ITEM_DESC = S.CHILD_ITEM_DESC,
    D.CHILD__ITEM_PRICE_AMT = S.CHILD__ITEM_PRICE_AMT,
    D.CHILD_ITEM_PRODUCT_ID = S.CHILD_ITEM_PRODUCT_ID,
    D.CHILD_ITEM_DESTINATION_CD = S.CHILD_ITEM_DESTINATION_CD,
    D.CHILD_ITEM_QTY = S.CHILD_ITEM_QTY,
    D.CHILD_SUGGESTED_SELL_RECOMMENDATION_ID = S.CHILD_SUGGESTED_SELL_RECOMMENDATION_ID,
    D.CHILD_ITEM_PRODUCT_CATEGORY_NM = S.CHILD_ITEM_PRODUCT_CATEGORY_NM,
    D.CHILD_ITEM_POS_ID = S.CHILD_ITEM_POS_ID,
    D.CHILD_ITEM_DISCOUNTED_AMT = S.CHILD_ITEM_DISCOUNTED_AMT,
    D.CHILD_ITEM_COMPONENT_ID = S.CHILD_ITEM_COMPONENT_ID,
    D.CHILD_ITEM_AVAILABLE_IND = S.CHILD_ITEM_AVAILABLE_IND,
    D.CHILD_ITEM_AVAILABLE_POS_ID = S.CHILD_ITEM_AVAILABLE_POS_ID,
    D.CHILD_ITEM_AVAILABLE_QTY = S.CHILD_ITEM_AVAILABLE_QTY,
    D.CHILD_ITEM_NOTE_TXT = S.CHILD_ITEM_NOTE_TXT,
    D.CHILD_PARENT_ITEM_ID = S.CHILD_PARENT_ITEM_ID,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    SOURCE_SYSTEM_NM,
    CHILD_ITEM_ID,
    CHILD_ITEM_DESC,
    CHILD__ITEM_PRICE_AMT,
    CHILD_ITEM_PRODUCT_ID,
    CHILD_ITEM_DESTINATION_CD,
    CHILD_ITEM_QTY,
    CHILD_SUGGESTED_SELL_RECOMMENDATION_ID,
    CHILD_ITEM_PRODUCT_CATEGORY_NM,
    CHILD_ITEM_POS_ID,
    CHILD_ITEM_DISCOUNTED_AMT,
    CHILD_ITEM_COMPONENT_ID,
    CHILD_ITEM_AVAILABLE_IND,
    CHILD_ITEM_AVAILABLE_POS_ID,
    CHILD_ITEM_AVAILABLE_QTY,
    CHILD_ITEM_NOTE_TXT,
    CHILD_PARENT_ITEM_ID,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.BRAND_ID,
    S.BUSINESS_DT,
    S.DIGITAL_ORDER_ID,
    S.LOC_ID,
    S.LINE_ITEM_NBR,
    S.SOURCE_SYSTEM_NM,
    S.CHILD_ITEM_ID,
    S.CHILD_ITEM_DESC,
    S.CHILD__ITEM_PRICE_AMT,
    S.CHILD_ITEM_PRODUCT_ID,
    S.CHILD_ITEM_DESTINATION_CD,
    S.CHILD_ITEM_QTY,
    S.CHILD_SUGGESTED_SELL_RECOMMENDATION_ID,
    S.CHILD_ITEM_PRODUCT_CATEGORY_NM,
    S.CHILD_ITEM_POS_ID,
    S.CHILD_ITEM_DISCOUNTED_AMT,
    S.CHILD_ITEM_COMPONENT_ID,
    S.CHILD_ITEM_AVAILABLE_IND,
    S.CHILD_ITEM_AVAILABLE_POS_ID,
    S.CHILD_ITEM_AVAILABLE_QTY,
    S.CHILD_ITEM_NOTE_TXT,
    S.CHILD_PARENT_ITEM_ID,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_LINE_MODIFIER_GROUP_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_line_modifier_group_stream')
	as MERGE INTO DIGITAL_ORDER_LINE_MODIFIER_GROUP D USING (
    WITH newest_modifier_groups AS (
    SELECT
    IFF(upper(brandid) = 'ARB', 'arbys',brandid) as BRAND_ID,
    to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
    id as DIGITAL_ORDER_ID,
    location_locationId as LOC_ID,
    items_lineItemId as LINE_ITEM_NBR,
    items_modifiergroups_id as MODIFIER_GROUP_ID,
    'IDP' as SOURCE_SYSTEM_NM,
    items_modifiergroups_posId as MODIFIER_GROUP_POS_ID,
    items_modifiergroups_productId as MODIFIER_GROUP_PRODUCT_ID,
    items_modifiergroups_description as MODIFIER_GROUP_DESC,
    LOAD_ID,
    LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_line_modifier_group_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND MODIFIER_GROUP_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,MODIFIER_GROUP_ID,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
    SELECT
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    MODIFIER_GROUP_ID,
    SOURCE_SYSTEM_NM,
    MODIFIER_GROUP_POS_ID,
    MODIFIER_GROUP_PRODUCT_ID,
    MODIFIER_GROUP_DESC,
    LOAD_ID,
    LOAD_DTTM
    FROM newest_modifier_groups
)S
ON
    S.BRAND_ID = D.BRAND_ID AND
    S.BUSINESS_DT = D.BUSINESS_DT AND
    S.DIGITAL_ORDER_ID = D.DIGITAL_ORDER_ID AND
    S.LOC_ID = D.LOC_ID AND
    S.LINE_ITEM_NBR = D.LINE_ITEM_NBR AND
    S.MODIFIER_GROUP_ID = D.MODIFIER_GROUP_ID AND
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET
    D.MODIFIER_GROUP_POS_ID = S.MODIFIER_GROUP_POS_ID,
    D.MODIFIER_GROUP_PRODUCT_ID = S.MODIFIER_GROUP_PRODUCT_ID,
    D.MODIFIER_GROUP_DESC = S.MODIFIER_GROUP_DESC,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    MODIFIER_GROUP_ID,
    SOURCE_SYSTEM_NM,
    MODIFIER_GROUP_POS_ID,
    MODIFIER_GROUP_PRODUCT_ID,
    MODIFIER_GROUP_DESC,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.BRAND_ID,
    S.BUSINESS_DT,
    S.DIGITAL_ORDER_ID,
    S.LOC_ID,
    S.LINE_ITEM_NBR,
    S.MODIFIER_GROUP_ID,
    S.SOURCE_SYSTEM_NM,
    S.MODIFIER_GROUP_POS_ID,
    S.MODIFIER_GROUP_PRODUCT_ID,
    S.MODIFIER_GROUP_DESC,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_LINE_MODIFIER_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_line_modifier_stream')
	as MERGE INTO DIGITAL_ORDER_LINE_MODIFIER D USING (
WITH newest_modifiers AS (
    SELECT
        IFF(upper(brandid) = 'ARB', 'arbys',brandid) as BRAND_ID,
        to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
        id as DIGITAL_ORDER_ID,
        location_locationId as LOC_ID,
        items_lineItemId as LINE_ITEM_NBR,
        items_modifiergroups_id as MODIFIER_GROUP_ID,
        items_modifiergroups_modifiers_id as MODIFIER_ID,
        'IDP' as SOURCE_SYSTEM_NM, -- which one
        items_modifiergroups_modifiers_description as MODIFIER_DESC,
        items_modifiergroups_modifiers_productId as MODIFIER_PRODUCT_ID,
        items_modifiergroups_modifiers_lineItemId as MODIFIER_LINE_ITEM_NBR,
        items_modifiergroups_modifiers_price as MODIFIER_PRICE_AMT,
        items_modifiergroups_modifiers_discountedPrice as MODIFIER_DISCOUNTED_PRICE_AMT,
        items_modifiergroups_modifiers_quantity as MODIFIER_QTY,
        items_modifiergroups_modifiers_displayName as MODIFIER_DISPLAY_NM,
        items_modifiergroups_modifiers_posId as MODIFIER_POS_ID,
        items_modifiergroups_modifiers_availability_isAvailable as MODIFIER_AVAILABLE_IND,
        items_modifiergroups_modifiers_availability_posId as MODIFIER_AVAILABLE_POS_ID,
        ITEMS_MODIFIERGROUPS_MODIFIERS_AVAILABILITY_QUANTITYAVALILABLE as MODIFIER_AVAILABLE_QTY,
        items_modifiergroups_modifiers_modifierActionCode as MODIFIER_ACTION_CD,
        items_modifiergroups_modifiers_modifierActionDescription as MODIFIER_ACTION_DESC,
        LOAD_ID,
        LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_line_modifier_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND MODIFIER_GROUP_ID IS NOT NULL AND MODIFIER_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,MODIFIER_GROUP_ID,MODIFIER_ID,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
    SELECT
        BRAND_ID,
        BUSINESS_DT,
        DIGITAL_ORDER_ID,
        LOC_ID,
        LINE_ITEM_NBR,
        MODIFIER_GROUP_ID,
        MODIFIER_ID,
        SOURCE_SYSTEM_NM,
        MODIFIER_DESC,
        MODIFIER_PRODUCT_ID,
        MODIFIER_LINE_ITEM_NBR,
        MODIFIER_PRICE_AMT,
        MODIFIER_DISCOUNTED_PRICE_AMT,
        MODIFIER_QTY,
        MODIFIER_DISPLAY_NM,
        MODIFIER_POS_ID,
        MODIFIER_AVAILABLE_IND,
        MODIFIER_AVAILABLE_POS_ID,
        MODIFIER_AVAILABLE_QTY,
        MODIFIER_ACTION_CD,
        MODIFIER_ACTION_DESC,
        LOAD_ID,
        LOAD_DTTM
    FROM newest_modifiers
)S
ON
    S.BRAND_ID = D.BRAND_ID AND
    S.BUSINESS_DT = D.BUSINESS_DT AND
    S.DIGITAL_ORDER_ID = D.DIGITAL_ORDER_ID AND
    S.LOC_ID = D.LOC_ID AND
    S.LINE_ITEM_NBR = D.LINE_ITEM_NBR AND
    S.MODIFIER_GROUP_ID = D.MODIFIER_GROUP_ID AND
    S.MODIFIER_ID = D.MODIFIER_ID AND
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET
    D.MODIFIER_DESC = S.MODIFIER_DESC,
    D.MODIFIER_PRODUCT_ID = S.MODIFIER_PRODUCT_ID,
    D.MODIFIER_LINE_ITEM_NBR = S.MODIFIER_LINE_ITEM_NBR,
    D.MODIFIER_PRICE_AMT = S.MODIFIER_PRICE_AMT,
    D.MODIFIER_DISCOUNTED_PRICE_AMT = S.MODIFIER_DISCOUNTED_PRICE_AMT,
    D.MODIFIER_QTY = S.MODIFIER_QTY,
    D.MODIFIER_DISPLAY_NM = S.MODIFIER_DISPLAY_NM,
    D.MODIFIER_POS_ID = S.MODIFIER_POS_ID,
    D.MODIFIER_AVAILABLE_IND = S.MODIFIER_AVAILABLE_IND,
    D.MODIFIER_AVAILABLE_POS_ID = S.MODIFIER_AVAILABLE_POS_ID,
    D.MODIFIER_AVAILABLE_QTY = S.MODIFIER_AVAILABLE_QTY,
    D.MODIFIER_ACTION_CD = S.MODIFIER_ACTION_CD,
    D.MODIFIER_ACTION_DESC = S.MODIFIER_ACTION_DESC,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    MODIFIER_GROUP_ID,
    MODIFIER_ID,
    SOURCE_SYSTEM_NM,
    MODIFIER_DESC,
    MODIFIER_PRODUCT_ID,
    MODIFIER_LINE_ITEM_NBR,
    MODIFIER_PRICE_AMT,
    MODIFIER_DISCOUNTED_PRICE_AMT,
    MODIFIER_QTY,
    MODIFIER_DISPLAY_NM,
    MODIFIER_POS_ID,
    MODIFIER_AVAILABLE_IND,
    MODIFIER_AVAILABLE_POS_ID,
    MODIFIER_AVAILABLE_QTY,
    MODIFIER_ACTION_CD,
    MODIFIER_ACTION_DESC,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.BRAND_ID,
    S.BUSINESS_DT,
    S.DIGITAL_ORDER_ID,
    S.LOC_ID,
    S.LINE_ITEM_NBR,
    S.MODIFIER_GROUP_ID,
    S.MODIFIER_ID,
    S.SOURCE_SYSTEM_NM,
    S.MODIFIER_DESC,
    S.MODIFIER_PRODUCT_ID,
    S.MODIFIER_LINE_ITEM_NBR,
    S.MODIFIER_PRICE_AMT,
    S.MODIFIER_DISCOUNTED_PRICE_AMT,
    S.MODIFIER_QTY,
    S.MODIFIER_DISPLAY_NM,
    S.MODIFIER_POS_ID,
    S.MODIFIER_AVAILABLE_IND,
    S.MODIFIER_AVAILABLE_POS_ID,
    S.MODIFIER_AVAILABLE_QTY,
    S.MODIFIER_ACTION_CD,
    S.MODIFIER_ACTION_DESC,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_LINE_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_line_stream')
	as MERGE INTO DIGITAL_ORDER_LINE D USING (
WITH newest_order_lines AS (
    SELECT
        IFF(upper(brandid) = 'ARB', 'arbys',brandid) as BRAND_ID,
        to_date(coalesce(orderPlacedDateTime,header_epoch_time)) as BUSINESS_DT,
        id as DIGITAL_ORDER_ID,
        location_locationId as LOC_ID,
        items_lineItemId as LINE_ITEM_NBR,
        'IDP' as SOURCE_SYSTEM_NM,
        items_id as  LINE_ITEM_ID,
        items_description as LINE_ITEM_DESC,
        items_price as LINE_ITEM_PRICE_AMT,
        items_productId as LINE_ITEM_PRODUCT_ID,
        items_destination as LINE_ITEM_DESTINATION_CD,
        items_quantity as LINE_ITEM_QTY,
        items_udpRecommendationId as SUGGESTED_SELL_RECOMMENDATION_ID,
        items_productKind as LINE_ITEM_PRODUCT_CATEGORY_NM,
        items_posId as LINE_ITEM_POS_ID,
        items_discountedPrice as LINE_ITEM_DISCOUNTED_AMT,
        items_componentId as LINE_ITEM_COMPONENT_ID,
        items_availability_isAvailable as LINE_ITEM_AVAILABLE_IND,
        items_availability_posId as LINE_ITEM_AVAILABLE_POS_ID,
        ITEMS_AVAILABILITY_QUANTITYAVALILABLE as LINE_ITEM_AVAILABLE_QTY,
        items_note as LINE_ITEM_NOTE_TXT,
        LOAD_ID,
        LOAD_DTTM
    FROM RDS_DEV.ARB.idp_order_flat_line_stream
    WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND LINE_ITEM_NBR is not NULL --data quality check, to be removed?
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,LINE_ITEM_NBR,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1

)
    SELECT
        BRAND_ID,
        BUSINESS_DT,
        DIGITAL_ORDER_ID,
        LOC_ID,
        LINE_ITEM_NBR,
        SOURCE_SYSTEM_NM,
        LINE_ITEM_ID,
        LINE_ITEM_DESC,
        LINE_ITEM_PRICE_AMT,
        LINE_ITEM_PRODUCT_ID,
        LINE_ITEM_DESTINATION_CD,
        LINE_ITEM_QTY,
        SUGGESTED_SELL_RECOMMENDATION_ID,
        LINE_ITEM_PRODUCT_CATEGORY_NM,
        LINE_ITEM_POS_ID,
        LINE_ITEM_DISCOUNTED_AMT,
        LINE_ITEM_COMPONENT_ID,
        LINE_ITEM_AVAILABLE_IND,
        LINE_ITEM_AVAILABLE_POS_ID,
        LINE_ITEM_AVAILABLE_QTY,
        LINE_ITEM_NOTE_TXT,
        LOAD_ID,
        LOAD_DTTM
    FROM newest_order_lines
)S
ON
    S.BRAND_ID = D.BRAND_ID AND
    S.BUSINESS_DT = D.BUSINESS_DT AND
    S.DIGITAL_ORDER_ID = D.DIGITAL_ORDER_ID AND
    S.LOC_ID = D.LOC_ID AND
    S.LINE_ITEM_NBR = D.LINE_ITEM_NBR AND
    S.SOURCE_SYSTEM_NM = D.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET
    D.LINE_ITEM_ID = S.LINE_ITEM_ID,
    D.LINE_ITEM_DESC = S.LINE_ITEM_DESC,
    D.LINE_ITEM_PRICE_AMT = S.LINE_ITEM_PRICE_AMT,
    D.LINE_ITEM_PRODUCT_ID = S.LINE_ITEM_PRODUCT_ID,
    D.LINE_ITEM_DESTINATION_CD = S.LINE_ITEM_DESTINATION_CD,
    D.LINE_ITEM_QTY = S.LINE_ITEM_QTY,
    D.SUGGESTED_SELL_RECOMMENDATION_ID = S.SUGGESTED_SELL_RECOMMENDATION_ID,
    D.LINE_ITEM_PRODUCT_CATEGORY_NM = S.LINE_ITEM_PRODUCT_CATEGORY_NM,
    D.LINE_ITEM_POS_ID = S.LINE_ITEM_POS_ID,
    D.LINE_ITEM_DISCOUNTED_AMT = S.LINE_ITEM_DISCOUNTED_AMT,
    D.LINE_ITEM_COMPONENT_ID = S.LINE_ITEM_COMPONENT_ID,
    D.LINE_ITEM_AVAILABLE_IND = S.LINE_ITEM_AVAILABLE_IND,
    D.LINE_ITEM_AVAILABLE_POS_ID = S.LINE_ITEM_AVAILABLE_POS_ID,
    D.LINE_ITEM_AVAILABLE_QTY = S.LINE_ITEM_AVAILABLE_QTY,
    D.LINE_ITEM_NOTE_TXT = S.LINE_ITEM_NOTE_TXT,
    D.UPDATE_ID = S.LOAD_ID,
    D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    LINE_ITEM_NBR,
    SOURCE_SYSTEM_NM,
    LINE_ITEM_ID,
    LINE_ITEM_DESC,
    LINE_ITEM_PRICE_AMT,
    LINE_ITEM_PRODUCT_ID,
    LINE_ITEM_DESTINATION_CD,
    LINE_ITEM_QTY,
    SUGGESTED_SELL_RECOMMENDATION_ID,
    LINE_ITEM_PRODUCT_CATEGORY_NM,
    LINE_ITEM_POS_ID,
    LINE_ITEM_DISCOUNTED_AMT,
    LINE_ITEM_COMPONENT_ID,
    LINE_ITEM_AVAILABLE_IND,
    LINE_ITEM_AVAILABLE_POS_ID,
    LINE_ITEM_AVAILABLE_QTY,
    LINE_ITEM_NOTE_TXT,
    LOAD_ID,
    LOAD_DTTM,
    UPDATE_ID,
    UPDATE_DTTM)
VALUES (
    S.BRAND_ID,
    S.BUSINESS_DT,
    S.DIGITAL_ORDER_ID,
    S.LOC_ID,
    S.LINE_ITEM_NBR,
    S.SOURCE_SYSTEM_NM,
    S.LINE_ITEM_ID,
    S.LINE_ITEM_DESC,
    S.LINE_ITEM_PRICE_AMT,
    S.LINE_ITEM_PRODUCT_ID,
    S.LINE_ITEM_DESTINATION_CD,
    S.LINE_ITEM_QTY,
    S.SUGGESTED_SELL_RECOMMENDATION_ID,
    S.LINE_ITEM_PRODUCT_CATEGORY_NM,
    S.LINE_ITEM_POS_ID,
    S.LINE_ITEM_DISCOUNTED_AMT,
    S.LINE_ITEM_COMPONENT_ID,
    S.LINE_ITEM_AVAILABLE_IND,
    S.LINE_ITEM_AVAILABLE_POS_ID,
    S.LINE_ITEM_AVAILABLE_QTY,
    S.LINE_ITEM_NOTE_TXT,
    S.LOAD_ID,
    S.LOAD_DTTM,
    S.LOAD_ID,
    S.LOAD_DTTM);
create or replace task IDP_ORDER_PAYMENT_TASK
	warehouse=IRB_UA_WH
	schedule='1 minute'
	when system$stream_has_data('RDS_DEV.ARB.idp_order_flat_payment_stream')
	as MERGE INTO DIGITAL_ORDER_PAYMENT D USING (
WITH newest_payments AS (
    SELECT
         IFF(upper(brandid) = 'ARB', 'arbys',brandid) AS BRAND_ID,
         to_date(coalesce(orderPlacedDateTime,header_epoch_time)) AS BUSINESS_DT,
         id AS DIGITAL_ORDER_ID,
         location_locationId AS LOC_ID,
         payments_index AS PAYMENT_SEQ_NBR,
         'IDP' AS SOURCE_SYSTEM_NM,
         payments_paymentType AS PAYMENT_TYP_CD,
         payments_cardIssuer AS PAYMENT_CARD_ISSUER_NM,
         payments_cardNumber AS  PAYMENT_CARD_NBR,
         payments_paymentAmount AS PAYMENT_AMT,
         payments_cardHolderName AS PAYMENT_CARD_HOLDER_NM,
         payments_paymentProcessCode AS PAYMENT_PROCESS_CD,
         payments_paymentStatus AS PAYMENT_STATUS_TXT,
         payments_authorizationCode AS PAYMENT_AUTHORIZATION_CD,
         payments_paymentProcessMessage AS PAYMENT_PROCESS_MESSAGE_TXT,
         LOAD_ID,
         LOAD_DTTM
     FROM RDS_DEV.ARB.idp_order_flat_payment_stream
     WHERE METADATA$ACTION = 'INSERT' AND STATUS='POS_SUBMITTED' AND SUBSCRIPTIONNAME!='historical_load'
     QUALIFY ROW_NUMBER() OVER (PARTITION BY BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,PAYMENT_SEQ_NBR,SOURCE_SYSTEM_NM ORDER BY header_epoch_time desc) = 1
)
SELECT
    BRAND_ID,
    BUSINESS_DT,
    DIGITAL_ORDER_ID,
    LOC_ID,
    PAYMENT_SEQ_NBR,
    SOURCE_SYSTEM_NM,
    PAYMENT_TYP_CD,
    PAYMENT_CARD_ISSUER_NM,
    PAYMENT_CARD_NBR,
    PAYMENT_AMT,
    PAYMENT_CARD_HOLDER_NM,
    PAYMENT_PROCESS_CD,
    PAYMENT_STATUS_TXT,
    PAYMENT_AUTHORIZATION_CD,
    PAYMENT_PROCESS_MESSAGE_TXT,
    LOAD_ID,
    LOAD_DTTM
FROM newest_payments
) S
ON
    D.BRAND_ID = S.BRAND_ID AND
    D.BUSINESS_DT = S.BUSINESS_DT AND
    D.DIGITAL_ORDER_ID = S.DIGITAL_ORDER_ID AND
    D.LOC_ID = S.LOC_ID AND
    D.PAYMENT_SEQ_NBR = S.PAYMENT_SEQ_NBR AND
    D.SOURCE_SYSTEM_NM = S.SOURCE_SYSTEM_NM
WHEN MATCHED THEN UPDATE SET D.PAYMENT_TYP_CD = S.PAYMENT_TYP_CD,
                            D.PAYMENT_CARD_ISSUER_NM = S.PAYMENT_CARD_ISSUER_NM,
                            D.PAYMENT_CARD_NBR = S.PAYMENT_CARD_NBR,
                            D.PAYMENT_AMT = S.PAYMENT_AMT,
                            D.PAYMENT_CARD_HOLDER_NM = S.PAYMENT_CARD_HOLDER_NM,
                            D.PAYMENT_PROCESS_CD = S.PAYMENT_PROCESS_CD,
                            D.PAYMENT_STATUS_TXT = S.PAYMENT_STATUS_TXT,
                            D.PAYMENT_AUTHORIZATION_CD = S.PAYMENT_AUTHORIZATION_CD,
                            D.PAYMENT_PROCESS_MESSAGE_TXT = S.PAYMENT_PROCESS_MESSAGE_TXT,
                            D.UPDATE_ID = S.LOAD_ID,
                            D.UPDATE_DTTM = S.LOAD_DTTM
WHEN NOT MATCHED THEN INSERT (BRAND_ID,BUSINESS_DT,DIGITAL_ORDER_ID,LOC_ID,PAYMENT_SEQ_NBR,SOURCE_SYSTEM_NM,PAYMENT_TYP_CD,PAYMENT_CARD_ISSUER_NM,PAYMENT_CARD_NBR,PAYMENT_AMT,PAYMENT_CARD_HOLDER_NM,PAYMENT_PROCESS_CD,PAYMENT_STATUS_TXT,PAYMENT_AUTHORIZATION_CD,PAYMENT_PROCESS_MESSAGE_TXT,LOAD_ID,LOAD_DTTM,UPDATE_ID,UPDATE_DTTM)
                    VALUES(S.BRAND_ID,S.BUSINESS_DT,S.DIGITAL_ORDER_ID,S.LOC_ID,S.PAYMENT_SEQ_NBR,S.SOURCE_SYSTEM_NM,S.PAYMENT_TYP_CD,S.PAYMENT_CARD_ISSUER_NM,S.PAYMENT_CARD_NBR,S.PAYMENT_AMT,S.PAYMENT_CARD_HOLDER_NM,S.PAYMENT_PROCESS_CD,S.PAYMENT_STATUS_TXT,S.PAYMENT_AUTHORIZATION_CD,S.PAYMENT_PROCESS_MESSAGE_TXT,S.LOAD_ID,S.LOAD_DTTM,S.LOAD_ID,S.LOAD_DTTM);