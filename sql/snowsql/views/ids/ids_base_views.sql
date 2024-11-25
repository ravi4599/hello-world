
CREATE OR REPLACE VIEW IDS_{{params.env}}.TXN_BV.FN_MEASURE_BV
(
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	MEASURE_NAME COMMENT 'Measure Name is a label utilized to specify a value. Sample values Tax Exempt Sales, Non-Taxable Sales, Paid Outs, and Over Short.
',
	MEASURE_DESC COMMENT 'Measure Description provides additional information about a measure. For example, Total Absolute Variance is taking the absolute value of a variance and aggregating it with other variances to provide the scope of the variance.',
	CREDIT_DEBIT_CODE COMMENT 'Credit Debit Code specifies if a measure is typically a credit or debit. Sample values, D or C.',
	DEFAULT_SORT_ID COMMENT 'Default Sort Identifier is a reporting sort order for measures when they appear in a report.',
	MEASURE_CATEGORY_NAME COMMENT 'Measure Category Name allows for a higher level grouping of measures. Samle values, Net Sales, Sales Tax,  Deposit, and Credit Cards.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
COMMENT = 'FN Measure contains the measure that can be utilized for reconciliation analysis. For example: Net Sales, Sales Tax, Cash Deposit, and Visa Card.'
AS 
SELECT FN_MEASURE.FN_MEASURE_ID,FN_MEASURE.MEASURE_NAME,FN_MEASURE.MEASURE_DESC,FN_MEASURE.CREDIT_DEBIT_CODE,FN_MEASURE.DEFAULT_SORT_ID,FN_MEASURE.MEASURE_CATEGORY_NAME,FN_MEASURE.SOURCE_SYSTEM_NAME,FN_MEASURE.LOAD_ID,FN_MEASURE.LOAD_DTTM,FN_MEASURE.UPDATE_ID,FN_MEASURE.UPDATE_DTTM
FROM IDS_{{params.env}}.TXN.FN_MEASURE ;

CREATE OR REPLACE VIEW IDS_{{params.env}}.TXN_BV.FN_SYSTEM_BV
(
	FN_SYSTEM_ID COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_SYSTEM_CATEGORY_CODE COMMENT 'FN System Category Code uniquely identifies a grouping for multiple systems in order to be analyzed by category. Sample values are POS, BO, SS and GL.',
	SYSTEM_NAME COMMENT 'System Name is a label for which data is sourced. For example,  PAR Brink Data, Oracle General Ledger, Sales System, and Altametrics. ',
	SYSTEM_DESC COMMENT 'System Description allows for additional information about a system, such as if it is only for a particular data elements.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
COMMENT = 'FN Systems specifies the different systems that are utilized for reconciliation analysis. For example: PAR Brink Data, Oracle General Ledger, Sales System, and Altametrics. '
AS 
SELECT FN_SYSTEM.FN_SYSTEM_ID,FN_SYSTEM.FN_SYSTEM_CATEGORY_CODE,FN_SYSTEM.SYSTEM_NAME,FN_SYSTEM.SYSTEM_DESC,FN_SYSTEM.BRAND_ID,FN_SYSTEM.SOURCE_SYSTEM_NAME,FN_SYSTEM.LOAD_ID,FN_SYSTEM.LOAD_DTTM,FN_SYSTEM.UPDATE_ID,FN_SYSTEM.UPDATE_DTTM
FROM IDS_{{params.env}}.TXN.FN_SYSTEM ;

CREATE OR REPLACE VIEW IDS_{{params.env}}.TXN_BV.FN_SYSTEM_CATEGORY_BV
(
	FN_SYSTEM_CATEGORY_CODE COMMENT 'FN System Category Code uniquely identifies a grouping for multiple systems in order to be analyzed by category. Sample values are POS, BO, SS and GL.',
	FN_SYSTEM_CATEGORY_NAME COMMENT 'FN System Category Name is a label for a grouping for multiple systems in order to be analyzed by category. Sample values are Point of Sale, Back Office, Sales System, and General Ledger.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
COMMENT = 'FN System Category allows for aggregation of data across multiple sources of similar data to a general system category.  For example: POS - Point of Sale, BO - Back Office, SS - Sales System, and GL - General Ledger. '
AS 
SELECT FN_SYSTEM_CATEGORY.FN_SYSTEM_CATEGORY_CODE,FN_SYSTEM_CATEGORY.FN_SYSTEM_CATEGORY_NAME,FN_SYSTEM_CATEGORY.SOURCE_SYSTEM_NAME,FN_SYSTEM_CATEGORY.LOAD_ID,FN_SYSTEM_CATEGORY.LOAD_DTTM,FN_SYSTEM_CATEGORY.UPDATE_ID,FN_SYSTEM_CATEGORY.UPDATE_DTTM
FROM IDS_{{params.env}}.TXN.FN_SYSTEM_CATEGORY ;

CREATE OR REPLACE VIEW IDS_{{params.env}}.TXN_BV.FN_SYSTEM_TO_MEASURE_REFERENCE_BV
(
	FN_SYSTEM_ID COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	SOURCE_SALES_SYSTEM_MEASURE_TEXT COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
COMMENT = 'FN System To Measure Reference is a generic mapping table from source data to measure.'
AS 
SELECT FN_SYSTEM_TO_MEASURE_REFERENCE.FN_SYSTEM_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.SOURCE_SALES_SYSTEM_MEASURE_TEXT,FN_SYSTEM_TO_MEASURE_REFERENCE.BRAND_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.SOURCE_SYSTEM_NAME,FN_SYSTEM_TO_MEASURE_REFERENCE.FN_MEASURE_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.LOAD_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.LOAD_DTTM,FN_SYSTEM_TO_MEASURE_REFERENCE.UPDATE_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.UPDATE_DTTM
FROM IDS_{{params.env}}.TXN.FN_SYSTEM_TO_MEASURE_REFERENCE ;

CREATE OR REPLACE VIEW IDS_{{params.env}}.TXN_BV.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BV
(
	FN_SYSTEM_ID COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	GL_ACCOUNT_CODE COMMENT 'GL Account Code represents an account used in the general ledger.',
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_DEFAULT_MEASURE_IND COMMENT 'GL Account Default Measure Indicator specifies if there are multiple measures for a GL account which one should be utilized if no additional information can be provided in selecting a measure. Valid values are true/false.',
	GL_ACCOUNT_PATTERN_IND COMMENT 'GL Account Pattern Indicator specifes if a pattern for a GL account is specified. For example 7% matches to Paid Outs. Valid values are true/false.',
	ACTIVE_IND COMMENT 'Active Indicator specifies if the mapping is currently in use. Valid value TRUE and FALSE.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
COMMENT = 'FN System GL Account To Measure Reference is utilized to map an account from source date to a measure.'
AS 
SELECT FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.FN_SYSTEM_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.BRAND_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.SOURCE_SYSTEM_NAME,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.GL_ACCOUNT_CODE,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.FN_MEASURE_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.GL_ACCOUNT_DEFAULT_MEASURE_IND,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.GL_ACCOUNT_PATTERN_IND,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.ACTIVE_IND,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.LOAD_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.LOAD_DTTM,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.UPDATE_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.UPDATE_DTTM
FROM IDS_{{params.env}}.TXN.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE ;


CREATE OR REPLACE VIEW IDS_{{params.env}}.TXN_BV.FN_DAILY_REV_MEASURE_BV
(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DATE COMMENT 'Business Date specifies the day of the year the value is valid for.',
	REST_ID COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
COMMENT = 'FN Daily Revenue Measure contains data from different sources that has been related to a common measure at the daily level.'
AS 
SELECT FN_DAILY_REV_MEASURE.BRAND_ID,FN_DAILY_REV_MEASURE.BUSINESS_DATE,FN_DAILY_REV_MEASURE.REST_ID,FN_DAILY_REV_MEASURE.FN_SYSTEM_ID,FN_DAILY_REV_MEASURE.FN_MEASURE_ID,FN_DAILY_REV_MEASURE.GL_ACCOUNT_CODE,FN_DAILY_REV_MEASURE.GL_COST_CTR,FN_DAILY_REV_MEASURE.SALE_USD_AMOUNT,FN_DAILY_REV_MEASURE.SALE_AMOUNT,FN_DAILY_REV_MEASURE.SALE_COUNT,FN_DAILY_REV_MEASURE.COUNTRY_CODE,FN_DAILY_REV_MEASURE.CURRENCY_CODE,FN_DAILY_REV_MEASURE.SOURCE_SYSTEM_NAME,FN_DAILY_REV_MEASURE.LOAD_ID,FN_DAILY_REV_MEASURE.LOAD_DTTM,FN_DAILY_REV_MEASURE.UPDATE_ID,FN_DAILY_REV_MEASURE.UPDATE_DTTM
FROM IDS_{{params.env}}.TXN.FN_DAILY_REV_MEASURE ;

CREATE OR REPLACE VIEW IDS_{{params.env}}.TXN_BV.FN_WEEKLY_REV_MEASURE_BV
(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	FISC_WK_END_DATE COMMENT 'Fiscal Week End Date specifies the day at the end of a fiscal week the value is valid for.',
	REST_ID COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
COMMENT = 'FN Weekly Revenue Measure contains data from different sources that has been related to a common measure at the weekly level.'
AS 
SELECT FN_WEEKLY_REV_MEASURE.BRAND_ID,FN_WEEKLY_REV_MEASURE.FISC_WK_END_DATE,FN_WEEKLY_REV_MEASURE.REST_ID,FN_WEEKLY_REV_MEASURE.FN_SYSTEM_ID,FN_WEEKLY_REV_MEASURE.FN_MEASURE_ID,FN_WEEKLY_REV_MEASURE.GL_ACCOUNT_CODE,FN_WEEKLY_REV_MEASURE.GL_COST_CTR,FN_WEEKLY_REV_MEASURE.SALE_USD_AMOUNT,FN_WEEKLY_REV_MEASURE.SALE_AMOUNT,FN_WEEKLY_REV_MEASURE.SALE_COUNT,FN_WEEKLY_REV_MEASURE.COUNTRY_CODE,FN_WEEKLY_REV_MEASURE.CURRENCY_CODE,FN_WEEKLY_REV_MEASURE.SOURCE_SYSTEM_NAME,FN_WEEKLY_REV_MEASURE.LOAD_ID,FN_WEEKLY_REV_MEASURE.LOAD_DTTM,FN_WEEKLY_REV_MEASURE.UPDATE_ID,FN_WEEKLY_REV_MEASURE.UPDATE_DTTM
FROM IDS_{{params.env}}.TXN.FN_WEEKLY_REV_MEASURE ;

CREATE OR REPLACE VIEW IDS_{{params.env}}.INT_REF_BV.REV_CTR_ITEM_BV
(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	REV_CTR_ITEM_ID COMMENT 'Revenue Center Item Identifier uniquely identifies an occurence of a soure item for a brand.',
	REV_CTR_ID COMMENT 'Revenue Center Identifier unique identifier for a brand revenue center for a given vendor.',
	SOURCE_ITEM_ID COMMENT 'Source Revenue Center Identifier is a unique identifier from a source system.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	SOURCE_ITEM_DESC COMMENT 'Source Item Description is a description about the source item to provide some context to the mapping',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
AS 
SELECT c.BRAND_ID,c.REV_CTR_ITEM_ID,c.REV_CTR_ID,c.SOURCE_ITEM_ID,c.SOURCE_SYSTEM_NAME,c.SOURCE_ITEM_DESC,c.LOAD_ID,c.LOAD_DTTM,c.UPDATE_ID,c.UPDATE_DTTM
FROM IDS_{{params.env}}.INT_REF.REV_CTR_ITEM c;


CREATE OR REPLACE VIEW IDS_{{params.env}}.INT_REF_BV.REV_CTR_BV
(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
    REV_CTR_ID COMMENT 'Revenue Center Identifier unique identifier for a brand revenue center for a given vendor.',
	SOURCE_REV_CTR_ID COMMENT 'Source Revenue Center Identifier is a unique identifier from a source system.',
	REV_CTR_DESC COMMENT 'Revenue Center Description describes the revenue center.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
	)COPY GRANTS 
AS 
SELECT c.BRAND_ID,c.SOURCE_REV_CTR_ID,c.REV_CTR_DESC,c.SOURCE_SYSTEM_NAME,c.LOAD_ID,c.LOAD_DTTM,c.UPDATE_ID,c.UPDATE_DTTM,c.REV_CTR_ID
FROM IDS_{{params.env}}.INT_REF.REV_CTR c;

CREATE OR REPLACE VIEW IDS_{{params.env}}.INT_REF_BV.TRANSFORMATION_CONFIGURATION_BV
(
    DOMAIN_NAME	COMMENT  'Domain Name contains the high level association to which the transformation is associated with, such as Finance, Customer, Location.',
    BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo WildWing), arbys (Arbys), dnkn (Dunkin), etc.',
    TRANSFORMATION_NAME	COMMENT 'Transformation name is a unique label within a domain/brand for an associated configuration for a set of configurations to be retieved. E2E_SALES_RECON_ALTAMETRICS_ADLS_TO_RDS.',
    CONFIGURATION_NAME	COMMENT 'Configuration Name specifies allows for a more specific configuration within a transformation when the same key field is used in multiple places.',
    CONFIGURATION_KEY_LABEL	COMMENT  'Configuration Key Label specifies what the configuartion is. The most common scenarios are key values pairs and this would identify the column that is used for comparison.',
    CONFIGURATION_VALUE	 COMMENT  'Configuration Value is the value that a typical column would be compared to.',
    CONFIGRATION_OPERATOR_TYPE COMMENT  'Configuration Operator Type specifies what type of logic is being applied. For example, if one wants to determine if a restaurant is in a set the the EQUAL operator would be used. Sample values, coud be EQUAL, LIKE, GT (Greater Than), LT (Less Than), NULL (Null Check).',
    CONFIGURATION_START_DATE COMMENT  'Configuration Start Date specifies when the configuration takes effect.',
    OPERATON_RETURN_VALUE COMMENT  'Operator Return Value specifies a return value if the expected conditions are met. For example, TRUE, FALSE, 2000-01-01.',
    OPERATOR_RETURN_TYPE COMMENT  'Operator Return Type specifies the expected return value of the operation. The Operator Return Type can then be used to cast to an approriate data type. For example, BOOLEAN, TEXT,INTEGER.',
    CONFIGURATION_END_DATE COMMENT  'Configuration End Date specifies when the configuration is not longer applicable.',
    SOURCE_SYSTEM_NAME	COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
    LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)COPY GRANTS 
AS 
SELECT c.DOMAIN_NAME, c.BRAND_ID, c.TRANSFORMATION_NAME, c.CONFIGURATION_NAME, c.CONFIGURATION_KEY_LABEL, c.CONFIGURATION_VALUE,
c.CONFIGRATION_OPERATOR_TYPE, c.CONFIGURATION_START_DATE, c.OPERATON_RETURN_VALUE,
c.OPERATOR_RETURN_TYPE,c.CONFIGURATION_END_DATE, c.SOURCE_SYSTEM_NAME, c.LOAD_ID, c.LOAD_DTTM, c.UPDATE_ID, c.UPDATE_DTTM
FROM IDS_{{params.env}}.INT_REF.TRANSFORMATION_CONFIGURATION c;


