CREATE OR REPLACE TABLE IDS_{{params.env}}.TXN.FN_DAILY_REV_MEASURE
(
	BRAND_ID STRING NOT NULL  COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DATE DATE NOT NULL  COMMENT 'Business Date specifies the day of the year the value is valid for.',
	REST_ID STRING NOT NULL  COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID INTEGER NOT NULL  COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID INTEGER NOT NULL  COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE STRING NOT NULL  COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR STRING NOT NULL  COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT NUMBER(18,2) NULL  COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT NUMBER(22,6) NULL  COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT INTEGER NULL  COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE STRING NULL  COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE STRING NULL  COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKFN_DAILY_REVENUE_MEASURE PRIMARY KEY (BRAND_ID, BUSINESS_DATE, REST_ID, FN_SYSTEM_ID, FN_MEASURE_ID, GL_ACCOUNT_CODE, GL_COST_CTR)
)
COPY GRANTS
COMMENT = 'FN Daily Revenue Measure contains data from different sources that has been related to a common measure at the daily level.';

CREATE OR REPLACE TABLE IDS_{{params.env}}.TXN.FN_MEASURE
(
	FN_MEASURE_ID INTEGER NOT NULL  COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	MEASURE_NAME STRING NOT NULL  COMMENT 'Measure Name is a label utilized to specify a value. Sample values Tax Exempt Sales, Non-Taxable Sales, Paid Outs, and Over Short.
',
	MEASURE_DESC STRING NULL  COMMENT 'Measure Description provides additional information about a measure. For example, Total Absolute Variance is taking the absolute value of a variance and aggregating it with other variances to provide the scope of the variance.',
	CREDIT_DEBIT_CODE STRING NULL  COMMENT 'Credit Debit Code specifies if a measure is typically a credit or debit. Sample values, D or C.',
	DEFAULT_SORT_ID INTEGER NULL  COMMENT 'Default Sort Identifier is a reporting sort order for measures when they appear in a report.',
	MEASURE_CATEGORY_NAME STRING NULL  COMMENT 'Measure Category Name allows for a higher level grouping of measures. Samle values, Net Sales, Sales Tax,  Deposit, and Credit Cards.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKFN_MEASURE PRIMARY KEY (FN_MEASURE_ID)
)
COPY GRANTS
COMMENT = 'FN Measure contains the measure that can be utilized for reconciliation analysis. For example: Net Sales, Sales Tax, Cash Deposit, and Visa Card.';

CREATE OR REPLACE TABLE IDS_{{params.env}}.TXN.FN_SYSTEM_CATEGORY
(
	FN_SYSTEM_CATEGORY_CODE STRING NOT NULL  COMMENT 'FN System Category Code uniquely identifies a grouping for multiple systems in order to be analyzed by category. Sample values are POS, BO, SS and GL.',
	FN_SYSTEM_CATEGORY_NAME STRING NOT NULL  COMMENT 'FN System Category Name is a label for a grouping for multiple systems in order to be analyzed by category. Sample values are Point of Sale, Back Office, Sales System, and General Ledger.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKFN_SYSTEM_CATEGORY PRIMARY KEY (FN_SYSTEM_CATEGORY_CODE)
)
COPY GRANTS
COMMENT = 'FN System Category allows for aggregation of data across multiple sources of similar data to a general system category.  For example: POS - Point of Sale, BO - Back Office, SS - Sales System, and GL - General Ledger. ';

CREATE OR REPLACE TABLE IDS_{{params.env}}.TXN.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE
(
	FN_SYSTEM_ID INTEGER NOT NULL  COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	BRAND_ID STRING NOT NULL  COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME STRING NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	GL_ACCOUNT_CODE STRING NOT NULL  COMMENT 'GL Account Code represents an account used in the general ledger.',
	FN_MEASURE_ID INTEGER NULL  COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_DEFAULT_MEASURE_IND boolean NULL  COMMENT 'GL Account Default Measure Indicator specifies if there are multiple measures for a GL account which one should be utilized if no additional information can be provided in selecting a measure. Valid values are true/false.',
	GL_ACCOUNT_PATTERN_IND boolean NULL  COMMENT 'GL Account Pattern Indicator specifes if a pattern for a GL account is specified. For example 7% matches to Paid Outs. Valid values are true/false.',
	ACTIVE_IND boolean NULL  COMMENT 'Active Indicator specifies if the mapping is currently in use. Valid value TRUE and FALSE.',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKFN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE PRIMARY KEY (FN_SYSTEM_ID, BRAND_ID, SOURCE_SYSTEM_NAME, GL_ACCOUNT_CODE)
)
COPY GRANTS
COMMENT = 'FN System GL Account To Measure Reference is utilized to map an account from source date to a measure.';

CREATE OR REPLACE TABLE IDS_{{params.env}}.TXN.FN_SYSTEM_TO_MEASURE_REFERENCE
(
	FN_SYSTEM_ID INTEGER NOT NULL  COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	SOURCE_SALES_SYSTEM_MEASURE_TEXT STRING NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID STRING NOT NULL  COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME STRING NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	FN_MEASURE_ID INTEGER NULL  COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKFN_SYSTEM_TO_MEASURE_REFERENCE PRIMARY KEY (FN_SYSTEM_ID, SOURCE_SALES_SYSTEM_MEASURE_TEXT, BRAND_ID, SOURCE_SYSTEM_NAME)
)
COPY GRANTS
COMMENT = 'FN System To Measure Reference is a generic mapping table from source data to measure.';

CREATE OR REPLACE TABLE IDS_{{params.env}}.TXN.FN_SYSTEM
(
	FN_SYSTEM_ID INTEGER NOT NULL  COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_SYSTEM_CATEGORY_CODE STRING NULL  COMMENT 'FN System Category Code uniquely identifies a grouping for multiple systems in order to be analyzed by category. Sample values are POS, BO, SS and GL.',
	SYSTEM_NAME STRING NOT NULL  COMMENT 'System Name is a label for which data is sourced. For example,  PAR Brink Data, Oracle General Ledger, Sales System, and Altametrics. ',
	SYSTEM_DESC STRING NOT NULL  COMMENT 'System Description allows for additional information about a system, such as if it is only for a particular data elements.',
	BRAND_ID STRING NULL  COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKFN_SYSTEM PRIMARY KEY (FN_SYSTEM_ID)
)
COPY GRANTS
COMMENT = 'FN Systems specifies the different systems that are utilized for reconciliation analysis. For example: PAR Brink Data, Oracle General Ledger, Sales System, and Altametrics. ';

CREATE OR REPLACE TABLE IDS_{{params.env}}.TXN.FN_WEEKLY_REV_MEASURE
(
	BRAND_ID STRING NOT NULL  COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	FISC_WK_END_DATE DATE NOT NULL  COMMENT 'Fiscal Week End Date specifies the day at the end of a fiscal week the value is valid for.',
	REST_ID STRING NOT NULL  COMMENT 'Restaurant Identifier uniquely identifies a restaurant within a brand.',
	FN_SYSTEM_ID INTEGER NOT NULL  COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	FN_MEASURE_ID INTEGER NOT NULL  COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	GL_ACCOUNT_CODE STRING NOT NULL  COMMENT 'GL Account Code represents an account used in the general ledger.',
	GL_COST_CTR STRING NOT NULL  COMMENT 'GL Cost Center represents a cost center used in the general ledger.',
	SALE_USD_AMOUNT NUMBER(18,2) NULL  COMMENT 'Sale USD Amount is the amount in US Dollars.',
	SALE_AMOUNT NUMBER(22,6) NULL  COMMENT 'Sale Amount is the amount in that was reported and can be in a currency other than USD.',
	SALE_COUNT INTEGER NULL  COMMENT 'Sale Count specifies a counter for the particular measure.',
	COUNTRY_CODE STRING NULL  COMMENT 'Country Code specifies the country code of the transaction. ',
	CURRENCY_CODE STRING NULL  COMMENT 'Currency Code specifies the currency of the Sale Amount.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKFN_WEEKLY_REVENUE_MEASURE PRIMARY KEY (BRAND_ID, FISC_WK_END_DATE, REST_ID, FN_SYSTEM_ID, FN_MEASURE_ID, GL_ACCOUNT_CODE, GL_COST_CTR)
)
COPY GRANTS;


CREATE OR REPLACE TABLE IDS_{{params.env}}.INT_REF.REV_CTR
(
	BRAND_ID STRING NOT NULL  COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	REV_CTR_ID INTEGER
	AUTOINCREMENT  NOT NULL  COMMENT 'Revenue Center Identifier unique identifier for a brand revenue center for a given vendor.',
	SOURCE_REV_CTR_ID STRING NOT NULL  COMMENT 'Source Revenue Center Identifier is a unique identifier from a source system.',
	REV_CTR_DESC STRING NOT NULL  COMMENT 'Revenue Center Description describes the revenue center.',
	SOURCE_SYSTEM_NAME STRING NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKREVENUE_CENTER PRIMARY KEY (BRAND_ID, REV_CTR_ID),
	CONSTRAINT XAK1REVENUE_CENTER UNIQUE (BRAND_ID,SOURCE_REV_CTR_ID,SOURCE_SYSTEM_NAME)
)
COMMENT = 'Revenue Center is a distinct operating unit of a business that is responsible for generating sales and is judged solely on its ability to generate sales. Revenue centers are typically defined in the Revenue Management Systems.';

CREATE OR REPLACE TABLE IDS_{{params.env}}.INT_REF.REV_CTR_ITEM
(
	BRAND_ID STRING NOT NULL  COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	REV_CTR_ITEM_ID INTEGER
	AUTOINCREMENT  NOT NULL  COMMENT 'Revenue Center Item Identifier uniquely identifies an occurence of a soure item for a brand.',
	REV_CTR_ID INTEGER NOT NULL  COMMENT 'Revenue Center Identifier unique identifier for a brand revenue center for a given vendor.',
	SOURCE_ITEM_ID STRING NOT NULL  COMMENT 'Source Revenue Center Identifier is a unique identifier from a source system.',
	SOURCE_SYSTEM_NAME STRING NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	SOURCE_ITEM_DESC STRING NULL  COMMENT 'Source Item Description is a description about the source item to provide some context to the mapping',
	LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CONSTRAINT XPKREVENUE_CENTER_ITEM PRIMARY KEY (BRAND_ID, REV_CTR_ITEM_ID),
	CONSTRAINT XAK1REVENUE_CENTER_ITEM UNIQUE (BRAND_ID,SOURCE_ITEM_ID,SOURCE_SYSTEM_NAME),
	CONSTRAINT R_417 FOREIGN KEY (BRAND_ID, REV_CTR_ID) REFERENCES IDS_{{params.env}}.INT_REF.REV_CTR (BRAND_ID, REV_CTR_ID)
)
COMMENT = 'Revenue Center Item allows for the mapping of Items from a POS system to a Revenue Center.';

CREATE OR REPLACE TABLE IDS_{{params.env}}.INT_REF.TRANSFORMATION_CONFIGURATION
(
    DOMAIN_NAME	                STRING NOT NULL COMMENT  'Domain Name contains the high level association to which the transformation is associated with, such as Finance, Customer, Location.',
    BRAND_ID	                STRING NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo WildWing), arbys (Arbys), dnkn (Dunkin), etc.',
    TRANSFORMATION_NAME	        STRING COMMENT  'Transformation name is a unique label within a domain/brand for an associated configuration for a set of configurations to be retieved. E2E_SALES_RECON_ALTAMETRICS_ADLS_TO_RDS.',
    CONFIGURATION_NAME	        STRING  NOT NULL COMMENT  'Configuration Name specifies allows for a more specific configuration within a transformation when the same key field is used in multiple places.',
    CONFIGURATION_KEY_LABEL	    STRING NOT NULL COMMENT  'Configuration Key Label specifies what the configuartion is. The most common scenarios are key values pairs and this would identify the column that is used for comparison.',
	CONFIGURATION_VALUE	        STRING NOT NULL COMMENT  'Configuration Value is the value that a typical column would be compared to.',
    CONFIGRATION_OPERATOR_TYPE	STRING COMMENT  'Configuration Operator Type specifies what type of logic is being applied. For example, if one wants to determine if a restaurant is in a set the the EQUAL operator would be used. Sample values, coud be EQUAL, LIKE, GT (Greater Than), LT (Less Than), NULL (Null Check).',
    CONFIGURATION_START_DATE	DATE   COMMENT  'Configuration Start Date specifies when the configuration takes effect.',
    OPERATON_RETURN_VALUE	    STRING COMMENT  'Operator Return Value specifies a return value if the expected conditions are met. For example, TRUE, FALSE, 2000-01-01.',
    OPERATOR_RETURN_TYPE	    STRING NOT NULL COMMENT  'Operator Return Type specifies the expected return value of the operation. The Operator Return Type can then be used to cast to an approriate data type. For example, BOOLEAN, TEXT,INTEGER.',
    CONFIGURATION_END_DATE	    DATE   COMMENT  'Configuration End Date specifies when the configuration is not longer applicable.',
    SOURCE_SYSTEM_NAME	        STRING NOT NULL  COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
    LOAD_ID INTEGER NULL  COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID INTEGER NULL  COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ NOT NULL  COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
)
COMMENT ='Transformation configuration is generic table - objective of this table is to remove hardcoded values from code and put these values in this table. Add revenue centers which are identified for Other Sales.'; 


