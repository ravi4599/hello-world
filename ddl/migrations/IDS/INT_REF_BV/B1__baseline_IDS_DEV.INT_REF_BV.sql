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
create view IF NOT EXISTS BRAND_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BRAND_NM COMMENT 'Brand Name specifies the code which represents an organization.  Sample names are Inspire Recognized Brands, Buffalo Wild Wing, Dunkin, etc.',
	BRAND_DESC COMMENT 'Brand Description contains additional information regarding the brand.',
	FINANCE_BRAND_CD COMMENT 'Current Finance Brand Code is identifier utilized on financial reports. For example, ARB-Arbys, BWW-Buffalo Wild Wings, SDI-Sonic, RTO-Rusty Taco, JJE-Jimmy Johns, DUN-Dunkin Donuts, BRO-Baskin Robbins.',
	COMPARABLE_REPORTING_BRAND_CD COMMENT 'Current Reporting Brand Code is identifier utilized on financial reports. For example, ARB-Arbys, BWW-Buffalo Wild Wings, SDI-Sonic, RTO-Rusty Taco, JJE-Jimmy Johns, DUN-Dunkin Donuts, BRO-Baskin Robbins.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT BRAND.BRAND_ID,BRAND.BRAND_NM,BRAND.BRAND_DESC,BRAND.FINANCE_BRAND_CD,BRAND.COMPARABLE_REPORTING_BRAND_CD,BRAND.SOURCE_SYSTEM_NM,BRAND.LOAD_ID,BRAND.LOAD_DTTM,BRAND.UPDATE_ID,BRAND.UPDATE_DTTM
FROM IDS_DEV.INT_REF.BRAND ;
create view IF NOT EXISTS COMP_CALENDAR_BV(
	DATE_KEY,
	CALENDAR_DT,
	FISCAL_YEAR_START_DT,
	FISCAL_QUARTER_START_DT,
	FISCAL_PERIOD_START_DT,
	FISCAL_WEEK_START_DT,
	FISCAL_1YR_COMP_DT,
	FISCAL_1YR_COMP_YEAR_DT,
	FISCAL_1YR_COMP_QUARTER_DT,
	FISCAL_1YR_COMP_PERIOD_DT,
	FISCAL_1YR_COMP_WEEK_DT,
	CALENDAR_1YR_COMP_DT,
	CALENDAR_1YR_COMP_YEAR_DT,
	CALENDAR_1YR_COMP_QUARTER_DT,
	CALENDAR_1YR_COMP_PERIOD_DT,
	CALENDAR_1YR_COMP_WEEK_DT,
	CALENDAR_2YR_COMP_DT,
	CALENDAR_2YR_COMP_YEAR_DT,
	CALENDAR_2YR_COMP_QUARTER_DT,
	CALENDAR_2YR_COMP_PERIOD_DT,
	CALENDAR_2YR_COMP_WEEK_DT,
	CALENDAR_3YR_COMP_DT,
	CALENDAR_3YR_COMP_YEAR_DT,
	CALENDAR_3YR_COMP_QUARTER_DT,
	CALENDAR_3YR_COMP_PERIOD_DT,
	CALENDAR_3YR_COMP_WEEK_DT,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM COMMENT 'The Date/Datetime the Record was Updated'
) as 
SELECT COMP_CALENDAR.DATE_KEY,COMP_CALENDAR.CALENDAR_DT,COMP_CALENDAR.FISCAL_YEAR_START_DT,COMP_CALENDAR.FISCAL_QUARTER_START_DT,COMP_CALENDAR.FISCAL_PERIOD_START_DT,COMP_CALENDAR.FISCAL_WEEK_START_DT,COMP_CALENDAR.FISCAL_1YR_COMP_DT,COMP_CALENDAR.FISCAL_1YR_COMP_YEAR_DT,COMP_CALENDAR.FISCAL_1YR_COMP_QUARTER_DT,COMP_CALENDAR.FISCAL_1YR_COMP_PERIOD_DT,COMP_CALENDAR.FISCAL_1YR_COMP_WEEK_DT,COMP_CALENDAR.CALENDAR_1YR_COMP_DT,COMP_CALENDAR.CALENDAR_1YR_COMP_YEAR_DT,COMP_CALENDAR.CALENDAR_1YR_COMP_QUARTER_DT,COMP_CALENDAR.CALENDAR_1YR_COMP_PERIOD_DT,COMP_CALENDAR.CALENDAR_1YR_COMP_WEEK_DT,COMP_CALENDAR.CALENDAR_2YR_COMP_DT,COMP_CALENDAR.CALENDAR_2YR_COMP_YEAR_DT,COMP_CALENDAR.CALENDAR_2YR_COMP_QUARTER_DT,COMP_CALENDAR.CALENDAR_2YR_COMP_PERIOD_DT,COMP_CALENDAR.CALENDAR_2YR_COMP_WEEK_DT,COMP_CALENDAR.CALENDAR_3YR_COMP_DT,COMP_CALENDAR.CALENDAR_3YR_COMP_YEAR_DT,COMP_CALENDAR.CALENDAR_3YR_COMP_QUARTER_DT,COMP_CALENDAR.CALENDAR_3YR_COMP_PERIOD_DT,COMP_CALENDAR.CALENDAR_3YR_COMP_WEEK_DT,COMP_CALENDAR.BRAND_ID,COMP_CALENDAR.SOURCE_SYSTEM_NM,COMP_CALENDAR.LOAD_ID,COMP_CALENDAR.LOAD_DTTM,COMP_CALENDAR.UPDATE_ID,COMP_CALENDAR.UPDATE_DTTM
FROM IDS_DEV.INT_REF.COMP_CALENDAR ;
create view IF NOT EXISTS COMP_DATES_BV(
	TIME_INTERVAL,
	BUSINESS_DT,
	CALENDAR_DT,
	CALENDAR_DAY_NM,
	FISCAL_YEAR_START_DT,
	FISCAL_YEAR_END_DT,
	FISCAL_QUARTER_START_DT,
	FISCAL_QUARTER_END_DT,
	FISCAL_PERIOD_START_DT,
	FISCAL_PERIOD_END_DT,
	FISCAL_WEEK_START_DT,
	FISCAL_WEEK_END_DT,
	FISCAL_WEEK_NBR,
	FISCAL_PERIOD_NBR,
	SOURCE_SYSTEM_NM,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as 
SELECT
TIME_INTERVAL, 
BUSINESS_DT ,
CALENDAR_DT ,
CALENDAR_DAY_NM ,
FISCAL_YEAR_START_DT ,
FISCAL_YEAR_END_DT ,
FISCAL_QUARTER_START_DT ,
FISCAL_QUARTER_END_DT ,
FISCAL_PERIOD_START_DT ,
FISCAL_PERIOD_END_DT ,
FISCAL_WEEK_START_DT ,
FISCAL_WEEK_END_DT ,
FISCAL_WEEK_NBR ,
FISCAL_PERIOD_NBR ,
SOURCE_SYSTEM_NM ,
LOAD_ID  ,
LOAD_DTTM  ,
UPDATE_ID ,
	UPDATE_DTTM   
FROM IDS_DEV.INT_REF.COMP_DATES ;
create view IF NOT EXISTS DATE_DIM_BV(
	DATE_KEY,
	CALENDAR_DT COMMENT 'Calendar Date is the actual Day, Month, and Year for a given day.',
	DATE_DISPLAY_NM COMMENT 'Current Day in date format ',
	CALENDAR_YEAR_NBR COMMENT 'Current Year in 4 digit  Number format ',
	CALENDAR_DAY_IN_YEAR_NBR COMMENT 'Day number for the calendar Year',
	CALENDAR_MONTH_NBR COMMENT 'Current Month number',
	CALENDAR_MONTH_NM COMMENT 'Name of the current Month',
	CALENDAR_DAYS_IN_YEAR_QTY COMMENT 'Total Number of days in calendar Year',
	CALENDAR_DAY_NM COMMENT 'Calendar Day Name ',
	CALENDAR_YEAR_START_DT COMMENT 'Start Date for the current Calendar',
	CALENDAR_YEAR_END_DT COMMENT 'End Date for the current Calendar',
	CALENDAR_QUARTER_NBR,
	CALENDAR_QUARTER_NM,
	CALENDAR_QUARTER_START_DT,
	CALENDAR_QUARTER_END_DT,
	CALENDAR_DAYS_IN_QUARTER_QTY COMMENT 'Number of days in Calendar quarter',
	CALENDAR_MONTH_START_DT,
	CALENDAR_MONTH_END_DT,
	CALENDAR_DAYS_IN_MONTH_QTY,
	CALENDAR_WEEK_IN_YEAR_NBR,
	CALENDAR_WEEK_START_DT,
	CALENDAR_WEEK_END_DT,
	WEEKDAY_IND COMMENT 'Weeday Indicator specifies if a calendar day is considered during the week, typically Monday thru Friday, or on the Weekend, Saturday or Sunday. True- Date is weekday,False- Date is a weekend ',
	FISCAL_YEAR_NBR,
	FISCAL_YEAR_START_DT,
	FISCAL_YEAR_END_DT,
	FISCAL_DAYS_IN_QUARTER_QTY,
	FISCAL_DAYS_IN_YEAR_QTY COMMENT 'Fiscal Days in Year Quantity',
	FISCAL_DAY_IN_PERIOD_NBR COMMENT 'Day number in Fiscal period',
	FISCAL_PERIOD_NBR COMMENT 'Fiscal Period Number ',
	FISCAL_QUARTER_NBR,
	FISCAL_QUARTER_START_DT,
	FISCAL_QUARTER_END_DT,
	FISCAL_PERIOD_START_DT,
	FISCAL_PERIOD_END_DT,
	FISCAL_DAYS_IN_PERIOD_QTY,
	FISCAL_WEEKS_IN_PERIOD_QTY,
	FISCAL_WEEKS_IN_YEAR_QTY,
	FISCAL_WEEK_NBR,
	FISCAL_WEEKS_IN_QUARTER_QTY,
	FISCAL_WEEK_START_DT,
	FISCAL_WEEK_END_DT,
	FISCAL_DAY_IN_WEEK_NBR,
	FISCAL_COMPARABLE_DT COMMENT 'Fiscal Comparable Date',
	CALENDAR_COMPARABLE_DT COMMENT 'Prior Year Calendar comp Date( the date is -364) ',
	CALENDAR_COMPARABLE_START_DT,
	CALENDAR_COMPARABLE_END_DT,
	CALENDAR_COMPARABLE_DAYS_IN_YEAR_QTY,
	CALENDAR_COMPARABLE_IN_YEAR_NBR,
	CALENDAR_COMPARABLE_DAYS_IN_PERIOD_QTY,
	CALENDAR_COMPARABLE_QUARTER_NBR,
	CALENDAR_COMPARABLE_QUARTER_START_DT COMMENT 'Comparable Calendar Quarter Start Date is the ',
	CALENDAR_COMPARABLE_QUARTER_END_DT COMMENT 'Comparable Calendar Quarter End Date',
	CALENDAR_COMPARABLE_PERIOD_START_DT,
	CALENDAR_COMPARABLE_PERIOD_END_DT,
	CALENDAR_COMPARABLE_WEEK_START_DT,
	CALENDAR_COMPARABLE_WEEK_END_DT,
	CALENDAR_COMPARABLE_DAY_IN_WEEK_NBR,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM COMMENT 'The Date/Datetime the Record was Updated'
) COMMENT='Date Dimension View is a one-to-one view of the Date_Dim table which provides hierarchies for analyzing data for different dates or date ranges, such as over weeks, months, or individual days. '
 as 
SELECT DATE_DIM.DATE_KEY,DATE_DIM.CALENDAR_DT,DATE_DIM.DATE_DISPLAY_NM,DATE_DIM.CALENDAR_YEAR_NBR,DATE_DIM.CALENDAR_DAY_IN_YEAR_NBR,DATE_DIM.CALENDAR_MONTH_NBR,DATE_DIM.CALENDAR_MONTH_NM,DATE_DIM.CALENDAR_DAYS_IN_YEAR_QTY,DATE_DIM.CALENDAR_DAY_NM,DATE_DIM.CALENDAR_YEAR_START_DT,DATE_DIM.CALENDAR_YEAR_END_DT,DATE_DIM.CALENDAR_QUARTER_NBR,DATE_DIM.CALENDAR_QUARTER_NM,DATE_DIM.CALENDAR_QUARTER_START_DT,DATE_DIM.CALENDAR_QUARTER_END_DT,DATE_DIM.CALENDAR_DAYS_IN_QUARTER_QTY,DATE_DIM.CALENDAR_MONTH_START_DT,DATE_DIM.CALENDAR_MONTH_END_DT,DATE_DIM.CALENDAR_DAYS_IN_MONTH_QTY,DATE_DIM.CALENDAR_WEEK_IN_YEAR_NBR,DATE_DIM.CALENDAR_WEEK_START_DT,DATE_DIM.CALENDAR_WEEK_END_DT,DATE_DIM.WEEKDAY_IND,DATE_DIM.FISCAL_YEAR_NBR,DATE_DIM.FISCAL_YEAR_START_DT,DATE_DIM.FISCAL_YEAR_END_DT,DATE_DIM.FISCAL_DAYS_IN_QUARTER_QTY,DATE_DIM.FISCAL_DAYS_IN_YEAR_QTY,DATE_DIM.FISCAL_DAY_IN_PERIOD_NBR,DATE_DIM.FISCAL_PERIOD_NBR,DATE_DIM.FISCAL_QUARTER_NBR,DATE_DIM.FISCAL_QUARTER_START_DT,DATE_DIM.FISCAL_QUARTER_END_DT,DATE_DIM.FISCAL_PERIOD_START_DT,DATE_DIM.FISCAL_PERIOD_END_DT,DATE_DIM.FISCAL_DAYS_IN_PERIOD_QTY,DATE_DIM.FISCAL_WEEKS_IN_PERIOD_QTY,DATE_DIM.FISCAL_WEEKS_IN_YEAR_QTY,DATE_DIM.FISCAL_WEEK_NBR,DATE_DIM.FISCAL_WEEKS_IN_QUARTER_QTY,DATE_DIM.FISCAL_WEEK_START_DT,DATE_DIM.FISCAL_WEEK_END_DT,DATE_DIM.FISCAL_DAY_IN_WEEK_NBR,DATE_DIM.FISCAL_COMPARABLE_DT,DATE_DIM.CALENDAR_COMPARABLE_DT,DATE_DIM.CALENDAR_COMPARABLE_START_DT,DATE_DIM.CALENDAR_COMPARABLE_END_DT,DATE_DIM.CALENDAR_COMPARABLE_DAYS_IN_YEAR_QTY,DATE_DIM.CALENDAR_COMPARABLE_IN_YEAR_NBR,DATE_DIM.CALENDAR_COMPARABLE_DAYS_IN_PERIOD_QTY,DATE_DIM.CALENDAR_COMPARABLE_QUARTER_NBR,DATE_DIM.CALENDAR_COMPARABLE_QUARTER_START_DT,DATE_DIM.CALENDAR_COMPARABLE_QUARTER_END_DT,DATE_DIM.CALENDAR_COMPARABLE_PERIOD_START_DT,DATE_DIM.CALENDAR_COMPARABLE_PERIOD_END_DT,DATE_DIM.CALENDAR_COMPARABLE_WEEK_START_DT,DATE_DIM.CALENDAR_COMPARABLE_WEEK_END_DT,DATE_DIM.CALENDAR_COMPARABLE_DAY_IN_WEEK_NBR,DATE_DIM.BRAND_ID,DATE_DIM.SOURCE_SYSTEM_NM,DATE_DIM.LOAD_ID,DATE_DIM.LOAD_DTTM,DATE_DIM.UPDATE_ID,DATE_DIM.UPDATE_DTTM
FROM IDS_DEV.INT_REF.DATE_DIM ;
create view IF NOT EXISTS DQ_SOURCE_METADATA_CONFIGURATION_BV(
	SOURCE_METADATA_CONFIGURATION_ID COMMENT 'SOURCE_METADATA_CONFIGURATION_ID Primary Key of the table.',
	DATABASE_NAME COMMENT 'Name of the Database that the source data will land.',
	SCHEMA_NAME COMMENT 'Name of the Schema  that the source data will land.',
	TABLE_NAME COMMENT 'Name of the Table that the source data will land.',
	COLUMN_NAME COMMENT 'Name of the Column that the source data will land.',
	COLUMN_SEQUENCE COMMENT 'Column Sequence specifies the order in which a column is checked within a source.',
	BRAND_ID COMMENT 'Brand of the advertiser the source data is associated with.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='DQ_SOURCE_METADATA_CONFIGURATION contains metadata information of all the tables which will be aligned with the Source Data Dictionary in the confluence. This table is used to validate the sources such as  files by checking whether the source files has all the columns listed in the Source_Metadata_Conf table and also checking the column order. '
 as 
SELECT DQ_SOURCE_METADATA_CONFIGURATION.SOURCE_METADATA_CONFIGURATION_ID,DQ_SOURCE_METADATA_CONFIGURATION.DATABASE_NAME,DQ_SOURCE_METADATA_CONFIGURATION.SCHEMA_NAME,DQ_SOURCE_METADATA_CONFIGURATION.TABLE_NAME,DQ_SOURCE_METADATA_CONFIGURATION.COLUMN_NAME,DQ_SOURCE_METADATA_CONFIGURATION.COLUMN_SEQUENCE,DQ_SOURCE_METADATA_CONFIGURATION.BRAND_ID,DQ_SOURCE_METADATA_CONFIGURATION.SOURCE_SYSTEM_NAME,DQ_SOURCE_METADATA_CONFIGURATION.LOAD_ID,DQ_SOURCE_METADATA_CONFIGURATION.LOAD_DTTM,DQ_SOURCE_METADATA_CONFIGURATION.UPDATE_ID,DQ_SOURCE_METADATA_CONFIGURATION.UPDATE_DTTM
FROM IDS_DEV.INT_REF.DQ_SOURCE_METADATA_CONFIGURATION ;
create view IF NOT EXISTS DQ_VALIDATION_RULE_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	DQ_RULE_ID COMMENT 'DQ Rule Identifier within a brand uniquely identifiers a rule. Sample Values: 1.',
	RULE_TYPE COMMENT 'Rule Type Sample Values: Count Validation.',
	RULE_DESC COMMENT 'Rule Description Sample Values: Validate Record Count from ADLS to RDS.',
	RULE_SEVERITY_TYPE COMMENT 'Rule Severity Type Sample Values: Critical.',
	TARGET_DATA_ZONE_CODE COMMENT 'Target Data Zone Code Sample Values: RDS.',
	TABLE_NAME COMMENT 'Table Name validation rule is applied to.  Sample Values: ACTUAL_SALES_TIME_SLOT.',
	COLUMN_NAME COMMENT 'Column Name for validation rule.  Sample Values: N/A.',
	RULE_SQL_TEXT COMMENT 'Rule SQL Text is the statement being executed for data quality validation.  Sample Values: Select Count(*) from RDS_DEV.ARB_.ACTUAL_SALES_TIME_SLOT WHERE FILENAME LIKE  ''''%RTI%''''.',
	RULE_LEVEL_TYPE COMMENT 'Rule Level Type specifies Table or Column level rule.  Sample Values: Table .',
	RULE_OWNER_TYPE COMMENT 'Rule Owner Type specifies who created the rule.  Sample Values: Business.',
	RULE_ACTIVE_IND COMMENT 'Rule Active Indicator specifies if the data quality rule can be run.',
	RULE_STATUS_TYPE COMMENT 'Rule Status Type Specifies if rule is Actice, In-Active, Not Valid.',
	RULE_STATUS_DESC COMMENT 'Rule Status Description allows for further describing the status if required. For example, Rule was deprecated due to column removal or Column added to other rule validation.',
	SOURCE_SYSTEM_NAME,
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM COMMENT 'The Date/Datetime the Record was Updated'
) COMMENT='Data Quality(DQ) Validation Rule contains the checks on data as data moves through its various stages.'
 as 
SELECT DQ_VALIDATION_RULE.BRAND_ID,DQ_VALIDATION_RULE.DQ_RULE_ID,DQ_VALIDATION_RULE.RULE_TYPE,DQ_VALIDATION_RULE.RULE_DESC,DQ_VALIDATION_RULE.RULE_SEVERITY_TYPE,DQ_VALIDATION_RULE.TARGET_DATA_ZONE_CODE,DQ_VALIDATION_RULE.TABLE_NAME,DQ_VALIDATION_RULE.COLUMN_NAME,DQ_VALIDATION_RULE.RULE_SQL_TEXT,DQ_VALIDATION_RULE.RULE_LEVEL_TYPE,DQ_VALIDATION_RULE.RULE_OWNER_TYPE,DQ_VALIDATION_RULE.RULE_ACTIVE_IND,DQ_VALIDATION_RULE.RULE_STATUS_TYPE,DQ_VALIDATION_RULE.RULE_STATUS_DESC,DQ_VALIDATION_RULE.SOURCE_SYSTEM_NAME,DQ_VALIDATION_RULE.LOAD_ID,DQ_VALIDATION_RULE.LOAD_DTTM,DQ_VALIDATION_RULE.UPDATE_ID,DQ_VALIDATION_RULE.UPDATE_DTTM
FROM IDS_DEV.INT_REF.DQ_VALIDATION_RULE ;
create view IF NOT EXISTS DQ_VALIDATION_RULE_RESULT_BV(
	DQ_RESULT_ID COMMENT 'Rule Result Identifier Sample Values: 1.',
	DQ_RULE_ID COMMENT 'DQ Rule Identifier within a brand uniquely identifiers a rule. Sample Values: 1.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	TABLE_BUSINESS_KEY COMMENT 'Table Business Key is the combination of fields the data qualtity check was performed on. Sample Values: STOREID|DATE|Filename.',
	BUSINESS_KEY_VALUE COMMENT 'Business Key Value contains tha specific business keys used in the data quality check if applicable to the rule.  Sample Values: N/A.',
	RULE_SQL_OUTPUT_TEXT COMMENT 'Rule SQL Output Text output of the rul.  Sample Values: 825.',
	DATA_BUSINESS_DATE COMMENT 'Data Business Date is the business day the data validation rule was runr for.  Sample Values: 20220208.',
	RESULT_STATUS_TYPE COMMENT 'Result Status Type Sample Values: Active.',
	SOURCE_SYSTEM_NAME,
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM COMMENT 'The Date/Datetime the Record was Updated'
) COMMENT='Data Quality(DQ) Validation Rule Result contains the result of a data check and is at the Business Date grain.'
 as 
SELECT DQ_VALIDATION_RULE_RESULT.DQ_RESULT_ID,DQ_VALIDATION_RULE_RESULT.DQ_RULE_ID,DQ_VALIDATION_RULE_RESULT.BRAND_ID,DQ_VALIDATION_RULE_RESULT.TABLE_BUSINESS_KEY,DQ_VALIDATION_RULE_RESULT.BUSINESS_KEY_VALUE,DQ_VALIDATION_RULE_RESULT.RULE_SQL_OUTPUT_TEXT,DQ_VALIDATION_RULE_RESULT.DATA_BUSINESS_DATE,DQ_VALIDATION_RULE_RESULT.RESULT_STATUS_TYPE,DQ_VALIDATION_RULE_RESULT.SOURCE_SYSTEM_NAME,DQ_VALIDATION_RULE_RESULT.LOAD_ID,DQ_VALIDATION_RULE_RESULT.LOAD_DTTM,DQ_VALIDATION_RULE_RESULT.UPDATE_ID,DQ_VALIDATION_RULE_RESULT.UPDATE_DTTM
FROM IDS_DEV.INT_REF.DQ_VALIDATION_RULE_RESULT ;
create view IF NOT EXISTS HOLIDAY_DIM_BV(
	HOLIDAY_KEY,
	DATE_KEY,
	BRAND_ID COMMENT 'campaignInspire recognized brand id. irb when applied to all brands ',
	HOLIDAY_NM COMMENT 'The Name of the Holiday',
	HOLIDAY_DESC COMMENT 'Name of the objective Holiday.',
	SOURCE_SYSTEM_NM COMMENT 'Inspire level project',
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM COMMENT 'The Date/Datetime the Record was Updated'
) COMMENT='Holiday Dimension View is a one-to-one view for the Holiday Dimension table contains a list of holidays that are observed by brand. The holidays that are recognized across all Inspire Recognized Brands will have a brand identifier of irb.'
 as 
SELECT HOLIDAY_DIM.HOLIDAY_KEY,HOLIDAY_DIM.DATE_KEY,HOLIDAY_DIM.BRAND_ID,HOLIDAY_DIM.HOLIDAY_NM,HOLIDAY_DIM.HOLIDAY_DESC,HOLIDAY_DIM.SOURCE_SYSTEM_NM,HOLIDAY_DIM.LOAD_ID,HOLIDAY_DIM.LOAD_DTTM,HOLIDAY_DIM.UPDATE_ID,HOLIDAY_DIM.UPDATE_DTTM
FROM IDS_DEV.INT_REF.HOLIDAY_DIM ;
create view IF NOT EXISTS STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day of when normal business operations occured.',
	STORE_ID COMMENT 'Number that uniquely identifies the Store.',
	ORDER_CHANNEL_NM COMMENT 'Unique channel id to identify order and fulfullmint mode.',
	MDM_PRODUCT_ID COMMENT 'MDM item ID for the item ordered.',
	PRODUCT_NM COMMENT 'Item Name is the label for the product that has been sold.',
	DMA_CD COMMENT 'Designated Marketing Area Code is a 3 digit code sourced from Nielsen.',
	DMA_NM COMMENT 'Designated Marketing Area Name',
	OWNERSHIP_TYP COMMENT 'Company Owned / Franchise',
	PRODUCT_STANDARD_PRICE_AMT COMMENT 'Product Standard Price Amount is the highest price for a product that the product was sold most frequently for the day.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Store Channel Product Daily Price contains the the most commonly sold price of a product per brand/restaurant/day/channel. '
 as 
SELECT STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.BRAND_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.BUSINESS_DT,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.STORE_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.ORDER_CHANNEL_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.MDM_PRODUCT_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.PRODUCT_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.DMA_CD,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.DMA_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.OWNERSHIP_TYP,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.PRODUCT_STANDARD_PRICE_AMT,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.SOURCE_SYSTEM_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.LOAD_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.LOAD_DTTM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.UPDATE_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE.UPDATE_DTTM
FROM IDS_DEV.INT_REF.STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE ;
create view IF NOT EXISTS STORE_COUNT_THRESHOLD_BV(
	BRAND_ID COMMENT 'Brand associated with store\n',
	OWNERSHIP_TYP COMMENT 'Company Owned or Franchished Store\n',
	THRESHOLD_PCT COMMENT 'Percent of stores that reported sales by Brand\n',
	LOAD_TYP COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM COMMENT 'The Date/Datetime the Record was Updated'
) as 
SELECT STORE_COUNT_THRESHOLD.BRAND_ID,STORE_COUNT_THRESHOLD.OWNERSHIP_TYP,STORE_COUNT_THRESHOLD.THRESHOLD_PCT,STORE_COUNT_THRESHOLD.LOAD_TYP,STORE_COUNT_THRESHOLD.SOURCE_SYSTEM_NM,STORE_COUNT_THRESHOLD.LOAD_ID,STORE_COUNT_THRESHOLD.LOAD_DTTM,STORE_COUNT_THRESHOLD.UPDATE_ID,STORE_COUNT_THRESHOLD.UPDATE_DTTM
FROM IDS_DEV.INT_REF.STORE_COUNT_THRESHOLD ;
create view IF NOT EXISTS STORE_COUNT_THRESHOLD_OVERRIDE_BV(
	BUSINESS_DT COMMENT 'Date of sales transaction\n',
	BRAND_ID COMMENT 'Brand associated with store\n',
	OVERRRIDE_EXCLUSION_IND COMMENT 'Indicator to Include Brand although below threshold\n',
	OVERRIDE_INCLUSION_IND COMMENT 'Indicator to not include Brand\n',
	LOAD_TYP COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM COMMENT 'The Date/Datetime the Record was Updated'
) as 
SELECT STORE_COUNT_THRESHOLD_OVERRIDE.BUSINESS_DT,STORE_COUNT_THRESHOLD_OVERRIDE.BRAND_ID,STORE_COUNT_THRESHOLD_OVERRIDE.OVERRRIDE_EXCLUSION_IND,STORE_COUNT_THRESHOLD_OVERRIDE.OVERRIDE_INCLUSION_IND,STORE_COUNT_THRESHOLD_OVERRIDE.LOAD_TYP,STORE_COUNT_THRESHOLD_OVERRIDE.SOURCE_SYSTEM_NM,STORE_COUNT_THRESHOLD_OVERRIDE.LOAD_ID,STORE_COUNT_THRESHOLD_OVERRIDE.LOAD_DTTM,STORE_COUNT_THRESHOLD_OVERRIDE.UPDATE_ID,STORE_COUNT_THRESHOLD_OVERRIDE.UPDATE_DTTM
FROM IDS_DEV.INT_REF.STORE_COUNT_THRESHOLD_OVERRIDE ;
create view IF NOT EXISTS SURVEY_ALTERNATE_LANDING_BV(
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY.',
	TARGET_SURVEY_ID COMMENT 'Target Survey Identifier uniquely identifies an Inspire Survey to map survey results to.',
	ALTERNATE_LANDING_PREFIX_NM COMMENT 'Alternate Landing Prefix Name is the prefix to be added to Survey Response for a different table set to land survey responses in. For example, Customer Satisfaction would result in data landing in Customer Satisfaction Survey Response.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Survey Alternate Landing specifies whether survey response data should land in a specifc set of repsonse tables instead of the general set of response tables.'
 as 
SELECT SURVEY_ALTERNATE_LANDING.SOURCE_SYSTEM_NM,SURVEY_ALTERNATE_LANDING.BRAND_ID,SURVEY_ALTERNATE_LANDING.SOURCE_SURVEY_ID,SURVEY_ALTERNATE_LANDING.TARGET_SURVEY_ID,SURVEY_ALTERNATE_LANDING.ALTERNATE_LANDING_PREFIX_NM,SURVEY_ALTERNATE_LANDING.LOAD_ID,SURVEY_ALTERNATE_LANDING.LOAD_DTTM,SURVEY_ALTERNATE_LANDING.UPDATE_ID,SURVEY_ALTERNATE_LANDING.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_ALTERNATE_LANDING ;
create view IF NOT EXISTS SURVEY_ANSWER_ATTRIBUTE_MAPPING_BV(
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY',
	SOURCE_ATTRIBUTE_QUESTION_ID COMMENT 'Source Attribute Question Identifier specifies an attribute of a message that maybe incorporated as a question in an Inspire Survey, or a question that is to become an attribute of the response table. For example, QID1, QID7_2, or STORE_TYPE.',
	SOURCE_SURVEY_VAL COMMENT 'Source Survey Value is the value coming in from the source survey and typically from a pick list. For example, 1,2,3,4,5, Likely and Unlikely.',
	TARGET_SURVEY_ID COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	TARGET_SURVEY_QUESTION_ID COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	TARGET_SURVEY_VAL COMMENT 'Target Survey Value  is the value that represents an option for a question. Typically this is an internal value not seen by the survey taker. For example, 1, 2, 3. This should tie to the value in Survey Question Choice, but is presented in the mapping if needed for easier reference.',
	TARGET_SURVEY_NM COMMENT 'Target Survey Name is the name of the Inspire Survey and is included in the mapping table for addtional clarity if needed.',
	TARGET_ATTRIBUTE_ID COMMENT 'Target Attribute Identifier specifies an attribute in an Inspire Standardized survey that is being pivoted from a source survey to a response attribute. For example, Overall Satisfaction Score Number or Day Part Name.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_ANSWER_ATTRIBUTE_MAPPING.SOURCE_SYSTEM_NM,SURVEY_ANSWER_ATTRIBUTE_MAPPING.BRAND_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.SOURCE_SURVEY_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.SOURCE_ATTRIBUTE_QUESTION_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.SOURCE_SURVEY_VAL,SURVEY_ANSWER_ATTRIBUTE_MAPPING.TARGET_SURVEY_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.TARGET_SURVEY_QUESTION_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.TARGET_SURVEY_VAL,SURVEY_ANSWER_ATTRIBUTE_MAPPING.TARGET_SURVEY_NM,SURVEY_ANSWER_ATTRIBUTE_MAPPING.TARGET_ATTRIBUTE_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.LOAD_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.LOAD_DTTM,SURVEY_ANSWER_ATTRIBUTE_MAPPING.UPDATE_ID,SURVEY_ANSWER_ATTRIBUTE_MAPPING.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_ANSWER_ATTRIBUTE_MAPPING ;
create view IF NOT EXISTS SURVEY_BV(
	SURVEY_ID COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_NM COMMENT 'Survey Name specifies a particular surver. For example, Customer Satisfaction.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	SURVEY_TYP_ID COMMENT 'Survey Type Identifier uniquely identifies an Inspire survey type.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY.SURVEY_ID,SURVEY.SURVEY_NM,SURVEY.BRAND_ID,SURVEY.SOURCE_SYSTEM_NM,SURVEY.SURVEY_TYP_ID,SURVEY.LOAD_ID,SURVEY.LOAD_DTTM,SURVEY.UPDATE_ID,SURVEY.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY ;
create view IF NOT EXISTS SURVEY_CHANNEL_BV(
	SURVEY_CHANNEL_ID COMMENT 'Survey Channel Identifier uniquely identifies a survey channel.',
	SURVEY_CHANNEL_CD COMMENT 'Survey Channel Code specifies a code on how an individual received a survey. For example, EMAIL, QR, ANON, RECEIPT.',
	SURVEY_CHANNEL_NM COMMENT 'Survey Channel Name is the label on how an individual received a survey. For example, Email, QR Code, Anonymous, Receipt.',
	SURVEY_SUB_CHANNEL_CD COMMENT 'Survey Sub Channel Code specifies a label to further refine how an individual received a survey. For example, EMAIL-INVITE, EMAIL-ECOML, QR, ANON, RECEIPT.',
	SURVEY_SUB_CHANNEL_NM COMMENT 'Survey Sub Channel Code specifies a code to further refine how an individual received a survey. For example,Email Invite, Email Ecommerce, QR Code, or Receipt.',
	SURVEY_DESC COMMENT 'Survey Description describes the surver channel. For example, Survey was submitted to the individual from a visit to a website where an email address was entered.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_CHANNEL.SURVEY_CHANNEL_ID,SURVEY_CHANNEL.SURVEY_CHANNEL_CD,SURVEY_CHANNEL.SURVEY_CHANNEL_NM,SURVEY_CHANNEL.SURVEY_SUB_CHANNEL_CD,SURVEY_CHANNEL.SURVEY_SUB_CHANNEL_NM,SURVEY_CHANNEL.SURVEY_DESC,SURVEY_CHANNEL.SOURCE_SYSTEM_NM,SURVEY_CHANNEL.BRAND_ID,SURVEY_CHANNEL.LOAD_ID,SURVEY_CHANNEL.LOAD_DTTM,SURVEY_CHANNEL.UPDATE_ID,SURVEY_CHANNEL.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_CHANNEL ;
create view IF NOT EXISTS SURVEY_CHANNEL_MAPPING_BV(
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY.',
	SOURCE_SURVEY_CHANNEL_CD COMMENT 'Source Survey Channel Code are the concatenated values from a surver that are utilized to determine a channel identifier. For example, anonymous|Receipt or anonymous|QR Code.',
	TARGET_CHANNEL_ID,
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_CHANNEL_MAPPING.SOURCE_SYSTEM_NM,SURVEY_CHANNEL_MAPPING.BRAND_ID,SURVEY_CHANNEL_MAPPING.SOURCE_SURVEY_ID,SURVEY_CHANNEL_MAPPING.SOURCE_SURVEY_CHANNEL_CD,SURVEY_CHANNEL_MAPPING.TARGET_CHANNEL_ID,SURVEY_CHANNEL_MAPPING.LOAD_ID,SURVEY_CHANNEL_MAPPING.LOAD_DTTM,SURVEY_CHANNEL_MAPPING.UPDATE_ID,SURVEY_CHANNEL_MAPPING.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_CHANNEL_MAPPING ;
create view IF NOT EXISTS SURVEY_ORDER_FULFILLMENT_BV(
	SURVEY_ORDER_FULFILLMENT_ID COMMENT 'Survey Order Channel Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_FULFILLMENT_CD COMMENT 'Survey Order Fulfillment Channel Code is the label for how an order was placed. For example, DELIVER, DI, CO, DRVTHRU, OAPUSHELF.',
	SURVEY_ORDER_FULFILLMENT_NM COMMENT 'Survey Order Fulfillment Channel Name is the label for how an order was fulfilled by a store. For example,Delivery, Dine In, Carry Out, Drive Thru, OA Pick Up Shelf.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_ORDER_FULFILLMENT.SURVEY_ORDER_FULFILLMENT_ID,SURVEY_ORDER_FULFILLMENT.SURVEY_ORDER_FULFILLMENT_CD,SURVEY_ORDER_FULFILLMENT.SURVEY_ORDER_FULFILLMENT_NM,SURVEY_ORDER_FULFILLMENT.BRAND_ID,SURVEY_ORDER_FULFILLMENT.SOURCE_SYSTEM_NM,SURVEY_ORDER_FULFILLMENT.LOAD_ID,SURVEY_ORDER_FULFILLMENT.LOAD_DTTM,SURVEY_ORDER_FULFILLMENT.UPDATE_ID,SURVEY_ORDER_FULFILLMENT.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_ORDER_FULFILLMENT ;
create view IF NOT EXISTS SURVEY_ORDER_PLACEMENT_BV(
	SURVEY_ORDER_PLACEMENT_ID COMMENT 'Survey Order Placement Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_PLACEMENT_CD COMMENT 'Survey Order Channel Name is the label for how an order was placed. For example, CALL, WEB, MOB, and ONSITE.',
	SURVEY_ORDER_PLACEMENT_NM COMMENT 'Survey Order Channel Name is the label for how an order was placed. For example, Call In, Online Via Website, Mobile App, and At Location.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_ORDER_PLACEMENT.SURVEY_ORDER_PLACEMENT_ID,SURVEY_ORDER_PLACEMENT.SURVEY_ORDER_PLACEMENT_CD,SURVEY_ORDER_PLACEMENT.SURVEY_ORDER_PLACEMENT_NM,SURVEY_ORDER_PLACEMENT.BRAND_ID,SURVEY_ORDER_PLACEMENT.SOURCE_SYSTEM_NM,SURVEY_ORDER_PLACEMENT.LOAD_ID,SURVEY_ORDER_PLACEMENT.LOAD_DTTM,SURVEY_ORDER_PLACEMENT.UPDATE_ID,SURVEY_ORDER_PLACEMENT.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_ORDER_PLACEMENT ;
create view IF NOT EXISTS SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY',
	SOURCE_ATTRIBUTE_QUESTION_ID COMMENT 'Source Attribute Question Identifier specifies an attribute of a message that maybe incorporated as a question in an Inspire Survey, or a question that is to become an attribute of the response table. For example, QID1, QID7_2, or STORE_TYPE.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	TARGET_ATTRIBUTE_ID COMMENT 'Target Attribute Identifier specifies an attribute in an Inspire Standardized survey that is being pivoted from a source survey to a response attribute. For example, Overall Satisfaction Score Number or Day Part Name.',
	TARGET_MASK_TXT COMMENT 'Target Mask Text helps in converting back and forth between data types. For example, if incoming data is a date 2021-01-02, then the mask yyyy-mm-dd could be used to convert the value to a date value.',
	TARGET_SURVEY_ID,
	TARGET_SURVEY_QUESTION_ID,
	TRANSFORMATION_TYP_CD COMMENT 'Transformation Type Code specifies the action necessary to transform an attribue or column. Valid values PASS THRU, PIVOT, PIVOT MAP, and MAP.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.BRAND_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.SOURCE_SURVEY_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.SOURCE_ATTRIBUTE_QUESTION_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.SOURCE_SYSTEM_NM,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_ATTRIBUTE_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_MASK_TXT,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_SURVEY_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_SURVEY_QUESTION_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TRANSFORMATION_TYP_CD,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.LOAD_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.LOAD_DTTM,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.UPDATE_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION ;
create view IF NOT EXISTS SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY',
	SOURCE_ATTRIBUTE_QUESTION_ID COMMENT 'Source Attribute Question Identifier specifies an attribute of a message that maybe incorporated as a question in an Inspire Survey, or a question that is to become an attribute of the response table. For example, QID1, QID7_2, or STORE_TYPE.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	TARGET_ATTRIBUTE_ID COMMENT 'Target Attribute Identifier specifies an attribute in an Inspire Standardized survey that is being pivoted from a source survey to a response attribute. For example, Overall Satisfaction Score Number or Day Part Name.',
	TARGET_MASK_TXT COMMENT 'Target Mask Text helps in converting back and forth between data types. For example, if incoming data is a date 2021-01-02, then the mask yyyy-mm-dd could be used to convert the value to a date value.',
	TARGET_SURVEY_ID,
	TARGET_SURVEY_QUESTION_ID,
	TRANSFORMATION_TYP_CD COMMENT 'Transformation Type Code specifies the action necessary to transform an attribue or column. Valid values PASS THRU, PIVOT, PIVOT MAP, and MAP.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.BRAND_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.SOURCE_SURVEY_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.SOURCE_ATTRIBUTE_QUESTION_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.SOURCE_SYSTEM_NM,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_ATTRIBUTE_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_MASK_TXT,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_SURVEY_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TARGET_SURVEY_QUESTION_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.TRANSFORMATION_TYP_CD,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.LOAD_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.LOAD_DTTM,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.UPDATE_ID,SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION ;
create view IF NOT EXISTS SURVEY_QUESTION_BV(
	SURVEY_ID COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_QUESTION_ID COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	SURVEY_QUESTION_TXT COMMENT 'Survey Question Text contains the sentence being asked to the respondant. For example, How clear and easy was the ordering process?\n',
	SURVEY_QUESTION_TYP_CD COMMENT 'Survey Question Type Code specifies if question is Free Form Text, Boolean or Pick List. For example, TEXT, BOOLEAN, LIST, DATE, etc.',
	SURVEY_QUESTION_CATEGORY_CD COMMENT 'Survey Question Category Code is a code to allow grouping of questions. For example, OSAT, Order Channel, Order Fulfillment, Loyalty, Service Quality, and Order Content.',
	RESPONSE_HEADER_ATTRIBUTE_IND COMMENT 'Response Header Attribute Indicator specifies if the question response is stored as an attribute on response header table instead of the answer table.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_QUESTION.SURVEY_ID,SURVEY_QUESTION.SURVEY_QUESTION_ID,SURVEY_QUESTION.SURVEY_QUESTION_TXT,SURVEY_QUESTION.SURVEY_QUESTION_TYP_CD,SURVEY_QUESTION.SURVEY_QUESTION_CATEGORY_CD,SURVEY_QUESTION.RESPONSE_HEADER_ATTRIBUTE_IND,SURVEY_QUESTION.SOURCE_SYSTEM_NM,SURVEY_QUESTION.BRAND_ID,SURVEY_QUESTION.LOAD_ID,SURVEY_QUESTION.LOAD_DTTM,SURVEY_QUESTION.UPDATE_ID,SURVEY_QUESTION.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_QUESTION ;
create view IF NOT EXISTS SURVEY_QUESTION_CHOICE_BV(
	SURVEY_ID COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_QUESTION_ID COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	SURVEY_QUESTION_CHOICE_ID COMMENT 'Survey Question Choice Identifier uniquely an option for a quesiton within a survey.',
	SURVEY_QUESTION_CHOICE_NM COMMENT 'Survey Question Choice Name is the label used for the options present to the survey recipient. For example, Somewhat likely, Very Likely, and Somewhat Unlikely.\n',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_QUESTION_CHOICE.SURVEY_ID,SURVEY_QUESTION_CHOICE.SURVEY_QUESTION_ID,SURVEY_QUESTION_CHOICE.SURVEY_QUESTION_CHOICE_ID,SURVEY_QUESTION_CHOICE.SURVEY_QUESTION_CHOICE_NM,SURVEY_QUESTION_CHOICE.SOURCE_SYSTEM_NM,SURVEY_QUESTION_CHOICE.BRAND_ID,SURVEY_QUESTION_CHOICE.LOAD_ID,SURVEY_QUESTION_CHOICE.LOAD_DTTM,SURVEY_QUESTION_CHOICE.UPDATE_ID,SURVEY_QUESTION_CHOICE.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_QUESTION_CHOICE ;
create view IF NOT EXISTS SURVEY_TYP_BV(
	SURVEY_TYP_ID COMMENT 'Survey Type Identifier uniquely identifies an Inspire survey type.',
	SURVEY_TYP_NM COMMENT 'Survey Type Name specifies the type of standard Inspire Survey. For example, Customer Satisfaction.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT SURVEY_TYP.SURVEY_TYP_ID,SURVEY_TYP.SURVEY_TYP_NM,SURVEY_TYP.BRAND_ID,SURVEY_TYP.SOURCE_SYSTEM_NM,SURVEY_TYP.LOAD_ID,SURVEY_TYP.LOAD_DTTM,SURVEY_TYP.UPDATE_ID,SURVEY_TYP.UPDATE_DTTM
FROM IDS_DEV.INT_REF.SURVEY_TYP ;
create view IF NOT EXISTS ZIP_TO_DMA_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	ZIP_CD COMMENT 'Zip Code a group of five or nine numbers that are added to a postal address to assist the sorting of mail.',
	CITY_NM COMMENT 'City Name is a label for a place where a large number of people live.',
	STATE_CD COMMENT 'State Code is a two-letter alphabetic codes defined in U.S. Federal Information Processing Standard Publication (\"FIPS PUB\") 5-2 to identify U.S. states and certain other associated areas.',
	CNTRY_CD COMMENT 'Country Code is a 3 character alphabetic geographical code used to represent countries and dependent areas',
	DMA_CD COMMENT 'Designated Market Area(DMA) Code is a 3-digit number created by Nielsen�s to delineate the geographic boundaries of 210 distinctive regions to assess TV penetration of audience counts within the U.S. for a viewership year. There are some 4 digit DMA codes that have been created for international franchisees.',
	ZIP_DMA_NM COMMENT 'Zip Designated Market Area(DMA) Name is label based on labels created by Nielsen�s to delineate the geographic boundaries of 210 distinctive regions to assess TV penetration of audience counts within the U.S. for a viewership year. DMA Name may be overridden with another name for reporting purposes. A zip code my reside in multiple DMAs and the label chosen for the zip code/DMA combination typically is for the DMA with the greatest coverage in a zip code.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Zip to DMA Base View contains a mapping of zip codes to DMA codes. For a mapping for a particular brand additional zip codes and or dma codes can be added to the data set in order account for zip codes that are not associated with an official DMA Code.'
 as 
SELECT ZIP_TO_DMA.BRAND_ID,ZIP_TO_DMA.ZIP_CD,ZIP_TO_DMA.CITY_NM,ZIP_TO_DMA.STATE_CD,ZIP_TO_DMA.CNTRY_CD,ZIP_TO_DMA.DMA_CD,ZIP_TO_DMA.ZIP_DMA_NM,ZIP_TO_DMA.SOURCE_SYSTEM_NM,ZIP_TO_DMA.LOAD_ID,ZIP_TO_DMA.LOAD_DTTM,ZIP_TO_DMA.UPDATE_ID,ZIP_TO_DMA.UPDATE_DTTM
FROM IDS_DEV.INT_REF.ZIP_TO_DMA ;