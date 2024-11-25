create TABLE IF NOT EXISTS BRAND (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BRAND_NM VARCHAR(16777216) NOT NULL COMMENT 'Brand Name specifies the code which represents an organization.  Sample names are Inspire Recognized Brands, Buffalo Wild Wing, Dunkin, etc.',
	BRAND_DESC VARCHAR(16777216) COMMENT 'Brand Description contains additional information regarding the brand.',
	FINANCE_BRAND_CD VARCHAR(16777216) COMMENT 'Current Finance Brand Code is identifier utilized on financial reports. For example, ARB-Arbys, BWW-Buffalo Wild Wings, SDI-Sonic, RTO-Rusty Taco, JJE-Jimmy Johns, DUN-Dunkin Donuts, BRO-Baskin Robbins.',
	COMPARABLE_REPORTING_BRAND_CD VARCHAR(18) COMMENT 'Current Reporting Brand Code is identifier utilized on financial reports. For example, ARB-Arbys, BWW-Buffalo Wild Wings, SDI-Sonic, RTO-Rusty Taco, JJE-Jimmy Johns, DUN-Dunkin Donuts, BRO-Baskin Robbins.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKBRAND primary key (BRAND_ID)
)COMMENT='Brand table contains the grouping of restaraunts.  For data that is cross brand irb-Inspire Recognized Brands is utilized.'
;
create TABLE IF NOT EXISTS CLOSED_HOUR (
	CLOSED_HR TIME(9)
);
create TABLE IF NOT EXISTS COMP_CALENDAR (
	DATE_KEY NUMBER(38,0) NOT NULL,
	CALENDAR_DT DATE,
	CALENDAR_DAY_NM VARCHAR(16777216),
	FISCAL_YEAR_START_DT DATE,
	FISCAL_QUARTER_START_DT DATE,
	FISCAL_PERIOD_START_DT DATE,
	FISCAL_WEEK_START_DT DATE,
	FISCAL_WEEK_END_DT DATE,
	FISCAL_YEAR_NBR NUMBER(38,0),
	FISCAL_QUARTER_NBR NUMBER(38,0),
	FISCAL_PERIOD_NBR NUMBER(38,0),
	FISCAL_WEEK_NBR NUMBER(38,0),
	FISCAL_1YR_DATE_KEY NUMBER(38,0),
	FISCAL_1YR_COMP_DT DATE,
	FISCAL_1YR_COMP_YEAR_DT DATE,
	FISCAL_1YR_COMP_QUARTER_DT DATE,
	FISCAL_1YR_COMP_PERIOD_DT DATE,
	FISCAL_1YR_COMP_WEEK_DT DATE,
	FISCAL_1YR_COMP_WEEK_END_DT DATE,
	CALENDAR_1YR_DATE_KEY NUMBER(38,0),
	CALENDAR_1YR_COMP_DT DATE,
	CALENDAR_1YR_COMP_YEAR_DT DATE,
	CALENDAR_1YR_COMP_QUARTER_DT DATE,
	CALENDAR_1YR_COMP_PERIOD_DT DATE,
	CALENDAR_1YR_COMP_WEEK_DT DATE,
	CALENDAR_1YR_COMP_WEEK_END_DT DATE,
	CALENDAR_2YR_COMP_DT DATE,
	CALENDAR_2YR_COMP_YEAR_DT DATE,
	CALENDAR_2YR_COMP_QUARTER_DT DATE,
	CALENDAR_2YR_COMP_PERIOD_DT DATE,
	CALENDAR_2YR_COMP_WEEK_DT DATE,
	CALENDAR_2YR_COMP_WEEK_END_DT DATE,
	CALENDAR_3YR_COMP_DT DATE,
	CALENDAR_3YR_COMP_YEAR_DT DATE,
	CALENDAR_3YR_COMP_QUARTER_DT DATE,
	CALENDAR_3YR_COMP_PERIOD_DT DATE,
	CALENDAR_3YR_COMP_WEEK_DT DATE,
	CALENDAR_3YR_COMP_WEEK_END_DT DATE,
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XAK1COMPARABLE_CALENDAR unique (CALENDAR_DT),
	constraint XPKCOMPARABLE_CALENDAR primary key (DATE_KEY),
	constraint R_552 foreign key (DATE_KEY) references DATE_DIM(DATE_KEY)
);
create TABLE IF NOT EXISTS COMP_DATES cluster by (BUSINESS_DT, TIME_INTERVAL, CALENDAR_DT)(
	TIME_INTERVAL VARCHAR(6),
	BUSINESS_DT DATE,
	DATE_KEY NUMBER(38,0),
	CALENDAR_DT DATE,
	CALENDAR_DAY_NM VARCHAR(16777216),
	FISCAL_YEAR_START_DT DATE,
	FISCAL_YEAR_END_DT DATE,
	FISCAL_QUARTER_START_DT DATE,
	FISCAL_QUARTER_END_DT DATE,
	FISCAL_PERIOD_START_DT DATE,
	FISCAL_PERIOD_END_DT DATE,
	FISCAL_WEEK_START_DT DATE,
	FISCAL_WEEK_END_DT DATE,
	FISCAL_WEEK_NBR NUMBER(2,0),
	FISCAL_PERIOD_NBR NUMBER(2,0),
	FISCAL_QUARTER_NBR NUMBER(1,0),
	FISCAL_YEAR_NBR NUMBER(4,0),
	HOLIDAY_IND VARCHAR(4),
	HOLIDAY_LIST_NM VARCHAR(255),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated'
);
create TABLE IF NOT EXISTS DAILY_FLASH_SALES_TEMP (
	SESSION_NO VARCHAR(16777216),
	TIME_INTERVAL VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	STORE_ID VARCHAR(16777216),
	BUSINESS_DT DATE,
	FISCAL_WEEK_NBR NUMBER(38,0),
	NET_SALES NUMBER(38,2),
	TRANS_CNT NUMBER(38,0),
	COMP_SALES NUMBER(38,2),
	COMP_TRANS NUMBER(38,0),
	DMA_NAME VARCHAR(16777216),
	DMA_CODE VARCHAR(16777216),
	OWNERSHIP_TYPE VARCHAR(16777216),
	STATE VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	OPEN_DATE DATE,
	CLOSURE_DATE DATE,
	TEMP_CLOSED_DATE DATE,
	REOPEN_DATE DATE
);
create TABLE IF NOT EXISTS DATE_DIM cluster by (DATE_KEY)(
	DATE_KEY NUMBER(38,0) NOT NULL,
	CALENDAR_DT DATE COMMENT 'Calendar Date is the actual Day, Month, and Year for a given day.',
	DATE_DISPLAY_NM VARCHAR(16777216) COMMENT 'Current Day in date format ',
	CALENDAR_YEAR_NBR NUMBER(38,0) COMMENT 'Current Year in 4 digit  Number format ',
	CALENDAR_DAY_IN_YEAR_NBR NUMBER(38,0) COMMENT 'Day number for the calendar Year',
	CALENDAR_MONTH_NBR NUMBER(38,0) COMMENT 'Current Month number',
	CALENDAR_MONTH_NM VARCHAR(16777216) COMMENT 'Name of the current Month',
	CALENDAR_DAYS_IN_YEAR_QTY NUMBER(38,0) COMMENT 'Total Number of days in calendar Year',
	CALENDAR_DAY_NM VARCHAR(16777216) COMMENT 'Calendar Day Name ',
	CALENDAR_YEAR_START_DT DATE COMMENT 'Start Date for the current Calendar',
	CALENDAR_YEAR_END_DT DATE COMMENT 'End Date for the current Calendar',
	CALENDAR_QUARTER_NBR NUMBER(38,0),
	CALENDAR_QUARTER_NM VARCHAR(16777216),
	CALENDAR_QUARTER_START_DT DATE,
	CALENDAR_QUARTER_END_DT DATE,
	CALENDAR_DAYS_IN_QUARTER_QTY NUMBER(38,0) COMMENT 'Number of days in Calendar quarter',
	CALENDAR_MONTH_START_DT DATE,
	CALENDAR_MONTH_END_DT DATE,
	CALENDAR_DAYS_IN_MONTH_QTY NUMBER(38,0),
	CALENDAR_WEEK_IN_YEAR_NBR NUMBER(38,0),
	CALENDAR_WEEK_START_DT DATE,
	CALENDAR_WEEK_END_DT DATE,
	WEEKDAY_IND BOOLEAN COMMENT 'Weeday Indicator specifies if a calendar day is considered during the week, typically Monday thru Friday, or on the Weekend, Saturday or Sunday. True- Date is weekday,False- Date is a weekend ',
	FISCAL_YEAR_NBR NUMBER(38,0),
	FISCAL_YEAR_START_DT DATE,
	FISCAL_YEAR_END_DT DATE,
	FISCAL_DAYS_IN_QUARTER_QTY NUMBER(38,0),
	FISCAL_DAYS_IN_YEAR_QTY NUMBER(38,0) COMMENT 'Fiscal Days in Year Quantity',
	FISCAL_DAY_IN_PERIOD_NBR NUMBER(38,0) COMMENT 'Day number in Fiscal period',
	FISCAL_PERIOD_NBR NUMBER(38,0) COMMENT 'Fiscal Period Number ',
	FISCAL_QUARTER_NBR NUMBER(38,0),
	FISCAL_QUARTER_START_DT DATE,
	FISCAL_QUARTER_END_DT DATE,
	FISCAL_PERIOD_START_DT DATE,
	FISCAL_PERIOD_END_DT DATE,
	FISCAL_DAYS_IN_PERIOD_QTY NUMBER(38,0),
	FISCAL_WEEKS_IN_PERIOD_QTY NUMBER(38,0),
	FISCAL_WEEKS_IN_YEAR_QTY NUMBER(38,0),
	FISCAL_WEEK_NBR NUMBER(38,0),
	FISCAL_WEEKS_IN_QUARTER_QTY NUMBER(38,0),
	FISCAL_WEEK_START_DT DATE,
	FISCAL_WEEK_END_DT DATE,
	FISCAL_DAY_IN_WEEK_NBR NUMBER(38,0),
	FISCAL_COMPARABLE_DT DATE COMMENT 'Fiscal Comparable Date',
	CALENDAR_COMPARABLE_DT DATE COMMENT 'Prior Year Calendar comp Date( the date is -364) ',
	CALENDAR_COMPARABLE_START_DT DATE,
	CALENDAR_COMPARABLE_END_DT DATE,
	CALENDAR_COMPARABLE_DAYS_IN_YEAR_QTY NUMBER(38,0),
	CALENDAR_COMPARABLE_IN_YEAR_NBR NUMBER(38,0),
	CALENDAR_COMPARABLE_DAYS_IN_PERIOD_QTY NUMBER(38,0),
	CALENDAR_COMPARABLE_QUARTER_NBR NUMBER(38,0),
	CALENDAR_COMPARABLE_QUARTER_START_DT DATE COMMENT 'Comparable Calendar Quarter Start Date is the ',
	CALENDAR_COMPARABLE_QUARTER_END_DT DATE COMMENT 'Comparable Calendar Quarter End Date',
	CALENDAR_COMPARABLE_PERIOD_START_DT DATE,
	CALENDAR_COMPARABLE_PERIOD_END_DT DATE,
	CALENDAR_COMPARABLE_WEEK_START_DT DATE,
	CALENDAR_COMPARABLE_WEEK_END_DT DATE,
	CALENDAR_COMPARABLE_DAY_IN_WEEK_NBR NUMBER(38,0),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	LOAD_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKDATE_DIM primary key (DATE_KEY)
)COMMENT='Date dimension provides hierarchies for analyzing data for different dates or date ranges, such as over weeks, months, or individual days. '
;
create TABLE IF NOT EXISTS DQ_SOURCE_METADATA_CONFIGURATION (
	SOURCE_METADATA_CONFIGURATION_ID NUMBER(38,0) NOT NULL autoincrement COMMENT 'SOURCE_METADATA_CONFIGURATION_ID Primary Key of the table.',
	DATABASE_NAME VARCHAR(16777216) NOT NULL COMMENT 'Name of the Database that the source data will land.',
	SCHEMA_NAME VARCHAR(16777216) NOT NULL COMMENT 'Name of the Schema  that the source data will land.',
	TABLE_NAME VARCHAR(16777216) NOT NULL COMMENT 'Name of the Table that the source data will land.',
	COLUMN_NAME VARCHAR(16777216) COMMENT 'Name of the Column that the source data will land.',
	COLUMN_SEQUENCE NUMBER(38,0) COMMENT 'Column Sequence specifies the order in which a column is checked within a source.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand of the advertiser the source data is associated with.',
	SOURCE_SYSTEM_NAME VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKCONF_ID primary key (SOURCE_METADATA_CONFIGURATION_ID)
)COMMENT='DQ_SOURCE_METADATA_CONFIGURATION contains metadata information of all the tables which will be aligned with the Source Data Dictionary in the confluence. This table is used to validate the sources such as  files by checking whether the source files has all the columns listed in the Source_Metadata_Conf table and also checking the column order. '
;
create TABLE IF NOT EXISTS DQ_VALIDATION_RULE (
	DQ_RULE_ID NUMBER(38,0) NOT NULL autoincrement COMMENT 'DQ Rule Identifier within a brand uniquely identifiers a rule. Sample Values: 1.',
	RULE_TYPE VARCHAR(16777216) NOT NULL COMMENT 'Rule Type Sample Values: Count Validation.',
	RULE_DESC VARCHAR(16777216) NOT NULL COMMENT 'Rule Description Sample Values: Validate Record Count from ADLS to RDS.',
	RULE_SEVERITY_TYPE VARCHAR(16777216) NOT NULL COMMENT 'Rule Severity Type Sample Values: Critical.',
	TARGET_DATA_ZONE_CODE VARCHAR(16777216) NOT NULL COMMENT 'Target Data Zone Code Sample Values: RDS.',
	TABLE_NAME VARCHAR(16777216) COMMENT 'Table Name validation rule is applied to.  Sample Values: ACTUAL_SALES_TIME_SLOT.',
	COLUMN_NAME VARCHAR(16777216) COMMENT 'Column Name for validation rule.  Sample Values: N/A.',
	RULE_SQL_TEXT VARCHAR(16777216) COMMENT 'Rule SQL Text is the statement being executed for data quality validation.  Sample Values: Select Count(*) from RDS_DEV.ARB_.ACTUAL_SALES_TIME_SLOT WHERE FILENAME LIKE  ''''%RTI%''''.',
	RULE_LEVEL_TYPE VARCHAR(16777216) COMMENT 'Rule Level Type specifies Table or Column level rule.  Sample Values: Table .',
	RULE_OWNER_TYPE VARCHAR(16777216) COMMENT 'Rule Owner Type specifies who created the rule.  Sample Values: Business.',
	RULE_ACTIVE_IND BOOLEAN DEFAULT FALSE COMMENT 'Rule Active Indicator specifies if the data quality rule can be run.',
	RULE_STATUS_TYPE VARCHAR(16777216) COMMENT 'Rule Status Type Specifies if rule is Actice, In-Active, Not Valid.',
	RULE_STATUS_DESC VARCHAR(16777216) COMMENT 'Rule Status Description allows for further describing the status if required. For example, Rule was deprecated due to column removal or Column added to other rule validation.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL,
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKDQ_VALIDATION_RULE primary key (DQ_RULE_ID)
)COMMENT='Data Quality(DQ) Validation Rule contains the checks on data as data moves through its various stages.'
;
create TABLE IF NOT EXISTS DQ_VALIDATION_RULE_RESULT (
	DQ_RESULT_ID NUMBER(38,0) NOT NULL autoincrement COMMENT 'Rule Result Identifier Sample Values: 1.',
	DQ_RULE_ID NUMBER(38,0) NOT NULL COMMENT 'DQ Rule Identifier within a brand uniquely identifiers a rule. Sample Values: 1.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	TABLE_BUSINESS_KEY VARCHAR(16777216) COMMENT 'Table Business Key is the combination of fields the data qualtity check was performed on. Sample Values: STOREID|DATE|Filename.',
	BUSINESS_KEY_VALUE VARCHAR(16777216) COMMENT 'Business Key Value contains tha specific business keys used in the data quality check if applicable to the rule.  Sample Values: N/A.',
	RULE_SQL_OUTPUT_TEXT VARCHAR(16777216) COMMENT 'Rule SQL Output Text output of the rul.  Sample Values: 825.',
	DATA_BUSINESS_DATE DATE COMMENT 'Data Business Date is the business day the data validation rule was runr for.  Sample Values: 20220208.',
	RESULT_STATUS_TYPE VARCHAR(16777216) COMMENT 'Result Status Type Sample Values: Active.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL,
	LOAD_ID VARCHAR(16777216) NOT NULL COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) NOT NULL COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKDQ_VALIDATION_RULE_RESULT primary key (DQ_RESULT_ID)
)COMMENT='Data Quality(DQ) Validation Rule Result contains the result of a data check and is at the Business Date grain.'
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
create TABLE IF NOT EXISTS HOLIDAY_DIM (
	HOLIDAY_KEY NUMBER(38,0) NOT NULL,
	DATE_KEY NUMBER(38,0) NOT NULL,
	BRAND_ID VARCHAR(16777216) COMMENT 'campaignInspire recognized brand id. irb when applied to all brands ',
	HOLIDAY_NM VARCHAR(255) NOT NULL COMMENT 'The Name of the Holiday',
	HOLIDAY_DESC VARCHAR(16777216) COMMENT 'Name of the objective Holiday.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Inspire level project',
	LOAD_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Inserted',
	UPDATE_ID VARCHAR(16777216) COMMENT 'This is the User Identifier of the Person or Process that Updated the Table',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKHOLIDAY_DIM primary key (HOLIDAY_KEY),
	constraint DATE_DIM_TO_HOLIDAY_DIM foreign key (DATE_KEY) references DATE_DIM(DATE_KEY)
)COMMENT='Store Holiday Dimension contains a list of holidays that are observed by brand. The holidays that are recognized across all Inspire Recognized Brands will have a brand identifier of irb.'
;
create TABLE IF NOT EXISTS ORDER_DETAIL_OFFERS_TEST (
	BRAND_ID VARCHAR(16777216),
	STORE_ID VARCHAR(16777216),
	MDM_ITEM_TYP VARCHAR(16777216),
	MDM_ITEM_ID VARCHAR(16777216),
	ITEM_NM VARCHAR(16777216),
	MDM_OFFER_ID NUMBER(38,0),
	OFFER_NM VARCHAR(16777216),
	MDM_PRODUCT_ID NUMBER(38,0),
	PRODUCT_NM VARCHAR(16777216),
	PRODUCT_HIERARCHY_ID NUMBER(38,0),
	LEVEL5_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL4_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL3_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL2_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	LEVEL1_PRODUCT_HIERARCHY_NM VARCHAR(16777216),
	OFFER_HIERARCHY_ID NUMBER(18,5),
	LEVEL5_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL4_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL3_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL2_OFFER_HIERARCHY_NM VARCHAR(16777216),
	LEVEL1_OFFER_HIERARCHY_NM VARCHAR(16777216),
	PRODUCT_MISC1 VARCHAR(16777216),
	PRODUCT_MISC2 VARCHAR(16777216),
	PRODUCT_MISC3 VARCHAR(16777216),
	PRODUCT_MISC4 VARCHAR(16777216),
	PRODUCT_MISC5 VARCHAR(16777216),
	PRODUCT_MISC6 VARCHAR(16777216),
	PRODUCT_MISC7 VARCHAR(16777216),
	PRODUCT_MISC8 VARCHAR(16777216),
	PRODUCT_MISC9 VARCHAR(16777216),
	PRODUCT_MISC10 VARCHAR(16777216),
	OFFER_MISC1 VARCHAR(16777216),
	OFFER_MISC2 VARCHAR(16777216),
	OFFER_MISC3 VARCHAR(16777216),
	OFFER_MISC4 VARCHAR(16777216),
	OFFER_MISC5 VARCHAR(16777216),
	OFFER_MISC6 VARCHAR(16777216),
	OFFER_MISC7 VARCHAR(16777216),
	OFFER_MISC8 VARCHAR(16777216),
	OFFER_MISC9 VARCHAR(16777216),
	OFFER_MISC10 VARCHAR(16777216)
);
create TABLE IF NOT EXISTS SONIC_SALES_TYPE_REF (
	SALES_TYP_COLUMN VARCHAR(50),
	SALES_TYP_CD VARCHAR(50),
	SALES_TYP_DESCRIPTION VARCHAR(50)
);
create TABLE IF NOT EXISTS STAGE_SURVEY (
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Name specifies a particular surver. For example, Customer Satisfaction.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	SURVEY_TYP_ID NUMBER(38,0) COMMENT 'Survey Type Identifier uniquely identifies an Inspire survey type.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY primary key (SURVEY_ID),
	constraint XAK1SURVEY unique (SURVEY_NM, BRAND_ID, SOURCE_SYSTEM_NM),
	constraint R_12 foreign key (SURVEY_TYP_ID) references STAGE_SURVEY_TYP(SURVEY_TYP_ID)
)COMMENT='Survey is a set of Inspire questions configured for reporting purposes across brands.\n\n'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_ALTERNATE_LANDING (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY.',
	TARGET_SURVEY_ID NUMBER(38,0) COMMENT 'Target Survey Identifier uniquely identifies an Inspire Survey to map survey results to.',
	ALTERNATE_LANDING_PREFIX_NM VARCHAR(16777216) NOT NULL COMMENT 'Alternate Landing Prefix Name is the prefix to be added to Survey Response for a different table set to land survey responses in. For example, Customer Satisfaction would result in data landing in Customer Satisfaction Survey Response.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ALTERNATE_LANDING primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID)
)COMMENT='Survey Alternate Landing specifies whether survey response data should land in a specifc set of repsonse tables instead of the general set of response tables.'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_ANSWER_ATTRIBUTE_MAPPING (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY',
	SOURCE_ATTRIBUTE_QUESTION_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Attribute Question Identifier specifies an attribute of a message that maybe incorporated as a question in an Inspire Survey, or a question that is to become an attribute of the response table. For example, QID1, QID7_2, or STORE_TYPE.',
	SOURCE_SURVEY_VAL VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Value is the value coming in from the source survey and typically from a pick list. For example, 1,2,3,4,5, Likely and Unlikely.',
	TARGET_SURVEY_ID NUMBER(38,0) COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	TARGET_SURVEY_QUESTION_ID NUMBER(38,0) COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	TARGET_SURVEY_VAL VARCHAR(16777216) COMMENT 'Target Survey Value  is the value that represents an option for a question. Typically this is an internal value not seen by the survey taker. For example, 1, 2, 3. This should tie to the value in Survey Question Choice, but is presented in the mapping if needed for easier reference.',
	TARGET_SURVEY_NM VARCHAR(16777216) COMMENT 'Target Survey Name is the name of the Inspire Survey and is included in the mapping table for addtional clarity if needed.',
	TARGET_ATTRIBUTE_ID VARCHAR(16777216) COMMENT 'Target Attribute Identifier specifies an attribute in an Inspire Standardized survey that is being pivoted from a source survey to a response attribute. For example, Overall Satisfaction Score Number or Day Part Name.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ANSWER_ATTRIBUTE_MAPPING primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID, SOURCE_ATTRIBUTE_QUESTION_ID, SOURCE_SURVEY_VAL)
)COMMENT='Survey Answer Attribute Mapping defines the mapping of a survey answer or attribute to an Inspire standardized answer or attribute. This table is populated for any questions in the Source Survey Question Attribute Transformation table with a transformation type of MAP, PIVOTMAP, or FUNCTIONMAP'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_CHANNEL (
	SURVEY_CHANNEL_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Channel Identifier uniquely identifies a survey channel.',
	SURVEY_CHANNEL_CD VARCHAR(16777216) COMMENT 'Survey Channel Code specifies a code on how an individual received a survey. For example, EMAIL, QR, ANON, RECEIPT.',
	SURVEY_CHANNEL_NM VARCHAR(16777216) COMMENT 'Survey Channel Name is the label on how an individual received a survey. For example, Email, QR Code, Anonymous, Receipt.',
	SURVEY_SUB_CHANNEL_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Sub Channel Code specifies a label to further refine how an individual received a survey. For example, EMAIL-INVITE, EMAIL-ECOML, QR, ANON, RECEIPT.',
	SURVEY_SUB_CHANNEL_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Sub Channel Code specifies a code to further refine how an individual received a survey. For example,Email Invite, Email Ecommerce, QR Code, or Receipt.',
	SURVEY_DESC VARCHAR(16777216) COMMENT 'Survey Description describes the surver channel. For example, Survey was submitted to the individual from a visit to a website where an email address was entered.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_CHANNEL primary key (SURVEY_CHANNEL_ID),
	constraint XAK1SURVEY_CHANNEL unique (SURVEY_SUB_CHANNEL_CD, SURVEY_CHANNEL_CD, BRAND_ID)
)COMMENT='Survey Channel specifies the method the individual survey was received by an individual.'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_CHANNEL_MAPPING (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY.',
	SOURCE_SURVEY_CHANNEL_CD VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Channel Code are the concatenated values from a surver that are utilized to determine a channel identifier. For example, anonymous|Receipt or anonymous|QR Code.',
	TARGET_CHANNEL_ID NUMBER(38,0) NOT NULL,
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_CHANNEL_MAPPING primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID, SOURCE_SURVEY_CHANNEL_CD)
)COMMENT='Survey Channel Mapping allows mapping of channels from a source survey (e.g. Qualtrics) to a standardized Inspire Survey Channel.'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_ORDER_FULFILLMENT (
	SURVEY_ORDER_FULFILLMENT_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Order Channel Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_FULFILLMENT_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Fulfillment Channel Code is the label for how an order was placed. For example, DELIVER, DI, CO, DRVTHRU, OAPUSHELF.',
	SURVEY_ORDER_FULFILLMENT_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Fulfillment Channel Name is the label for how an order was fulfilled by a store. For example,Delivery, Dine In, Carry Out, Drive Thru, OA Pick Up Shelf.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ORDER_FULFILLMENT primary key (SURVEY_ORDER_FULFILLMENT_ID)
)COMMENT='Survey Order Fulfillment Channel identifies the method by which an order was fulfilled at a restaurant.'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_ORDER_PLACEMENT (
	SURVEY_ORDER_PLACEMENT_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Order Placement Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_PLACEMENT_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Channel Name is the label for how an order was placed. For example, CALL, WEB, MOB, and ONSITE.',
	SURVEY_ORDER_PLACEMENT_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Channel Name is the label for how an order was placed. For example, Call In, Online Via Website, Mobile App, and At Location.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ORDER_PLACEMENT primary key (SURVEY_ORDER_PLACEMENT_ID)
)COMMENT='Survey Order Channel identifies the method by which an order was placed at a restaurant.'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_QUESTION (
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_QUESTION_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	SURVEY_QUESTION_TXT VARCHAR(16777216) NOT NULL COMMENT 'Survey Question Text contains the sentence being asked to the respondant. For example, How clear and easy was the ordering process?\n',
	SURVEY_QUESTION_TYP_CD VARCHAR(16777216) COMMENT 'Survey Question Type Code specifies if question is Free Form Text, Boolean or Pick List. For example, TEXT, BOOLEAN, LIST, DATE, etc.',
	SURVEY_QUESTION_CATEGORY_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Question Category Code is a code to allow grouping of questions. For example, OSAT, Order Channel, Order Fulfillment, Loyalty, Service Quality, and Order Content.',
	RESPONSE_HEADER_ATTRIBUTE_IND BOOLEAN COMMENT 'Response Header Attribute Indicator specifies if the question response is stored as an attribute on response header table instead of the answer table.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_QUESTION primary key (SURVEY_ID, SURVEY_QUESTION_ID),
	constraint XAK1SURVEY_QUESTION unique (BRAND_ID, SOURCE_SYSTEM_NM, SURVEY_QUESTION_CATEGORY_CD),
	constraint R_6 foreign key (SURVEY_ID) references STAGE_SURVEY(SURVEY_ID)
)COMMENT='Survey Question identifies the list of questions configured for each Inspire survey across brands.'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY',
	SOURCE_ATTRIBUTE_QUESTION_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Attribute Question Identifier specifies an attribute of a message that maybe incorporated as a question in an Inspire Survey, or a question that is to become an attribute of the response table. For example, QID1, QID7_2, or STORE_TYPE.',
	TARGET_SURVEY_ID NUMBER(38,0),
	TARGET_SURVEY_QUESTION_ID NUMBER(38,0),
	TARGET_ATTRIBUTE_ID VARCHAR(16777216) COMMENT 'Target Attribute Identifier specifies an attribute in an Inspire Standardized survey that is being pivoted from a source survey to a response attribute. For example, Overall Satisfaction Score Number or Day Part Name.',
	TRANSFORMATION_TYP_CD VARCHAR(16777216) COMMENT 'Transformation Type Code specifies the action necessary to transform an attribue or column. Valid values PASS THRU, PIVOT, PIVOT MAP, and MAP.',
	TARGET_MASK_TXT VARCHAR(16777216) COMMENT 'Target Mask Text helps in converting back and forth between data types. For example, if incoming data is a date 2021-01-02, then the mask yyyy-mm-dd could be used to convert the value to a date value.',
	TARGET_DATA_TYP_CD VARCHAR(16777216) COMMENT 'Target Data Type Code specifies the data type of a transformed value. This is useful when a conversion from one data type to another is necessary such as string to date.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID, SOURCE_ATTRIBUTE_QUESTION_ID)
)COMMENT='Source Survey Question Attribue Transformation identifies the mapping/transformation requirements for source questions to Inspire (target) questions or attributes. This table is utilized in specifying whether a question or attribute should be pivoted from question to attribute, attribute to question, or question to question.'
;
create TABLE IF NOT EXISTS STAGE_SURVEY_QUESTION_CHOICE (
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_QUESTION_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	SURVEY_QUESTION_CHOICE_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Question Choice Identifier uniquely an option for a quesiton within a survey.',
	SURVEY_QUESTION_CHOICE_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Question Choice Name is the label used for the options present to the survey recipient. For example, Somewhat likely, Very Likely, and Somewhat Unlikely.\n',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_QUESTION_CHOICE primary key (SURVEY_ID, SURVEY_QUESTION_ID, SURVEY_QUESTION_CHOICE_ID),
	constraint SURVEY_QUESTION_TO_SURVEY_QUESTION_CHOICE foreign key (SURVEY_ID, SURVEY_QUESTION_ID) references STAGE_SURVEY_QUESTION(SURVEY_ID,SURVEY_QUESTION_ID)
)COMMENT='Survey Question Choice are the standardized response options for survey questions. '
;
create TABLE IF NOT EXISTS STAGE_SURVEY_TYP (
	SURVEY_TYP_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Type Identifier uniquely identifies an Inspire survey type.',
	SURVEY_TYP_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Type Name specifies the type of standard Inspire Survey. For example, Customer Satisfaction.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_TYPE primary key (SURVEY_TYP_ID),
	constraint XAK1SURVEY_TYPE unique (SURVEY_TYP_NM, SOURCE_SYSTEM_NM, BRAND_ID)
)COMMENT='Survey Type specifies the survey category assigned to each Inspire survey.\n\n'
;
create TABLE IF NOT EXISTS STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT DATE NOT NULL COMMENT 'Business Date is the day of when normal business operations occured.',
	STORE_ID VARCHAR(16777216) NOT NULL COMMENT 'Number that uniquely identifies the Store.',
	ORDER_CHANNEL_NM VARCHAR(16777216) NOT NULL COMMENT 'Unique channel id to identify order and fulfullmint mode.',
	MDM_PRODUCT_ID NUMBER(38,0) NOT NULL COMMENT 'MDM item ID for the item ordered.',
	PRODUCT_NM VARCHAR(100) COMMENT 'Item Name is the label for the product that has been sold.',
	DMA_CD VARCHAR(4) COMMENT 'Designated Marketing Area Code is a 3 digit code sourced from Nielsen.',
	DMA_NM VARCHAR(16777216) COMMENT 'Designated Marketing Area Name',
	OWNERSHIP_TYP VARCHAR(16777216) COMMENT 'Company Owned / Franchise',
	PRODUCT_STANDARD_PRICE_AMT NUMBER(18,2) COMMENT 'Product Standard Price Amount is the highest price for a product that the product was sold most frequently for the day.',
	SOURCE_SYSTEM_NM VARCHAR(255) COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSTORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE primary key (BRAND_ID, BUSINESS_DT, STORE_ID, ORDER_CHANNEL_NM, MDM_PRODUCT_ID)
)COMMENT='Store Channel Product Daily Price contains the the most commonly sold price of a product per brand/restaurant/day/channel. '
;
create TABLE IF NOT EXISTS STORE_COUNT_THRESHOLD (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand associated with store\n',
	OWNERSHIP_TYP VARCHAR(16777216) NOT NULL COMMENT 'Company Owned or Franchished Store\n',
	THRESHOLD_PCT NUMBER(18,0) NOT NULL COMMENT 'Percent of stores that reported sales by Brand\n',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKSTORE_COUNT_THRESHOLD primary key (BRAND_ID, OWNERSHIP_TYP, THRESHOLD_PCT)
);
create TABLE IF NOT EXISTS STORE_COUNT_THRESHOLD_OVERRIDE (
	BUSINESS_DT DATE NOT NULL COMMENT 'Date of sales transaction\n',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand associated with store\n',
	OVERRRIDE_EXCLUSION_IND VARCHAR(16777216) NOT NULL COMMENT 'Indicator to Include Brand although below threshold\n',
	OVERRIDE_INCLUSION_IND VARCHAR(16777216) NOT NULL COMMENT 'Indicator to not include Brand\n',
	LOAD_TYP VARCHAR(16777216) COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The Date/Datetime the Record was Updated',
	constraint XPKSTORE_COUNT_THRESHOLD_OVERRIDE primary key (BUSINESS_DT, BRAND_ID, OVERRRIDE_EXCLUSION_IND, OVERRIDE_INCLUSION_IND)
);
create TABLE IF NOT EXISTS SURVEY (
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Name specifies a particular surver. For example, Customer Satisfaction.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	SURVEY_TYP_ID NUMBER(38,0) COMMENT 'Survey Type Identifier uniquely identifies an Inspire survey type.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY primary key (SURVEY_ID),
	constraint XAK1SURVEY unique (SURVEY_NM, BRAND_ID, SOURCE_SYSTEM_NM),
	constraint R_12 foreign key (SURVEY_TYP_ID) references SURVEY_TYP(SURVEY_TYP_ID)
)COMMENT='Survey is a set of Inspire questions configured for reporting purposes across brands.\n\n'
;
create TABLE IF NOT EXISTS SURVEY_ALTERNATE_LANDING (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY.',
	TARGET_SURVEY_ID NUMBER(38,0) COMMENT 'Target Survey Identifier uniquely identifies an Inspire Survey to map survey results to.',
	ALTERNATE_LANDING_PREFIX_NM VARCHAR(16777216) NOT NULL COMMENT 'Alternate Landing Prefix Name is the prefix to be added to Survey Response for a different table set to land survey responses in. For example, Customer Satisfaction would result in data landing in Customer Satisfaction Survey Response.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ALTERNATE_LANDING primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID)
)COMMENT='Survey Alternate Landing specifies whether survey response data should land in a specifc set of repsonse tables instead of the general set of response tables.'
;
create TABLE IF NOT EXISTS SURVEY_ANSWER_ATTRIBUTE_MAPPING (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY',
	SOURCE_ATTRIBUTE_QUESTION_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Attribute Question Identifier specifies an attribute of a message that maybe incorporated as a question in an Inspire Survey, or a question that is to become an attribute of the response table. For example, QID1, QID7_2, or STORE_TYPE.',
	SOURCE_SURVEY_VAL VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Value is the value coming in from the source survey and typically from a pick list. For example, 1,2,3,4,5, Likely and Unlikely.',
	TARGET_SURVEY_ID NUMBER(38,0) COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	TARGET_SURVEY_QUESTION_ID NUMBER(38,0) COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	TARGET_SURVEY_VAL VARCHAR(16777216) COMMENT 'Target Survey Value  is the value that represents an option for a question. Typically this is an internal value not seen by the survey taker. For example, 1, 2, 3. This should tie to the value in Survey Question Choice, but is presented in the mapping if needed for easier reference.',
	TARGET_SURVEY_NM VARCHAR(16777216) COMMENT 'Target Survey Name is the name of the Inspire Survey and is included in the mapping table for addtional clarity if needed.',
	TARGET_ATTRIBUTE_ID VARCHAR(16777216) COMMENT 'Target Attribute Identifier specifies an attribute in an Inspire Standardized survey that is being pivoted from a source survey to a response attribute. For example, Overall Satisfaction Score Number or Day Part Name.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ANSWER_ATTRIBUTE_MAPPING primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID, SOURCE_ATTRIBUTE_QUESTION_ID, SOURCE_SURVEY_VAL)
)COMMENT='Survey Answer Attribute Mapping defines the mapping of a survey answer or attribute to an Inspire standardized answer or attribute. This table is populated for any questions in the Source Survey Question Attribute Transformation table with a transformation type of MAP, PIVOTMAP, or FUNCTIONMAP'
;
create TABLE IF NOT EXISTS SURVEY_ANSWER_ATTRIBUTE_MAPPING_BKP_MEDALLIA (
	SOURCE_SYSTEM_NM VARCHAR(255),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SURVEY_ID VARCHAR(16777216),
	SOURCE_ATTRIBUTE_QUESTION_ID VARCHAR(16777216),
	SOURCE_SURVEY_VAL VARCHAR(16777216),
	TARGET_SURVEY_ID NUMBER(38,0),
	TARGET_SURVEY_QUESTION_ID NUMBER(38,0),
	TARGET_SURVEY_VAL VARCHAR(16777216),
	TARGET_SURVEY_NM VARCHAR(16777216),
	TARGET_ATTRIBUTE_ID VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS SURVEY_CHANNEL (
	SURVEY_CHANNEL_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Channel Identifier uniquely identifies a survey channel.',
	SURVEY_CHANNEL_CD VARCHAR(16777216) COMMENT 'Survey Channel Code specifies a code on how an individual received a survey. For example, EMAIL, QR, ANON, RECEIPT.',
	SURVEY_CHANNEL_NM VARCHAR(16777216) COMMENT 'Survey Channel Name is the label on how an individual received a survey. For example, Email, QR Code, Anonymous, Receipt.',
	SURVEY_SUB_CHANNEL_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Sub Channel Code specifies a label to further refine how an individual received a survey. For example, EMAIL-INVITE, EMAIL-ECOML, QR, ANON, RECEIPT.',
	SURVEY_SUB_CHANNEL_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Sub Channel Code specifies a code to further refine how an individual received a survey. For example,Email Invite, Email Ecommerce, QR Code, or Receipt.',
	SURVEY_DESC VARCHAR(16777216) COMMENT 'Survey Description describes the surver channel. For example, Survey was submitted to the individual from a visit to a website where an email address was entered.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_CHANNEL primary key (SURVEY_CHANNEL_ID),
	constraint XAK1SURVEY_CHANNEL unique (SURVEY_SUB_CHANNEL_CD, SURVEY_CHANNEL_CD, BRAND_ID)
)COMMENT='Survey Channel specifies the method the individual survey was received by an individual.'
;
create TABLE IF NOT EXISTS SURVEY_CHANNEL_MAPPING (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY.',
	SOURCE_SURVEY_CHANNEL_CD VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Channel Code are the concatenated values from a surver that are utilized to determine a channel identifier. For example, anonymous|Receipt or anonymous|QR Code.',
	TARGET_CHANNEL_ID NUMBER(38,0) NOT NULL,
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_CHANNEL_MAPPING primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID, SOURCE_SURVEY_CHANNEL_CD)
)COMMENT='Survey Channel Mapping allows mapping of channels from a source survey (e.g. Qualtrics) to a standardized Inspire Survey Channel.'
;
create TABLE IF NOT EXISTS SURVEY_ORDER_FULFILLMENT (
	SURVEY_ORDER_FULFILLMENT_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Order Channel Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_FULFILLMENT_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Fulfillment Channel Code is the label for how an order was placed. For example, DELIVER, DI, CO, DRVTHRU, OAPUSHELF.',
	SURVEY_ORDER_FULFILLMENT_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Fulfillment Channel Name is the label for how an order was fulfilled by a store. For example,Delivery, Dine In, Carry Out, Drive Thru, OA Pick Up Shelf.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ORDER_FULFILLMENT primary key (SURVEY_ORDER_FULFILLMENT_ID)
)COMMENT='Survey Order Fulfillment Channel identifies the method by which an order was fulfilled at a restaurant.'
;
create TABLE IF NOT EXISTS SURVEY_ORDER_PLACEMENT (
	SURVEY_ORDER_PLACEMENT_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Order Placement Identifier uniquely identifies how an order was placed at a store.',
	SURVEY_ORDER_PLACEMENT_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Channel Name is the label for how an order was placed. For example, CALL, WEB, MOB, and ONSITE.',
	SURVEY_ORDER_PLACEMENT_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Order Channel Name is the label for how an order was placed. For example, Call In, Online Via Website, Mobile App, and At Location.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_ORDER_PLACEMENT primary key (SURVEY_ORDER_PLACEMENT_ID)
)COMMENT='Survey Order Channel identifies the method by which an order was placed at a restaurant.'
;
create TABLE IF NOT EXISTS SURVEY_QUESTION (
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_QUESTION_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	SURVEY_QUESTION_TXT VARCHAR(16777216) NOT NULL COMMENT 'Survey Question Text contains the sentence being asked to the respondant. For example, How clear and easy was the ordering process?\n',
	SURVEY_QUESTION_TYP_CD VARCHAR(16777216) COMMENT 'Survey Question Type Code specifies if question is Free Form Text, Boolean or Pick List. For example, TEXT, BOOLEAN, LIST, DATE, etc.',
	SURVEY_QUESTION_CATEGORY_CD VARCHAR(16777216) NOT NULL COMMENT 'Survey Question Category Code is a code to allow grouping of questions. For example, OSAT, Order Channel, Order Fulfillment, Loyalty, Service Quality, and Order Content.',
	RESPONSE_HEADER_ATTRIBUTE_IND BOOLEAN COMMENT 'Response Header Attribute Indicator specifies if the question response is stored as an attribute on response header table instead of the answer table.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_QUESTION primary key (SURVEY_ID, SURVEY_QUESTION_ID),
	constraint XAK1SURVEY_QUESTION unique (BRAND_ID, SOURCE_SYSTEM_NM, SURVEY_QUESTION_CATEGORY_CD),
	constraint R_6 foreign key (SURVEY_ID) references SURVEY(SURVEY_ID)
)COMMENT='Survey Question identifies the list of questions configured for each Inspire survey across brands.'
;
create TABLE IF NOT EXISTS SURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION (
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SURVEY_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Survey Identifier is a unique identifier for a survey that is to be mapped to an Inspire survey. For exmaple, SV_e4K52qOul7NjPdY',
	SOURCE_ATTRIBUTE_QUESTION_ID VARCHAR(16777216) NOT NULL COMMENT 'Source Attribute Question Identifier specifies an attribute of a message that maybe incorporated as a question in an Inspire Survey, or a question that is to become an attribute of the response table. For example, QID1, QID7_2, or STORE_TYPE.',
	TARGET_SURVEY_ID NUMBER(38,0),
	TARGET_SURVEY_QUESTION_ID NUMBER(38,0),
	TARGET_ATTRIBUTE_ID VARCHAR(16777216) COMMENT 'Target Attribute Identifier specifies an attribute in an Inspire Standardized survey that is being pivoted from a source survey to a response attribute. For example, Overall Satisfaction Score Number or Day Part Name.',
	TRANSFORMATION_TYP_CD VARCHAR(16777216) COMMENT 'Transformation Type Code specifies the action necessary to transform an attribue or column. Valid values PASS THRU, PIVOT, PIVOT MAP, and MAP.',
	TARGET_MASK_TXT VARCHAR(16777216) COMMENT 'Target Mask Text helps in converting back and forth between data types. For example, if incoming data is a date 2021-01-02, then the mask yyyy-mm-dd could be used to convert the value to a date value.',
	TARGET_DATA_TYP_CD VARCHAR(16777216) COMMENT 'Target Data Type Code specifies the data type of a transformed value. This is useful when a conversion from one data type to another is necessary such as string to date.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_QUESTION_ATTRIBUTE_TRANSFORMATION primary key (SOURCE_SYSTEM_NM, BRAND_ID, SOURCE_SURVEY_ID, SOURCE_ATTRIBUTE_QUESTION_ID)
)COMMENT='Source Survey Question Attribue Transformation identifies the mapping/transformation requirements for source questions to Inspire (target) questions or attributes. This table is utilized in specifying whether a question or attribute should be pivoted from question to attribute, attribute to question, or question to question.'
;
create TABLE IF NOT EXISTS SURVEY_QUESTION_CHOICE (
	SURVEY_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Identifier uniquely identifies an Inspire survey.',
	SURVEY_QUESTION_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Question Identifier uniquely identifies a question within a survey.',
	SURVEY_QUESTION_CHOICE_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Question Choice Identifier uniquely an option for a quesiton within a survey.',
	SURVEY_QUESTION_CHOICE_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Question Choice Name is the label used for the options present to the survey recipient. For example, Somewhat likely, Very Likely, and Somewhat Unlikely.\n',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_QUESTION_CHOICE primary key (SURVEY_ID, SURVEY_QUESTION_ID, SURVEY_QUESTION_CHOICE_ID),
	constraint SURVEY_QUESTION_TO_SURVEY_QUESTION_CHOICE foreign key (SURVEY_ID, SURVEY_QUESTION_ID) references SURVEY_QUESTION(SURVEY_ID,SURVEY_QUESTION_ID)
)COMMENT='Survey Question Choice are the standardized response options for survey questions. '
;
create TABLE IF NOT EXISTS SURVEY_TYP (
	SURVEY_TYP_ID NUMBER(38,0) NOT NULL COMMENT 'Survey Type Identifier uniquely identifies an Inspire survey type.',
	SURVEY_TYP_NM VARCHAR(16777216) NOT NULL COMMENT 'Survey Type Name specifies the type of standard Inspire Survey. For example, Customer Satisfaction.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKSURVEY_TYPE primary key (SURVEY_TYP_ID),
	constraint XAK1SURVEY_TYPE unique (SURVEY_TYP_NM, SOURCE_SYSTEM_NM, BRAND_ID)
)COMMENT='Survey Type specifies the survey category assigned to each Inspire survey.\n\n'
;
create TABLE IF NOT EXISTS VW_COMP_CAL_DATE_3YR (
	DATE_KEY NUMBER(38,0),
	DATE_ID DATE,
	FISCAL_YEAR_START_DT DATE,
	FISCAL_QUARTER_START_DT DATE,
	FISCAL_PERIOD_START_DT DATE,
	FISCAL_WEEK_START_DT DATE,
	FISCAL1YRCOMP_DATEID DATE,
	FISCAL1YRCOMP_YEAR DATE,
	FISCAL1YRCOMP_QUARTER DATE,
	FISCAL1YRCOMP_PERIOD DATE,
	FISCAL1YRCOMP_WEEK DATE,
	CALENDAR1YRCOMP_DATEID DATE,
	CALENDAR1YRCOMP_YEAR DATE,
	CALENDAR1YRCOMP_QUARTER DATE,
	CALENDAR1YRCOMP_PERIOD DATE,
	CALENDAR1YRCOMP_WEEK DATE,
	CALENDAR2YRCOMP_DATEID DATE,
	CALENDAR2YRCOMP_YEAR DATE,
	CALENDAR2YRCOMP_QUARTER DATE,
	CALENDAR2YRCOMP_PERIOD DATE,
	CALENDAR2YRCOMP_WEEK DATE,
	CALENDAR3YRCOMP_DATEID DATE,
	CALENDAR3YRCOMP_YEAR DATE,
	CALENDAR3YRCOMP_QUARTER DATE,
	CALENDAR3YRCOMP_PERIOD DATE,
	CALENDAR3YRCOMP_WEEK DATE
);
create TABLE IF NOT EXISTS ZIP_TO_DMA (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	ZIP_CD VARCHAR(20) NOT NULL COMMENT 'Zip Code a group of five or nine numbers that are added to a postal address to assist the sorting of mail.',
	CITY_NM VARCHAR(255) COMMENT 'City Name is a label for a place where a large number of people live.',
	STATE_CD VARCHAR(3) COMMENT 'State Code is a two-letter alphabetic codes defined in U.S. Federal Information Processing Standard Publication (\"FIPS PUB\") 5-2 to identify U.S. states and certain other associated areas.',
	CNTRY_CD VARCHAR(3) COMMENT 'Country Code is a 3 character alphabetic geographical code used to represent countries and dependent areas',
	DMA_CD VARCHAR(10) COMMENT 'Designated Market Area(DMA) Code is a 3-digit number created by Nielsen�s to delineate the geographic boundaries of 210 distinctive regions to assess TV penetration of audience counts within the U.S. for a viewership year. There are some 4 digit DMA codes that have been created for international franchisees.',
	ZIP_DMA_NM VARCHAR(50) COMMENT 'Zip Designated Market Area(DMA) Name is label based on labels created by Nielsen�s to delineate the geographic boundaries of 210 distinctive regions to assess TV penetration of audience counts within the U.S. for a viewership year. DMA Name may be overridden with another name for reporting purposes. A zip code my reside in multiple DMAs and the label chosen for the zip code/DMA combination typically is for the DMA with the greatest coverage in a zip code.',
	SOURCE_SYSTEM_NM VARCHAR(255) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	constraint XPKZIP_TO_DMA primary key (BRAND_ID, ZIP_CD)
)COMMENT='Zip to DMA contains a mapping of zip codes to DMA codes. For a mapping for a particular brand additional zip codes and or dma codes can be added to the data set in order account for zip codes that are not associated with an official DMA Code.'
;
CREATE FILE FORMAT IF NOT EXISTS CSV_INGEST_INT_REF
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
;
CREATE FILE FORMAT IF NOT EXISTS DQ_LOAD_CSV_FORMAT
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	ENCODING = 'iso-8859-1'
;
CREATE FILE FORMAT IF NOT EXISTS SURVEY_CHANNEL_MAPPING_CSV
	SKIP_HEADER = 1
;
CREATE PROCEDURE IF NOT EXISTS CHECK_DMA("ENV" VARCHAR(16777216), "BRAND" VARCHAR(16777216), "SOURCE_SYSTEM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
        var rule_sql_output_text = "SELECT listagg(RULE_SQL_OUTPUT_TEXT, '' | '') FROM IDS_" + ENV + ".INT_REF.DQ_VALIDATION_RULE_RESULT WHERE BRAND_ID = ''" + BRAND + "'' AND SOURCE_SYSTEM_NAME = ''" + SOURCE_SYSTEM + "'' AND DQ_RULE_ID = 3 AND RESULT_STATUS_TYPE = ''true''";
        var new_dma_statement = snowflake.createStatement({sqlText: rule_sql_output_text});
        var new_dma_execution = new_dma_statement.execute();
        new_dma_execution.next();
        var new_dma_result = new_dma_execution.getColumnValue(1);
        
        try {
            var update_dma_rows = "UPDATE IDS_" + ENV + ".INT_REF.DQ_VALIDATION_RULE_RESULT SET RESULT_STATUS_TYPE = ''false'' WHERE BRAND_ID = ''" + BRAND + "'' AND SOURCE_SYSTEM_NAME = ''" + SOURCE_SYSTEM + "'' AND DQ_RULE_ID = 3 AND RESULT_STATUS_TYPE = ''true''";
            var update_checked_dma_statement = snowflake.createStatement({sqlText: update_dma_rows});
            var update_checked_dma_execution = update_checked_dma_statement.execute();
            }
        catch(err){}
        
        return new_dma_result;
    ';
CREATE PROCEDURE IF NOT EXISTS CHECK_PARTNERS("ENV" VARCHAR(16777216), "BRAND" VARCHAR(16777216), "SOURCE_SYSTEM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
        var rule_sql_output_text = "SELECT listagg(RULE_SQL_OUTPUT_TEXT, '' | '') FROM IDS_" + ENV + ".INT_REF.DQ_VALIDATION_RULE_RESULT WHERE BRAND_ID = ''" + BRAND + "'' AND SOURCE_SYSTEM_NAME = ''" + SOURCE_SYSTEM + "'' AND DQ_RULE_ID = 2";
        var new_partners_statement = snowflake.createStatement({sqlText: rule_sql_output_text});
        var new_partners_execution = new_partners_statement.execute();
        new_partners_execution.next();
        var new_partners_result = new_partners_execution.getColumnValue(1);
        return new_partners_result;
    ';
CREATE PROCEDURE IF NOT EXISTS CHECK_RULES("ENV" VARCHAR(16777216), "BRAND" VARCHAR(16777216), "SOURCE_SYSTEM" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
        var sql_rules_text = "SELECT * FROM IDS_" + ENV + ".INT_REF.DQ_VALIDATION_RULE WHERE RULE_ACTIVE_IND = TRUE AND BRAND_ID = ''" + BRAND + "'' AND SOURCE_SYSTEM_NAME = ''" + SOURCE_SYSTEM + "''";
        //var sql_rules_text = "SELECT * FROM DQ_VALIDATION_RULE WHERE RULE_ACTIVE_IND = TRUE AND DQ_RULE_ID IN (1,2,3,4)";
        var sql_rules_statement = snowflake.createStatement( {sqlText:sql_rules_text} );
        var sql_rules_execution = sql_rules_statement.execute();
        var sql_rules_final_result = [];
        
        while (sql_rules_execution.next()){
            var rule_text = sql_rules_execution.getColumnValue(''RULE_SQL_TEXT'');
            var rule_id = sql_rules_execution.getColumnValue(''DQ_RULE_ID'');
            var rule_brand = sql_rules_execution.getColumnValue(''BRAND_ID'');
            var rule_business_key = sql_rules_execution.getColumnValue(''COLUMN_NAME'');
            var rule_source = sql_rules_execution.getColumnValue(''SOURCE_SYSTEM_NAME'');
            var rule_status_type = sql_rules_execution.getColumnValue(''RULE_ACTIVE_IND'');
            var rule_business_key_value = "";
            var rule_output_text = "";
                        
            var rule_statement = snowflake.createStatement({sqlText:rule_text});
            var rule_execution = rule_statement.execute();
            while (rule_execution.next()) {
            
                var rule_result = rule_execution.getColumnValue(1);
                if (rule_business_key == "N/A") {
                    rule_business_key_value = "N/A";
                    rule_output_text = rule_result;
                } else {
                    rule_business_key_value = rule_result;
                    rule_output_text = "N/A";
                }
                
                var insert_sql_text = "\\
                    INSERT INTO DQ_VALIDATION_RULE_RESULT (DQ_RULE_ID, BRAND_ID, TABLE_BUSINESS_KEY, BUSINESS_KEY_VALUE, RULE_SQL_OUTPUT_TEXT, DATA_BUSINESS_DATE, RESULT_STATUS_TYPE, SOURCE_SYSTEM_NAME, LOAD_ID, LOAD_DTTM, UPDATE_ID, UPDATE_DTTM) \\
                    VALUES (\\
                        " + rule_id + ", \\
                        ''" + rule_brand + "'', \\
                        ''" + rule_business_key + "'', \\
                        ''" + rule_business_key_value + "'', \\
                        ''" + rule_output_text + "'', \\
                        CURRENT_DATE, \\
                        ''" + rule_status_type + "'', \\
                        ''" + rule_source + "'', \\
                        TO_CHAR(CURRENT_DATE, ''YYYYMMDD''), \\
                        TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)), \\
                        TO_CHAR(CURRENT_DATE, ''YYYYMMDD''), \\
                        TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP)) \\
                    )"
                var insert_statement = snowflake.createStatement({sqlText:insert_sql_text});
                var insert_execution = insert_statement.execute();
                //insert_execution.next();
                
                final_result_json = {rule_id, rule_brand, rule_source, rule_business_key, rule_business_key_value, rule_output_text};
                sql_rules_final_result.push(final_result_json);
            }
    
      }
      return sql_rules_final_result; 
    ';
CREATE PROCEDURE IF NOT EXISTS DAILY_FLASH_SALES_PROC("BRAND_NM" VARCHAR(16777216))
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS OWNER
AS '
--DECLARE
--    BRAND_NAME VARCHAR default ''ALL'';
BEGIN
    --let BRAND_NAME varchar := BRAND_NM;
    UPDATE FLASH_REPORT_BRAND_CONFIGURATION
    SET INCLUDE_BRAND_FLG = true
    where BRAND = :BRAND_NM;
END;


';
CREATE FUNCTION IF NOT EXISTS FN_INSPIRE_BRAND_FLASH_REPORT("BUSINESS_DATE" DATE)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS '

    return "res";
';
CREATE PROCEDURE IF NOT EXISTS SP_INSPIRE_BRAND_FLASH_REPORT("BUSINESS_DATE" DATE)
RETURNS TABLE ("AGG_LEVEL" VARCHAR(16777216), "BUSINESS_DATE" DATE, "BRAND" VARCHAR(16777216), "STORE_NBR" VARCHAR(16777216), "DMA_NM" VARCHAR(16777216), "DMA_CD" VARCHAR(16777216), "L1NAME" VARCHAR(16777216), "L2NAME" VARCHAR(16777216), "L3NAME" VARCHAR(16777216), "L4NAME" VARCHAR(16777216), "L5NAME" VARCHAR(16777216), "OWNERSHIP_TYP" VARCHAR(16777216), "STATE" VARCHAR(16777216), "COUNTRY" VARCHAR(16777216), "DAY_OF_WEEK" VARCHAR(10), "START_DT_CY" DATE, "END_DT_CY" DATE, "NET_SALES_CY" NUMBER(38,2), "TRANS_CY" NUMBER(38,0), "COMP_SALES_CY" NUMBER(38,2), "COMP_TRANS_CY" NUMBER(38,0), "FISC_DATE_LY" DATE, "FISC_START_DT_LY" DATE, "FISC_END_DT_LY" DATE, "FISC_NET_SALES_LY" NUMBER(38,2), "FISC_TRANS_LY" NUMBER(38,0), "FISC_COMP_SALES_LY" NUMBER(38,2), "FISC_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_LY" DATE, "CAL_COMP_START_DT_LY" DATE, "CAL_COMP_END_DT_LY" DATE, "CAL_NET_SALES_LY" NUMBER(38,2), "CAL_TRANS_LY" NUMBER(38,0), "CAL_COMP_SALES_LY" NUMBER(38,2), "CAL_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_2Y" DATE, "CAL_COMP_START_DT_2Y" DATE, "CAL_COMP_END_DT_2Y" DATE, "CAL_NET_SALES_2Y" NUMBER(38,2), "CAL_TRANS_2Y" NUMBER(38,0), "CAL_COMP_SALES_2Y" NUMBER(38,2), "CAL_COMP_TRANS_2Y" NUMBER(38,0), "CAL_COMP_DATE_3Y" DATE, "CAL_COMP_START_DT_3Y" DATE, "CAL_COMP_END_DT_3Y" DATE, "CAL_NET_SALES_3Y" NUMBER(38,2), "CAL_TRANS_3Y" NUMBER(38,0), "CAL_COMP_SALES_3Y" NUMBER(38,2), "CAL_COMP_TRANS_3Y" NUMBER(38,0))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    call SP_INSPIRE_BRAND_FLASH_REPORT (:BUSINESS_DATE, ''ALL'', ''Y'');
    let flash_report resultset := (select * from table(result_scan(last_query_id())));
    return table(flash_report);
END
';
CREATE PROCEDURE IF NOT EXISTS SP_INSPIRE_BRAND_FLASH_REPORT("BUSINESS_DATE" DATE, "BRAND" VARCHAR(16777216), "THRESHOLD_FLG" VARCHAR(16777216))
RETURNS TABLE ("AGG_LEVEL" VARCHAR(16777216), "BUSINESS_DATE" DATE, "BRAND" VARCHAR(16777216), "STORE_NBR" VARCHAR(16777216), "DMA_NM" VARCHAR(16777216), "DMA_CD" VARCHAR(16777216), "L1NAME" VARCHAR(16777216), "L2NAME" VARCHAR(16777216), "L3NAME" VARCHAR(16777216), "L4NAME" VARCHAR(16777216), "L5NAME" VARCHAR(16777216), "OWNERSHIP_TYP" VARCHAR(16777216), "STATE" VARCHAR(16777216), "COUNTRY" VARCHAR(16777216), "DAY_OF_WEEK" VARCHAR(10), "START_DT_CY" DATE, "END_DT_CY" DATE, "NET_SALES_CY" NUMBER(38,2), "TRANS_CY" NUMBER(38,0), "COMP_SALES_CY" NUMBER(38,2), "COMP_TRANS_CY" NUMBER(38,0), "FISC_DATE_LY" DATE, "FISC_START_DT_LY" DATE, "FISC_END_DT_LY" DATE, "FISC_NET_SALES_LY" NUMBER(38,2), "FISC_TRANS_LY" NUMBER(38,0), "FISC_COMP_SALES_LY" NUMBER(38,2), "FISC_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_LY" DATE, "CAL_COMP_START_DT_LY" DATE, "CAL_COMP_END_DT_LY" DATE, "CAL_NET_SALES_LY" NUMBER(38,2), "CAL_TRANS_LY" NUMBER(38,0), "CAL_COMP_SALES_LY" NUMBER(38,2), "CAL_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_2Y" DATE, "CAL_COMP_START_DT_2Y" DATE, "CAL_COMP_END_DT_2Y" DATE, "CAL_NET_SALES_2Y" NUMBER(38,2), "CAL_TRANS_2Y" NUMBER(38,0), "CAL_COMP_SALES_2Y" NUMBER(38,2), "CAL_COMP_TRANS_2Y" NUMBER(38,0), "CAL_COMP_DATE_3Y" DATE, "CAL_COMP_START_DT_3Y" DATE, "CAL_COMP_END_DT_3Y" DATE, "CAL_NET_SALES_3Y" NUMBER(38,2), "CAL_TRANS_3Y" NUMBER(38,0), "CAL_COMP_SALES_3Y" NUMBER(38,2), "CAL_COMP_TRANS_3Y" NUMBER(38,0))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    DELETE FROM IDS_DEV.DATASTORE.DAILY_FLASH_SALES_STG
    WHERE SESSION_NO = CURRENT_SESSION();
        
    INSERT INTO IDS_DEV.DATASTORE.DAILY_FLASH_SALES_STG
    SELECT CURRENT_SESSION() as SESSION_NO, ''CY'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V(:BUSINESS_DATE, ''CY'', :BRAND, :THRESHOLD_FLG))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''FISC1Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V(:BUSINESS_DATE, ''FISC1Y'', :BRAND, :THRESHOLD_FLG))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''CAL1Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V(:BUSINESS_DATE, ''CAL1Y'', :BRAND, :THRESHOLD_FLG))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''CAL2Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V(:BUSINESS_DATE, ''CAL2Y'', :BRAND, :THRESHOLD_FLG))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''CAL3Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V(:BUSINESS_DATE, ''CAL3Y'', :BRAND, :THRESHOLD_FLG));
    let flash_report resultset := (
        
        select 
        fiscal_curr_year.AGG_LEVEL, 
        fiscal_curr_year.AGG_START_DT AS BUSINESS_DATE,
        CASE WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''ARG'',''ARBYS'') THEN ''ARB''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''BWW'') THEN ''BWW''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''SDI'',''SONIC'') THEN ''SON''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''JJ'') THEN ''JJ''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''DNKN'') THEN ''DUN''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''BSKN'') THEN ''BR''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''RTO'') THEN ''RT''
            ELSE ''UNKNOWN''
            END as BRAND,
        fiscal_curr_year.STORE_ID as STORE_NBR,
        fiscal_curr_year.DMA_NAME as DMA_NM,
        fiscal_curr_year.DMA_CODE as DMA_CD,
        lv.LEVEL1_NM as L1Name,
        lv.LEVEL2_NM as L2Name,
        lv.LEVEL3_NM as L3Name,
        lv.LEVEL4_NM as L4Name,
        lv.LEVEL5_NM as L5Name,
        fiscal_curr_year.OWNERSHIP_TYPE as OWNERSHIP_TYP,
        fiscal_curr_year.STATE,
        fiscal_curr_year.COUNTRY,
        fiscal_curr_year.DAY_OF_WEEK,
        fiscal_curr_year.AGG_START_DT as START_DT_CY,
        fiscal_curr_year.END_DT as END_DT_CY,
        fiscal_curr_year.Net_Sales as Net_Sales_CY,
        fiscal_curr_year.Trans_Cnt as Trans_CY,
        CASE 
        WHEN fiscal_curr_year.Comp_Sales IS NULL THEN NULL
        WHEN 
        (fiscal_curr_year.Net_Sales = 0 OR fiscal_curr_year.Comp_Sales = 0) AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE 
        fiscal_curr_year.Comp_Sales 
        END 
        as Comp_Sales_CY,
        CASE 
        WHEN fiscal_curr_year.Comp_Trans IS NULL THEN NULL
        WHEN 
        (fiscal_curr_year.Net_Sales = 0 OR fiscal_curr_year.Comp_Trans = 0) AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR fiscal_curr_year.Comp_Trans = 0
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
        fiscal_curr_year.Comp_Trans 
        END 
        as Comp_Trans_CY,
        fiscal_last_year.AGG_START_DT as Fisc_DATE_LY,
        fiscal_last_year.AGG_START_DT as FISC_START_DT_LY,
        fiscal_last_year.END_DT as FISC_END_DT_LY,
        fiscal_last_year.Net_Sales as Fisc_Net_Sales_LY,
        fiscal_last_year.Trans_Cnt as Fisc_Trans_LY,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Sales IS NULL THEN NULL
        WHEN 
        (
            fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (fiscal_last_year.Net_Sales = 0 OR fiscal_last_year.Comp_Sales = 0)
            AND NOT nvl(fiscal_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    fiscal_last_year.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(fiscal_last_year.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.FISC_1Y_SALES IS NOT NULL
                THEN fiscal_last_year.Comp_Sales - WP.FISC_1Y_SALES   --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.FISC_1Y_SALES IS NOT NULL
                THEN fiscal_last_year.Comp_Sales - WY.FISC_1Y_SALES   --ODI-57
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.FISC_1Y_SALES IS NOT NULL
                THEN fiscal_last_year.Comp_Sales - WQ.FISC_1Y_SALES   --ODI-57
                ELSE fiscal_last_year.Comp_Sales
            END
        END 
        as Fisc_Comp_Sales_LY,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Trans IS NULL THEN NULL
        WHEN 
        (fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (fiscal_last_year.Net_Sales = 0 OR fiscal_last_year.Comp_Trans = 0)
            AND NOT nvl(fiscal_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    fiscal_last_year.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(fiscal_last_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.FISC_1Y_TRANS IS NOT NULL
                THEN fiscal_last_year.Comp_Trans - WP.FISC_1Y_TRANS  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.FISC_1Y_TRANS IS NOT NULL 
                THEN fiscal_last_year.Comp_Trans - WY.FISC_1Y_TRANS  --ODI-57
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.FISC_1Y_TRANS IS NOT NULL
                THEN fiscal_last_year.Comp_Trans - WQ.FISC_1Y_TRANS  --ODI-57
                ELSE fiscal_last_year.Comp_Trans
            END
        END 
        as Fisc_Comp_Trans_LY,
        calendar_last_year.AGG_START_DT as Cal_Comp_DATE_LY,
        calendar_last_year.AGG_START_DT as CAL_COMP_START_DT_LY,
        calendar_last_year.END_DT as CAL_COMP_END_DT_LY,
        calendar_last_year.Net_Sales as Cal_Net_Sales_LY,
        calendar_last_year.Trans_Cnt as Cal_Trans_LY,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Sales IS NULL THEN NULL
        WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL 
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (calendar_last_year.Net_Sales = 0 OR calendar_last_year.Comp_Sales = 0)
            AND NOT nvl(calendar_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    calendar_last_year.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_last_year.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_1Y_SALES IS NOT NULL
                THEN calendar_last_year.Comp_Sales - WP.CAL_1Y_SALES  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD''  AND WY.CAL_1Y_SALES IS NOT NULL
                THEN calendar_last_year.Comp_Sales - WY.CAL_1Y_SALES  --ODI-57
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_1Y_SALES IS NOT NULL
                THEN calendar_last_year.Comp_Sales - WQ.CAL_1Y_SALES  --ODI-57
                ELSE calendar_last_year.Comp_Sales
            END
        END as Cal_Comp_Sales_LY,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Trans IS NULL THEN NULL
        WHEN 
        (
            fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (calendar_last_year.Net_Sales = 0 OR calendar_last_year.Comp_Trans = 0)
            AND NOT nvl(calendar_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    calendar_last_year.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_last_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET)
            WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_1Y_TRANS IS NOT NULL
            THEN calendar_last_year.Comp_Trans - WP.CAL_1Y_TRANS  --ODI-515
            WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_1Y_TRANS IS NOT NULL
            THEN calendar_last_year.Comp_Trans - WY.CAL_1Y_TRANS
            WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_1Y_TRANS IS NOT NULL
            THEN calendar_last_year.Comp_Trans - WQ.CAL_1Y_TRANS
            ELSE calendar_last_year.Comp_Trans
            END
        END as Cal_Comp_Trans_LY,
        calendar_two_years.AGG_START_DT as Cal_Comp_DATE_2Y,
        calendar_two_years.AGG_START_DT as CAL_COMP_START_DT_2Y,
        calendar_two_years.END_DT as CAL_COMP_END_DT_2Y,
        calendar_two_years.Net_Sales as Cal_Net_Sales_2Y,
        calendar_two_years.Trans_Cnt as Cal_Trans_2Y,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Sales IS NULL THEN NULL
        WHEN 
        (
            fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR   
        (calendar_two_years.Net_Sales = 0 OR calendar_two_years.Comp_Sales = 0)
            AND NOT nvl(calendar_two_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_two_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    calendar_two_years.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_two_years.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_2Y_SALES IS NOT NULL
                THEN calendar_two_years.Comp_Sales - WP.CAL_2Y_SALES  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_2Y_SALES IS NOT NULL
                THEN calendar_two_years.Comp_Sales - WY.CAL_2Y_SALES
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_2Y_SALES IS NOT NULL
                THEN calendar_two_years.Comp_Sales - WQ.CAL_2Y_SALES
                ELSE calendar_two_years.Comp_Sales
            END
        END 
        as Cal_Comp_Sales_2Y,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Trans IS NULL THEN NULL
        WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR
        (calendar_two_years.Net_Sales = 0 OR calendar_two_years.Comp_Trans = 0)
            AND NOT nvl(calendar_two_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_two_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    calendar_two_years.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_two_years.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_2Y_TRANS IS NOT NULL
                THEN calendar_two_years.Comp_Trans - WP.CAL_2Y_TRANS  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_2Y_TRANS IS NOT NULL
                THEN calendar_two_years.Comp_Trans - WY.CAL_2Y_TRANS
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_2Y_TRANS IS NOT NULL
                THEN calendar_two_years.Comp_Trans - WQ.CAL_2Y_TRANS
                ELSE calendar_two_years.Comp_Trans
            END
        END as Cal_Comp_Trans_2Y,
        calendar_three_years.AGG_START_DT as Cal_Comp_DATE_3Y,
        calendar_three_years.AGG_START_DT as CAL_COMP_START_DT_3Y,
        calendar_three_years.END_DT as CAL_COMP_END_DT_3Y,
        calendar_three_years.Net_Sales as Cal_Net_Sales_3Y,
        calendar_three_years.Trans_Cnt as Cal_Trans_3Y,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Sales IS NULL THEN NULL
        WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR
        (calendar_three_years.Net_Sales = 0 OR calendar_three_years.Comp_Sales = 0)
            AND NOT nvl(calendar_three_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_three_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    calendar_three_years.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_three_years.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_3Y_SALES IS NOT NULL
                THEN calendar_three_years.Comp_Sales - WP.CAL_3Y_SALES  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_3Y_SALES IS NOT NULL
                THEN calendar_three_years.Comp_Sales - WY.CAL_3Y_SALES
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_3Y_SALES IS NOT NULL
                THEN calendar_three_years.Comp_Sales - WQ.CAL_3Y_SALES
                ELSE calendar_three_years.Comp_Sales
            END
        END as Cal_Comp_Sales_3Y,
        CASE 
        WHEN NOT fiscal_curr_year.THRESHOLD_FLAG or fiscal_curr_year.Comp_Trans IS NULL THEN NULL
        WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        ) 
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR
        (calendar_three_years.Net_Sales = 0 OR calendar_three_years.Comp_Trans = 0)
            AND NOT nvl(calendar_three_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_three_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND (fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE OR NOT fiscal_curr_year.WEEK_TRESHOLD[ARRAY_POSITION(cast(6 as variant), fiscal_curr_year.WEEKDAY_OFFSET)])
                THEN
                    calendar_three_years.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_three_years.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_3Y_TRANS IS NOT NULL
                THEN calendar_three_years.Comp_Trans  - WP.CAL_3Y_TRANS  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_3Y_TRANS IS NOT NULL
                THEN calendar_three_years.Comp_Trans  - WY.CAL_3Y_TRANS
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_3Y_TRANS IS NOT NULL
                THEN calendar_three_years.Comp_Trans  - WQ.CAL_3Y_TRANS
                ELSE calendar_three_years.Comp_Trans
            END
        END as Cal_Comp_Trans_3Y
        FROM 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL(:BUSINESS_DATE, ''CY'')) as fiscal_curr_year
        inner join COMP_CALENDAR dt
        on dt.CALENDAR_DT = fiscal_curr_year.business_dt
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL(:BUSINESS_DATE,''FISC1Y'')) as fiscal_last_year
        on dt.FISCAL_1YR_COMP_DT = fiscal_last_year.business_dt
        and fiscal_curr_year.store_id = fiscal_last_year.store_id
        and fiscal_curr_year.brand_id = fiscal_last_year.brand_id
        and fiscal_curr_year.AGG_LEVEL = fiscal_last_year.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL(:BUSINESS_DATE,''CAL1Y'')) as calendar_last_year
        on dt.CALENDAR_1YR_COMP_DT = calendar_last_year.business_dt
        and fiscal_curr_year.store_id = calendar_last_year.store_id
        and fiscal_curr_year.brand_id = calendar_last_year.brand_id
        and fiscal_curr_year.AGG_LEVEL = calendar_last_year.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL(:BUSINESS_DATE,''CAL2Y'')) as calendar_two_years
        on dt.CALENDAR_2YR_COMP_DT = calendar_two_years.business_dt
        and fiscal_curr_year.store_id = calendar_two_years.store_id
        and fiscal_curr_year.brand_id = calendar_two_years.brand_id
        and fiscal_curr_year.AGG_LEVEL = calendar_two_years.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL(:BUSINESS_DATE,''CAL3Y'')) as calendar_three_years
        on dt.CALENDAR_3YR_COMP_DT = calendar_three_years.business_dt
        and fiscal_curr_year.store_id = calendar_three_years.store_id
        and fiscal_curr_year.brand_id = calendar_three_years.brand_id
        and fiscal_curr_year.AGG_LEVEL = calendar_three_years.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_WEEKS_IN_PERIODS(:BUSINESS_DATE)) as WP
        on  fiscal_curr_year.store_id = WP.store_id
        and fiscal_curr_year.brand_id = WP.brand_id
        and fiscal_curr_year.AGG_START_DT = WP.FISCAL_PERIOD_START_DT
        AND fiscal_last_year.AGG_START_DT = WP.FISCAL_1YR_COMP_PERIOD_DT 
        AND calendar_last_year.AGG_START_DT = WP.CALENDAR_1YR_COMP_PERIOD_DT
        AND calendar_two_years.AGG_START_DT = NVL(WP.CALENDAR_2YR_COMP_PERIOD_DT,calendar_two_years.AGG_START_DT)
        AND calendar_three_years.AGG_START_DT = NVL(WP.CALENDAR_3YR_COMP_PERIOD_DT,calendar_three_years.AGG_START_DT)
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_WEEKS_IN_QUARTERS(:BUSINESS_DATE)) as WQ
        on   fiscal_curr_year.store_id = WQ.store_id
        and fiscal_curr_year.brand_id = WQ.brand_id
        and fiscal_curr_year.AGG_START_DT = WQ.FISCAL_QUARTER_START_DT
        AND fiscal_last_year.AGG_START_DT = WQ.FISCAL_1YR_COMP_QUARTER_DT 
        AND calendar_last_year.AGG_START_DT = WQ.CALENDAR_1YR_COMP_QUARTER_DT
        AND calendar_two_years.AGG_START_DT = NVL(WQ.CALENDAR_2YR_COMP_QUARTER_DT,calendar_two_years.AGG_START_DT)
        AND calendar_three_years.AGG_START_DT = NVL(WQ.CALENDAR_3YR_COMP_QUARTER_DT,calendar_three_years.AGG_START_DT)
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_WEEKS_IN_YEARS(:BUSINESS_DATE)) as WY
        on   fiscal_curr_year.store_id = WY.store_id
        and fiscal_curr_year.brand_id = WY.brand_id
        and fiscal_curr_year.AGG_START_DT = WY.FISCAL_YEAR_START_DT
        AND fiscal_last_year.AGG_START_DT = WY.FISCAL_1YR_COMP_YEAR_DT 
        AND calendar_last_year.AGG_START_DT = WY.CALENDAR_1YR_COMP_YEAR_DT
        AND calendar_two_years.AGG_START_DT = NVL(WY.CALENDAR_2YR_COMP_YEAR_DT,calendar_two_years.AGG_START_DT)
        AND calendar_three_years.AGG_START_DT = NVL(WY.CALENDAR_3YR_COMP_YEAR_DT,calendar_three_years.AGG_START_DT)
        left outer join 
        IDH_DEV.OPERATION.LOCATION_HIERARCHY_V lv 
        on  lv.LOCATION_ID = fiscal_curr_year.STORE_ID
        and lv.brand_id = fiscal_curr_year.brand_id
        order by 
        fiscal_curr_year.brand_id ASC, 
        fiscal_curr_year.store_id ASC,
        (case when fiscal_curr_year.AGG_LEVEL = ''DAY'' then 1
            when fiscal_curr_year.AGG_LEVEL = ''WTD'' then 2
            when fiscal_curr_year.AGG_LEVEL = ''PTD'' then 3
            when fiscal_curr_year.AGG_LEVEL = ''QTD'' then 4
            when fiscal_curr_year.AGG_LEVEL = ''YTD'' then 5 END) ASC,
        fiscal_curr_year.AGG_START_DT DESC

    );

    return table(flash_report);
END
';
CREATE PROCEDURE IF NOT EXISTS SP_INSPIRE_BRAND_FLASH_REPORT_WITHOUT_THRESHOLD("BUSINESS_DATE" DATE)
RETURNS TABLE ("AGG_LEVEL" VARCHAR(16777216), "BUSINESS_DATE" DATE, "BRAND" VARCHAR(16777216), "STORE_NBR" VARCHAR(16777216), "DMA_NM" VARCHAR(16777216), "DMA_CD" VARCHAR(16777216), "L1NAME" VARCHAR(16777216), "L2NAME" VARCHAR(16777216), "L3NAME" VARCHAR(16777216), "L4NAME" VARCHAR(16777216), "L5NAME" VARCHAR(16777216), "OWNERSHIP_TYP" VARCHAR(16777216), "STATE" VARCHAR(16777216), "COUNTRY" VARCHAR(16777216), "DAY_OF_WEEK" VARCHAR(10), "START_DT_CY" DATE, "END_DT_CY" DATE, "NET_SALES_CY" NUMBER(38,2), "TRANS_CY" NUMBER(38,0), "COMP_SALES_CY" NUMBER(38,2), "COMP_TRANS_CY" NUMBER(38,0), "FISC_DATE_LY" DATE, "FISC_START_DT_LY" DATE, "FISC_END_DT_LY" DATE, "FISC_NET_SALES_LY" NUMBER(38,2), "FISC_TRANS_LY" NUMBER(38,0), "FISC_COMP_SALES_LY" NUMBER(38,2), "FISC_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_LY" DATE, "CAL_COMP_START_DT_LY" DATE, "CAL_COMP_END_DT_LY" DATE, "CAL_NET_SALES_LY" NUMBER(38,2), "CAL_TRANS_LY" NUMBER(38,0), "CAL_COMP_SALES_LY" NUMBER(38,2), "CAL_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_2Y" DATE, "CAL_COMP_START_DT_2Y" DATE, "CAL_COMP_END_DT_2Y" DATE, "CAL_NET_SALES_2Y" NUMBER(38,2), "CAL_TRANS_2Y" NUMBER(38,0), "CAL_COMP_SALES_2Y" NUMBER(38,2), "CAL_COMP_TRANS_2Y" NUMBER(38,0), "CAL_COMP_DATE_3Y" DATE, "CAL_COMP_START_DT_3Y" DATE, "CAL_COMP_END_DT_3Y" DATE, "CAL_NET_SALES_3Y" NUMBER(38,2), "CAL_TRANS_3Y" NUMBER(38,0), "CAL_COMP_SALES_3Y" NUMBER(38,2), "CAL_COMP_TRANS_3Y" NUMBER(38,0))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    call SP_INSPIRE_BRAND_FLASH_REPORT_without_threshold (:BUSINESS_DATE, ''ALL'');
    let flash_report resultset := (select * from table(result_scan(last_query_id())));
    return table(flash_report);
END
';
CREATE PROCEDURE IF NOT EXISTS SP_INSPIRE_BRAND_FLASH_REPORT_WITHOUT_THRESHOLD("BUSINESS_DATE" DATE, "BRAND" VARCHAR(16777216))
RETURNS TABLE ("AGG_LEVEL" VARCHAR(16777216), "BUSINESS_DATE" DATE, "BRAND" VARCHAR(16777216), "STORE_NBR" VARCHAR(16777216), "DMA_NM" VARCHAR(16777216), "DMA_CD" VARCHAR(16777216), "L1NAME" VARCHAR(16777216), "L2NAME" VARCHAR(16777216), "L3NAME" VARCHAR(16777216), "L4NAME" VARCHAR(16777216), "L5NAME" VARCHAR(16777216), "OWNERSHIP_TYP" VARCHAR(16777216), "STATE" VARCHAR(16777216), "COUNTRY" VARCHAR(16777216), "DAY_OF_WEEK" VARCHAR(10), "START_DT_CY" DATE, "END_DT_CY" DATE, "NET_SALES_CY" NUMBER(38,2), "TRANS_CY" NUMBER(38,0), "COMP_SALES_CY" NUMBER(38,2), "COMP_TRANS_CY" NUMBER(38,0), "FISC_DATE_LY" DATE, "FISC_START_DT_LY" DATE, "FISC_END_DT_LY" DATE, "FISC_NET_SALES_LY" NUMBER(38,2), "FISC_TRANS_LY" NUMBER(38,0), "FISC_COMP_SALES_LY" NUMBER(38,2), "FISC_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_LY" DATE, "CAL_COMP_START_DT_LY" DATE, "CAL_COMP_END_DT_LY" DATE, "CAL_NET_SALES_LY" NUMBER(38,2), "CAL_TRANS_LY" NUMBER(38,0), "CAL_COMP_SALES_LY" NUMBER(38,2), "CAL_COMP_TRANS_LY" NUMBER(38,0), "CAL_COMP_DATE_2Y" DATE, "CAL_COMP_START_DT_2Y" DATE, "CAL_COMP_END_DT_2Y" DATE, "CAL_NET_SALES_2Y" NUMBER(38,2), "CAL_TRANS_2Y" NUMBER(38,0), "CAL_COMP_SALES_2Y" NUMBER(38,2), "CAL_COMP_TRANS_2Y" NUMBER(38,0), "CAL_COMP_DATE_3Y" DATE, "CAL_COMP_START_DT_3Y" DATE, "CAL_COMP_END_DT_3Y" DATE, "CAL_NET_SALES_3Y" NUMBER(38,2), "CAL_TRANS_3Y" NUMBER(38,0), "CAL_COMP_SALES_3Y" NUMBER(38,2), "CAL_COMP_TRANS_3Y" NUMBER(38,0))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

    DELETE FROM IDS_DEV.DATASTORE.DAILY_FLASH_SALES_STG
    WHERE SESSION_NO = CURRENT_SESSION();

    INSERT INTO IDS_DEV.DATASTORE.DAILY_FLASH_SALES_STG
    SELECT CURRENT_SESSION() as SESSION_NO, ''CY'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V_without_threshold(:BUSINESS_DATE, ''CY'', :BRAND))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''FISC1Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V_without_threshold(:BUSINESS_DATE, ''FISC1Y'', :BRAND))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''CAL1Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V_without_threshold(:BUSINESS_DATE, ''CAL1Y'', :BRAND))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''CAL2Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V_without_threshold(:BUSINESS_DATE, ''CAL2Y'', :BRAND))
    UNION
    SELECT CURRENT_SESSION() as SESSION_NO, ''CAL3Y'' as TIME_INTERVAL, * from table(IDH_DEV.SALES.DAILY_FLASH_SALES_V_without_threshold(:BUSINESS_DATE, ''CAL3Y'', :BRAND));
    let flash_report resultset := (
        
        select 
        fiscal_curr_year.AGG_LEVEL, 
        fiscal_curr_year.AGG_START_DT AS BUSINESS_DATE,
        CASE WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''ARG'',''ARBYS'') THEN ''ARB''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''BWW'') THEN ''BWW''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''SDI'',''SONIC'') THEN ''SON''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''JJ'') THEN ''JJ''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''DNKN'') THEN ''DUN''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''BSKN'') THEN ''BR''
            WHEN UPPER(fiscal_curr_year.BRAND_ID) IN (''RTO'') THEN ''RT''
            ELSE ''UNKNOWN''
            END as BRAND,
        fiscal_curr_year.STORE_ID as STORE_NBR,
        fiscal_curr_year.DMA_NAME as DMA_NM,
        fiscal_curr_year.DMA_CODE as DMA_CD,
        lv.LEVEL1_NM as L1Name,
        lv.LEVEL2_NM as L2Name,
        lv.LEVEL3_NM as L3Name,
        lv.LEVEL4_NM as L4Name,
        lv.LEVEL5_NM as L5Name,
        fiscal_curr_year.OWNERSHIP_TYPE as OWNERSHIP_TYP,
        fiscal_curr_year.STATE,
        fiscal_curr_year.COUNTRY,
        fiscal_curr_year.DAY_OF_WEEK,
        fiscal_curr_year.AGG_START_DT as START_DT_CY,
        fiscal_curr_year.END_DT as END_DT_CY,
        fiscal_curr_year.Net_Sales as Net_Sales_CY,
        fiscal_curr_year.Trans_Cnt as Trans_CY,
        CASE WHEN 
        (fiscal_curr_year.Net_Sales = 0 OR fiscal_curr_year.Comp_Sales = 0) AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE 
        fiscal_curr_year.Comp_Sales 
        END 
        as Comp_Sales_CY,
        CASE WHEN 
        (fiscal_curr_year.Net_Sales = 0 OR fiscal_curr_year.Comp_Trans = 0) AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR fiscal_curr_year.Comp_Trans = 0
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
        fiscal_curr_year.Comp_Trans 
        END 
        as Comp_Trans_CY,
        fiscal_last_year.AGG_START_DT as Fisc_DATE_LY,
        fiscal_last_year.AGG_START_DT as FISC_START_DT_LY,
        fiscal_last_year.END_DT as FISC_END_DT_LY,
        fiscal_last_year.Net_Sales as Fisc_Net_Sales_LY,
        fiscal_last_year.Trans_Cnt as Fisc_Trans_LY,
        CASE WHEN 
        (
            fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (fiscal_last_year.Net_Sales = 0 OR fiscal_last_year.Comp_Sales = 0)
            AND NOT nvl(fiscal_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    fiscal_last_year.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(fiscal_last_year.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.FISC_1Y_SALES IS NOT NULL
                THEN fiscal_last_year.Comp_Sales - WP.FISC_1Y_SALES   --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.FISC_1Y_SALES IS NOT NULL
                THEN fiscal_last_year.Comp_Sales - WY.FISC_1Y_SALES   --ODI-57
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.FISC_1Y_SALES IS NOT NULL
                THEN fiscal_last_year.Comp_Sales - WQ.FISC_1Y_SALES   --ODI-57
                ELSE fiscal_last_year.Comp_Sales
            END
        END 
        as Fisc_Comp_Sales_LY,
        CASE WHEN 
        (fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (fiscal_last_year.Net_Sales = 0 OR fiscal_last_year.Comp_Trans = 0)
            AND NOT nvl(fiscal_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    fiscal_last_year.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(fiscal_last_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, fiscal_last_year.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.FISC_1Y_TRANS IS NOT NULL
                THEN fiscal_last_year.Comp_Trans - WP.FISC_1Y_TRANS  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.FISC_1Y_TRANS IS NOT NULL 
                THEN fiscal_last_year.Comp_Trans - WY.FISC_1Y_TRANS  --ODI-57
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.FISC_1Y_TRANS IS NOT NULL
                THEN fiscal_last_year.Comp_Trans - WQ.FISC_1Y_TRANS  --ODI-57
                ELSE fiscal_last_year.Comp_Trans
            END
        END 
        as Fisc_Comp_Trans_LY,
        calendar_last_year.AGG_START_DT as Cal_Comp_DATE_LY,
        calendar_last_year.AGG_START_DT as CAL_COMP_START_DT_LY,
        calendar_last_year.END_DT as CAL_COMP_END_DT_LY,
        calendar_last_year.Net_Sales as Cal_Net_Sales_LY,
        calendar_last_year.Trans_Cnt as Cal_Trans_LY,
        CASE WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL 
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (calendar_last_year.Net_Sales = 0 OR calendar_last_year.Comp_Sales = 0)
            AND NOT nvl(calendar_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    calendar_last_year.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_last_year.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_1Y_SALES IS NOT NULL
                THEN calendar_last_year.Comp_Sales - WP.CAL_1Y_SALES  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD''  AND WY.CAL_1Y_SALES IS NOT NULL
                THEN calendar_last_year.Comp_Sales - WY.CAL_1Y_SALES  --ODI-57
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_1Y_SALES IS NOT NULL
                THEN calendar_last_year.Comp_Sales - WQ.CAL_1Y_SALES  --ODI-57
                ELSE calendar_last_year.Comp_Sales
            END
        END as Cal_Comp_Sales_LY,
        CASE WHEN 
        (
            fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR 
        (calendar_last_year.Net_Sales = 0 OR calendar_last_year.Comp_Trans = 0)
            AND NOT nvl(calendar_last_year.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    calendar_last_year.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_last_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, calendar_last_year.WEEKDAY_OFFSET)
            WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_1Y_TRANS IS NOT NULL
            THEN calendar_last_year.Comp_Trans - WP.CAL_1Y_TRANS  --ODI-515
            WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_1Y_TRANS IS NOT NULL
            THEN calendar_last_year.Comp_Trans - WY.CAL_1Y_TRANS
            WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_1Y_TRANS IS NOT NULL
            THEN calendar_last_year.Comp_Trans - WQ.CAL_1Y_TRANS
            ELSE calendar_last_year.Comp_Trans
            END
        END as Cal_Comp_Trans_LY,
        calendar_two_years.AGG_START_DT as Cal_Comp_DATE_2Y,
        calendar_two_years.AGG_START_DT as CAL_COMP_START_DT_2Y,
        calendar_two_years.END_DT as CAL_COMP_END_DT_2Y,
        calendar_two_years.Net_Sales as Cal_Net_Sales_2Y,
        calendar_two_years.Trans_Cnt as Cal_Trans_2Y,
        CASE WHEN 
        (
            fiscal_curr_year.Net_Sales = 0 
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR   
        (calendar_two_years.Net_Sales = 0 OR calendar_two_years.Comp_Sales = 0)
            AND NOT nvl(calendar_two_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_two_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    calendar_two_years.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_two_years.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_2Y_SALES IS NOT NULL
                THEN calendar_two_years.Comp_Sales - WP.CAL_2Y_SALES  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_2Y_SALES IS NOT NULL
                THEN calendar_two_years.Comp_Sales - WY.CAL_2Y_SALES
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_2Y_SALES IS NOT NULL
                THEN calendar_two_years.Comp_Sales - WQ.CAL_2Y_SALES
                ELSE calendar_two_years.Comp_Sales
            END
        END 
        as Cal_Comp_Sales_2Y,
        CASE WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR
        (calendar_two_years.Net_Sales = 0 OR calendar_two_years.Comp_Trans = 0)
            AND NOT nvl(calendar_two_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_two_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    calendar_two_years.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_two_years.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, calendar_two_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_2Y_TRANS IS NOT NULL
                THEN calendar_two_years.Comp_Trans - WP.CAL_2Y_TRANS  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_2Y_TRANS IS NOT NULL
                THEN calendar_two_years.Comp_Trans - WY.CAL_2Y_TRANS
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_2Y_TRANS IS NOT NULL
                THEN calendar_two_years.Comp_Trans - WQ.CAL_2Y_TRANS
                ELSE calendar_two_years.Comp_Trans
            END
        END as Cal_Comp_Trans_2Y,
        calendar_three_years.AGG_START_DT as Cal_Comp_DATE_3Y,
        calendar_three_years.AGG_START_DT as CAL_COMP_START_DT_3Y,
        calendar_three_years.END_DT as CAL_COMP_END_DT_3Y,
        calendar_three_years.Net_Sales as Cal_Net_Sales_3Y,
        calendar_three_years.Trans_Cnt as Cal_Trans_3Y,
        CASE WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        )
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR
        (calendar_three_years.Net_Sales = 0 OR calendar_three_years.Comp_Sales = 0)
            AND NOT nvl(calendar_three_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_three_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    calendar_three_years.Comp_Sales - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_three_years.WEEK_NET_SALES, fiscal_curr_year.WEEK_NET_SALES, fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_3Y_SALES IS NOT NULL
                THEN calendar_three_years.Comp_Sales - WP.CAL_3Y_SALES  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_3Y_SALES IS NOT NULL
                THEN calendar_three_years.Comp_Sales - WY.CAL_3Y_SALES
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_3Y_SALES IS NOT NULL
                THEN calendar_three_years.Comp_Sales - WQ.CAL_3Y_SALES
                ELSE calendar_three_years.Comp_Sales
            END
        END as Cal_Comp_Sales_3Y,
        CASE WHEN 
        (
            fiscal_curr_year.Net_Sales = 0
            OR 
            fiscal_curr_year.Comp_Sales IS NULL
            OR 
            fiscal_curr_year.Comp_Sales = 0
        ) 
            AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
        OR
        (calendar_three_years.Net_Sales = 0 OR calendar_three_years.Comp_Trans = 0)
            AND NOT nvl(calendar_three_years.IS_XMAS_THANKSGIVING, false)
        OR NOT fiscal_curr_year.OPEN_15P_FLAG --ODI-53
        OR fiscal_curr_year.CLOSED_BEFORE_PERIOD_END_FLAG --ODI-55
        OR NOT calendar_three_years.OPEN_AFTER_HONEYMOON_FLAG -- ODI-199
        THEN 0 
        ELSE
            CASE
                WHEN --ODI-2977
                    fiscal_curr_year.AGG_LEVEL = ''WTD'' AND NOT nvl(fiscal_curr_year.IS_XMAS_THANKSGIVING, false)
                    AND ARRAY_INTERSECTION(fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET) IS NOT NULL
                    AND fiscal_curr_year.AGG_START_DT + 6 <> :BUSINESS_DATE
                THEN
                    calendar_three_years.Comp_Trans - IDH_DEV.SALES.EXCLUDE_FROM_ARRAY(calendar_three_years.WEEK_TRANS_CNTS, fiscal_curr_year.WEEK_TRANS_CNTS, fiscal_curr_year.WEEKDAY_OFFSET, calendar_three_years.WEEKDAY_OFFSET)
                WHEN fiscal_curr_year.AGG_LEVEL = ''PTD'' AND WP.CAL_3Y_TRANS IS NOT NULL
                THEN calendar_three_years.Comp_Trans  - WP.CAL_3Y_TRANS  --ODI-515
                WHEN fiscal_curr_year.AGG_LEVEL = ''YTD'' AND WY.CAL_3Y_TRANS IS NOT NULL
                THEN calendar_three_years.Comp_Trans  - WY.CAL_3Y_TRANS
                WHEN fiscal_curr_year.AGG_LEVEL = ''QTD'' AND WQ.CAL_3Y_TRANS IS NOT NULL
                THEN calendar_three_years.Comp_Trans  - WQ.CAL_3Y_TRANS
                ELSE calendar_three_years.Comp_Trans
            END
        END as Cal_Comp_Trans_3Y
        FROM 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL_without_threshold(:BUSINESS_DATE, ''CY'')) as fiscal_curr_year
        inner join COMP_CALENDAR dt
        on dt.CALENDAR_DT = fiscal_curr_year.business_dt
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL_without_threshold(:BUSINESS_DATE,''FISC1Y'')) as fiscal_last_year
        on dt.FISCAL_1YR_COMP_DT = fiscal_last_year.business_dt
        and fiscal_curr_year.store_id = fiscal_last_year.store_id
        and fiscal_curr_year.brand_id = fiscal_last_year.brand_id
        and fiscal_curr_year.AGG_LEVEL = fiscal_last_year.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL_without_threshold(:BUSINESS_DATE,''CAL1Y'')) as calendar_last_year
        on dt.CALENDAR_1YR_COMP_DT = calendar_last_year.business_dt
        and fiscal_curr_year.store_id = calendar_last_year.store_id
        and fiscal_curr_year.brand_id = calendar_last_year.brand_id
        and fiscal_curr_year.AGG_LEVEL = calendar_last_year.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL_without_threshold(:BUSINESS_DATE,''CAL2Y'')) as calendar_two_years
        on dt.CALENDAR_2YR_COMP_DT = calendar_two_years.business_dt
        and fiscal_curr_year.store_id = calendar_two_years.store_id
        and fiscal_curr_year.brand_id = calendar_two_years.brand_id
        and fiscal_curr_year.AGG_LEVEL = calendar_two_years.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_NO_COMP_FISCAL_without_threshold(:BUSINESS_DATE,''CAL3Y'')) as calendar_three_years
        on dt.CALENDAR_3YR_COMP_DT = calendar_three_years.business_dt
        and fiscal_curr_year.store_id = calendar_three_years.store_id
        and fiscal_curr_year.brand_id = calendar_three_years.brand_id
        and fiscal_curr_year.AGG_LEVEL = calendar_three_years.AGG_LEVEL
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_WEEKS_IN_PERIODS_without_threshold(:BUSINESS_DATE)) as WP
        on  fiscal_curr_year.store_id = WP.store_id
        and fiscal_curr_year.brand_id = WP.brand_id
        and fiscal_curr_year.AGG_START_DT = WP.FISCAL_PERIOD_START_DT
        AND fiscal_last_year.AGG_START_DT = WP.FISCAL_1YR_COMP_PERIOD_DT 
        AND calendar_last_year.AGG_START_DT = WP.CALENDAR_1YR_COMP_PERIOD_DT
        AND calendar_two_years.AGG_START_DT = NVL(WP.CALENDAR_2YR_COMP_PERIOD_DT,calendar_two_years.AGG_START_DT)
        AND calendar_three_years.AGG_START_DT = NVL(WP.CALENDAR_3YR_COMP_PERIOD_DT,calendar_three_years.AGG_START_DT)
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_WEEKS_IN_QUARTERS_without_threshold(:BUSINESS_DATE)) as WQ
        on   fiscal_curr_year.store_id = WQ.store_id
        and fiscal_curr_year.brand_id = WQ.brand_id
        and fiscal_curr_year.AGG_START_DT = WQ.FISCAL_QUARTER_START_DT
        AND fiscal_last_year.AGG_START_DT = WQ.FISCAL_1YR_COMP_QUARTER_DT 
        AND calendar_last_year.AGG_START_DT = WQ.CALENDAR_1YR_COMP_QUARTER_DT
        AND calendar_two_years.AGG_START_DT = NVL(WQ.CALENDAR_2YR_COMP_QUARTER_DT,calendar_two_years.AGG_START_DT)
        AND calendar_three_years.AGG_START_DT = NVL(WQ.CALENDAR_3YR_COMP_QUARTER_DT,calendar_three_years.AGG_START_DT)
        left outer join 
        table(IDH_DEV.SALES.FLASH_SUMMARY_COMP_WEEKS_IN_YEARS_without_threshold(:BUSINESS_DATE)) as WY
        on   fiscal_curr_year.store_id = WY.store_id
        and fiscal_curr_year.brand_id = WY.brand_id
        and fiscal_curr_year.AGG_START_DT = WY.FISCAL_YEAR_START_DT
        AND fiscal_last_year.AGG_START_DT = WY.FISCAL_1YR_COMP_YEAR_DT 
        AND calendar_last_year.AGG_START_DT = WY.CALENDAR_1YR_COMP_YEAR_DT
        AND calendar_two_years.AGG_START_DT = NVL(WY.CALENDAR_2YR_COMP_YEAR_DT,calendar_two_years.AGG_START_DT)
        AND calendar_three_years.AGG_START_DT = NVL(WY.CALENDAR_3YR_COMP_YEAR_DT,calendar_three_years.AGG_START_DT)
        left outer join 
        IDH_DEV.OPERATION.LOCATION_HIERARCHY_V lv 
        on  lv.LOCATION_ID = fiscal_curr_year.STORE_ID
        and lv.brand_id = fiscal_curr_year.brand_id
        order by 
        fiscal_curr_year.brand_id ASC, 
        fiscal_curr_year.store_id ASC,
        (case when fiscal_curr_year.AGG_LEVEL = ''DAY'' then 1
            when fiscal_curr_year.AGG_LEVEL = ''WTD'' then 2
            when fiscal_curr_year.AGG_LEVEL = ''PTD'' then 3
            when fiscal_curr_year.AGG_LEVEL = ''QTD'' then 4
            when fiscal_curr_year.AGG_LEVEL = ''YTD'' then 5 END) ASC,
        fiscal_curr_year.AGG_START_DT DESC

    );
    


    return table(flash_report);
END
';
CREATE FUNCTION IF NOT EXISTS SUMMARY_SALES("BUSINESS_DATE" VARCHAR(16777216))
RETURNS TABLE ("STORE_ID" VARCHAR(16777216), "NET_SALES_AMT" NUMBER(12,2), "TRANSACTION_CNT" NUMBER(38,0), "CALCULATIONTYPE" VARCHAR(16777216))
LANGUAGE SQL
AS '

SELECT
    dfsf.STORE_ID
    , SUM(dfsf.NET_SALES_AMT) AS NET_SALES_AMT
    , SUM(dfsf.TRANSACTION_CNT) AS TRANSACTION_CNT
    , ''WTD'' AS CalculationType
FROM
    IDS_DEV.DATASTORE.DAILY_FLASH_SALES_FACT dfsf
INNER JOIN
    IDH_DEV.SHARED.DATE_DIM_V d
ON  dfsf.BUSINESS_DT = d.CALENDAR_DT
WHERE
    dfsf.BRAND_ID = ''ARG''
AND dfsf.SOURCE_SYSTEM_NM = ''RTI''
AND NOT (dfsf.BRAND_ID = ''ARG'' AND dfsf.SOURCE_SYSTEM_NM <> ''RTI'')
GROUP BY
    dfsf.STORE_ID

';