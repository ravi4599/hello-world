create TABLE IF NOT EXISTS CAMPAIGN (
	CAMPAIGN_KEY NUMBER(38,0) NOT NULL COMMENT 'Incremental value and primary key of the table.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand ',
	CAMPAIGN_NM VARCHAR(16777216) NOT NULL COMMENT '\t\nThe entire advertising effort conducted within a predetermined time frame, usually driven by a set of advertising goals. A campaign includes criteria such as media vehicle, markets, demos, dayparts, flight dates and spot lengths.',
	CAMPAIGN_DESC VARCHAR(16777216) COMMENT 'The entire advertising effort conducted within a predetermined time frame, usually driven by a set of advertising goals. A campaign includes criteria such as media vehicle, markets, demos, dayparts, flight dates and spot lengths.This provides more details.',
	STANDARD_CAMPAIGN_NM VARCHAR(16777216) COMMENT 'Brand level standard Campaign Name',
	STANDARD_CAMPAIGN_DESC VARCHAR(16777216) COMMENT 'Brand level standard Campaign Description',
	STANDARD_OBJECTIVE_TYP VARCHAR(16777216) COMMENT 'Standard Objective of the Campaign',
	MEDIA_TYP VARCHAR(16777216) COMMENT 'Type of Media like TV, Digital, Social/Search etc.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) NOT NULL COMMENT 'Source system name.',
	EFFECTIVE_BEGIN_DT DATE NOT NULL COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT DATE NOT NULL COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	CURRENT_IND NUMBER(1,0),
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM DATE NOT NULL COMMENT 'The Date/Datetime the Record was Inserted',
	constraint XAK1CAMPAIGN unique (CAMPAIGN_NM, BRAND_ID, EFFECTIVE_END_DT),
	constraint XPKCAMPAIGN primary key (CAMPAIGN_KEY)
)COMMENT='This table contains media campaign Information.'
;
create TABLE IF NOT EXISTS CHANNEL_HIERARCHY (
	CHANNEL_HIERARCHY_KEY NUMBER(38,0) NOT NULL COMMENT 'Incremental value and primary key of the table.',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand',
	CHANNEL_NM VARCHAR(16777216) NOT NULL COMMENT 'Type of Media or Network/ Station names.Values of Channel, Sub Channel, Sub Channel 2',
	CHANNEL_CATEGORY_NM VARCHAR(16777216) NOT NULL COMMENT 'Type of Media or Network/ Station names.Values of Channel, Sub Channel, Sub Channel 2',
	CHANNEL_SUBCATEGORY_NM VARCHAR(16777216) NOT NULL COMMENT 'Type of Media or Network/ Station names.Values of Channel, Sub Channel, Sub Channel 2',
	PARTNER_NM VARCHAR(16777216) NOT NULL COMMENT 'Partner Name, Level 4 data.Values of Partner.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) NOT NULL COMMENT 'Source system name.',
	EFFECTIVE_BEGIN_DT DATE NOT NULL COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT DATE NOT NULL COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	CURRENT_IND NUMBER(1,0),
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'The Date/Datetime the Record was Inserted',
	constraint XAK1CHANNELHIERARCHY unique (CHANNEL_NM, CHANNEL_SUBCATEGORY_NM, CHANNEL_CATEGORY_NM, PARTNER_NM, EFFECTIVE_END_DT, BRAND_ID),
	constraint XPKCHANNELHIERARCHY primary key (CHANNEL_HIERARCHY_KEY)
)COMMENT='This table contains Channel hierarchy details.'
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
create TABLE IF NOT EXISTS MEDIA_ACTIVITY_FACT (
	CAMPAIGN_KEY NUMBER(38,0) NOT NULL COMMENT 'Incremental value and primary key of the table.',
	STRATEGY_KEY NUMBER(38,0) NOT NULL COMMENT 'Incremental value and primary key of the table.',
	CHANNEL_HIERARCHY_KEY NUMBER(38,0) NOT NULL COMMENT 'Incremental value and primary key of the table.',
	DMA_CD VARCHAR(16777216) NOT NULL COMMENT 'DMA Code provided by Nielsen',
	WEEK_START_DT DATE NOT NULL COMMENT 'Start of the Week',
	BRAND_ID VARCHAR(38) NOT NULL,
	SPEND_TYPE_KEY NUMBER(38,0) COMMENT 'Auto-Incremental',
	TRP_CNT NUMBER(38,2) COMMENT 'This is the Target Rating Point of the Program or Advert',
	GRP_CNT NUMBER(38,2) COMMENT 'This is the Gross Rating Point of the Program or Advert',
	CLICKS_CNT NUMBER(38,2) COMMENT 'Number of Clicks',
	SPEND_AMT NUMBER(38,6) COMMENT 'Amount Spent.',
	CURRENCY_CD VARCHAR(16777216) COMMENT 'Currency Code',
	IMPRESSIONS_CNT NUMBER(38,2) COMMENT 'Number of views',
	RATINGS_CNT NUMBER(38,2) COMMENT 'Rating of a program',
	SOURCE_SYSTEM_NM VARCHAR(16777216) NOT NULL COMMENT 'Source system name.',
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'The Date/Datetime the Record was Inserted',
	constraint XAK1MEDIAACTIVITYFACT unique (WEEK_START_DT, DMA_CD, BRAND_ID),
	constraint XPKMEDIAACTIVITYFACT primary key (CAMPAIGN_KEY, STRATEGY_KEY, CHANNEL_HIERARCHY_KEY, DMA_CD, WEEK_START_DT),
	constraint SPENDTYPE_TO_MEDIAACTIVITYFACT foreign key (SPEND_TYPE_KEY) references SPEND_TYPE(SPEND_TYPE_KEY),
	constraint CAMPAIGN_TO_MEDIAACTIVITYFACT foreign key (CAMPAIGN_KEY) references CAMPAIGN(CAMPAIGN_KEY),
	constraint CHANNELHIERARCHY_TO_MEDIAACTIVITYFACT foreign key (CHANNEL_HIERARCHY_KEY) references CHANNEL_HIERARCHY(CHANNEL_HIERARCHY_KEY),
	constraint STRATEGY_TO_MEDIAACTIVITYFACT foreign key (STRATEGY_KEY) references STRATEGY(STRATEGY_KEY)
)COMMENT='This table contains Media Spend amount information.This table contains media activity information like Impressions, Clicks'
;
create TABLE IF NOT EXISTS SPEND_TYPE (
	SPEND_TYPE_KEY NUMBER(38,0) NOT NULL COMMENT 'Auto-Incremental',
	BRAND_ID VARCHAR(16777216) NOT NULL,
	SPEND_TYP_NM VARCHAR(16777216) NOT NULL COMMENT 'Auto-Incremental',
	SPEND_TYPE_DESC VARCHAR(16777216) COMMENT 'Spend Type Description',
	SOURCE_SYSTEM_NM VARCHAR(16777216) NOT NULL COMMENT 'Source system name.',
	EFFECTIVE_BEGIN_DT DATE NOT NULL COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT DATE NOT NULL COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	CURRENT_IND NUMBER(1,0),
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM DATE NOT NULL COMMENT 'The Date/Datetime the Record was Inserted',
	constraint XAK1SPENDTYPE unique (SPEND_TYP_NM, BRAND_ID, EFFECTIVE_END_DT),
	constraint XPKSPENDTYPE primary key (SPEND_TYPE_KEY)
)COMMENT='This table contains Media Spend typeinformation.'
;
create TABLE IF NOT EXISTS STRATEGY (
	STRATEGY_KEY NUMBER(38,0) NOT NULL COMMENT 'Incremental value and primary key of the table.',
	BRAND_ID VARCHAR(16777216) COMMENT 'Brand ',
	STRATEGY_NM VARCHAR(16777216) NOT NULL COMMENT 'Contains Objective, Message, Ad information.',
	STRATEGY_TYPE_ID VARCHAR(16777216) COMMENT 'Defines if the strategy is Objective/Message/Ad type.',
	STRATEGY_TYPE_NM VARCHAR(16777216) NOT NULL COMMENT 'Type of Strategy : Objective / Message / Ad / Objective+Message',
	STRATEGY_TYPE_DESC VARCHAR(16777216) COMMENT 'Type of Strategy : Objective / Message / Ad / Objective+Message. This Provides more details.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) COMMENT 'Source system name.',
	EFFECTIVE_BEGIN_DT DATE NOT NULL COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT DATE NOT NULL COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	CURRENT_IND NUMBER(1,0),
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM DATE NOT NULL COMMENT 'The Date/Datetime the Record was Inserted',
	constraint XAK1STRATEGY unique (STRATEGY_NM, BRAND_ID, EFFECTIVE_END_DT),
	constraint XPKSTRATEGY primary key (STRATEGY_KEY)
)COMMENT='This table contains Objective/ad information'
;