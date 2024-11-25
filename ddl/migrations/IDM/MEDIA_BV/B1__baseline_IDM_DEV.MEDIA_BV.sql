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
create view IF NOT EXISTS CAMPAIGN_BV(
	CAMPAIGN_KEY COMMENT 'Incremental value and primary key of the table.',
	CAMPAIGN_NM COMMENT 'The entire advertising effort conducted within a predetermined time frame, usually driven by a set of advertising goals. A campaign includes criteria such as media vehicle, markets, demos, dayparts, flight dates and spot lengths.',
	CAMPAIGN_DESC COMMENT 'The entire advertising effort conducted within a predetermined time frame, usually driven by a set of advertising goals. A campaign includes criteria such as media vehicle, markets, demos, dayparts, flight dates and spot lengths.This provides more details.',
	BRAND_ID COMMENT 'Brand ',
	SOURCE_SYSTEM_NM COMMENT 'Source system name.',
	EFFECTIVE_BEGIN_DT COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	CURRENT_IND,
	STANDARD_CAMPAIGN_NM COMMENT 'Brand level standard Campaign Name',
	STANDARD_CAMPAIGN_DESC COMMENT 'Brand level standard Campaign Description',
	STANDARD_OBJECTIVE_TYP COMMENT 'Standard Objective of the Campaign',
	MEDIA_TYP COMMENT 'Type of Media like TV, Digital, Social/Search etc.'
) COMMENT='This table contains media campaign Information.'
 as 
SELECT CAMPAIGN.CAMPAIGN_KEY,CAMPAIGN.CAMPAIGN_NM,CAMPAIGN.CAMPAIGN_DESC,CAMPAIGN.BRAND_ID,CAMPAIGN.SOURCE_SYSTEM_NM,CAMPAIGN.EFFECTIVE_BEGIN_DT,CAMPAIGN.EFFECTIVE_END_DT,CAMPAIGN.LOAD_ID,CAMPAIGN.LAST_UPDATED_DTTM,CAMPAIGN.CURRENT_IND,CAMPAIGN.STANDARD_CAMPAIGN_NM,CAMPAIGN.STANDARD_CAMPAIGN_DESC,CAMPAIGN.STANDARD_OBJECTIVE_TYP,CAMPAIGN.MEDIA_TYP
FROM IDM_DEV.MEDIA.CAMPAIGN ;
create view IF NOT EXISTS CHANNEL_HIERARCHY_BV(
	CHANNEL_HIERARCHY_KEY COMMENT 'Incremental value and primary key of the table.',
	CHANNEL_NM COMMENT 'Type of Media or Network/ Station names.Values of Channel, Sub Channel, Sub Channel 2',
	BRAND_ID COMMENT 'Brand',
	PARTNER_NM COMMENT 'Partner Name, Level 4 data.Values of Partner.',
	CHANNEL_CATEGORY_NM COMMENT 'Type of Media or Network/ Station names.Values of Channel, Sub Channel, Sub Channel 2',
	CHANNEL_SUBCATEGORY_NM COMMENT 'Type of Media or Network/ Station names.Values of Channel, Sub Channel, Sub Channel 2',
	EFFECTIVE_BEGIN_DT COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	CURRENT_IND,
	SOURCE_SYSTEM_NM COMMENT 'Source system name.'
) COMMENT='This table contains Channel hierarchy details.'
 as 
SELECT CHANNEL_HIERARCHY.CHANNEL_HIERARCHY_KEY,CHANNEL_HIERARCHY.CHANNEL_NM,CHANNEL_HIERARCHY.BRAND_ID,CHANNEL_HIERARCHY.PARTNER_NM,CHANNEL_HIERARCHY.CHANNEL_CATEGORY_NM,CHANNEL_HIERARCHY.CHANNEL_SUBCATEGORY_NM,CHANNEL_HIERARCHY.EFFECTIVE_BEGIN_DT,CHANNEL_HIERARCHY.EFFECTIVE_END_DT,CHANNEL_HIERARCHY.LOAD_ID,CHANNEL_HIERARCHY.LAST_UPDATED_DTTM,CHANNEL_HIERARCHY.CURRENT_IND,CHANNEL_HIERARCHY.SOURCE_SYSTEM_NM
FROM IDM_DEV.MEDIA.CHANNEL_HIERARCHY ;
create view IF NOT EXISTS MEDIA_ACTIVITY_FACT_BV(
	CHANNEL_HIERARCHY_KEY COMMENT 'Incremental value and primary key of the table.',
	WEEK_START_DT COMMENT 'Start of the Week',
	SPEND_AMT COMMENT 'Amount Spent.',
	CURRENCY_CD COMMENT 'Currency Code',
	BRAND_ID,
	DMA_CD COMMENT 'DMA Code provided by Nielsen',
	IMPRESSIONS_CNT COMMENT 'Number of views',
	RATINGS_CNT COMMENT 'Rating of a program',
	CLICKS_CNT COMMENT 'Number of Clicks',
	GRP_CNT COMMENT 'This is the Gross Rating Point of the Program or Advert',
	TRP_CNT COMMENT 'This is the Target Rating Point of the Program or Advert',
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	SOURCE_SYSTEM_NM COMMENT 'Source system name.',
	SPEND_TYPE_KEY COMMENT 'Auto-Incremental',
	STRATEGY_KEY COMMENT 'Incremental value and primary key of the table.',
	CAMPAIGN_KEY COMMENT 'Incremental value and primary key of the table.'
) COMMENT='This table contains Media Spend amount information.This table contains media activity information like Impressions, Clicks.'
 as 
SELECT MEDIA_ACTIVITY_FACT.CHANNEL_HIERARCHY_KEY,MEDIA_ACTIVITY_FACT.WEEK_START_DT,MEDIA_ACTIVITY_FACT.SPEND_AMT,MEDIA_ACTIVITY_FACT.CURRENCY_CD,MEDIA_ACTIVITY_FACT.BRAND_ID,MEDIA_ACTIVITY_FACT.DMA_CD,MEDIA_ACTIVITY_FACT.IMPRESSIONS_CNT,MEDIA_ACTIVITY_FACT.RATINGS_CNT,MEDIA_ACTIVITY_FACT.CLICKS_CNT,MEDIA_ACTIVITY_FACT.GRP_CNT,MEDIA_ACTIVITY_FACT.TRP_CNT,MEDIA_ACTIVITY_FACT.LOAD_ID,MEDIA_ACTIVITY_FACT.LAST_UPDATED_DTTM,MEDIA_ACTIVITY_FACT.SOURCE_SYSTEM_NM,MEDIA_ACTIVITY_FACT.SPEND_TYPE_KEY,MEDIA_ACTIVITY_FACT.STRATEGY_KEY,MEDIA_ACTIVITY_FACT.CAMPAIGN_KEY
FROM IDM_DEV.MEDIA.MEDIA_ACTIVITY_FACT ;
create view IF NOT EXISTS SPEND_TYPE_BV(
	SPEND_TYPE_KEY COMMENT 'Auto-Incremental',
	SPEND_TYP_NM COMMENT 'Auto-Incremental',
	SPEND_TYPE_DESC COMMENT 'Spend Type Description',
	BRAND_ID,
	EFFECTIVE_BEGIN_DT COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	SOURCE_SYSTEM_NM COMMENT 'Source system name.',
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	CURRENT_IND
) COMMENT='This table contains Media Spend typeinformation.'
 as 
SELECT SPEND_TYPE.SPEND_TYPE_KEY,SPEND_TYPE.SPEND_TYP_NM,SPEND_TYPE.SPEND_TYPE_DESC,SPEND_TYPE.BRAND_ID,SPEND_TYPE.EFFECTIVE_BEGIN_DT,SPEND_TYPE.EFFECTIVE_END_DT,SPEND_TYPE.SOURCE_SYSTEM_NM,SPEND_TYPE.LOAD_ID,SPEND_TYPE.LAST_UPDATED_DTTM,SPEND_TYPE.CURRENT_IND
FROM IDM_DEV.MEDIA.SPEND_TYPE ;
create view IF NOT EXISTS STRATEGY_BV(
	STRATEGY_KEY COMMENT 'Incremental value and primary key of the table.',
	STRATEGY_NM COMMENT 'Contains Objective, Message, Ad information.',
	STRATEGY_TYPE_ID COMMENT 'Defines if the strategy is Objective/Message/Ad type.',
	BRAND_ID COMMENT 'Brand ',
	SOURCE_SYSTEM_NM COMMENT 'Source system name.',
	STRATEGY_TYPE_NM COMMENT 'Type of Strategy : Objective / Message / Ad / Objective+Message',
	STRATEGY_TYPE_DESC COMMENT 'Type of Strategy : Objective / Message / Ad / Objective+Message. This Provides more details.',
	EFFECTIVE_BEGIN_DT COMMENT 'This is the date the Record Became Active',
	EFFECTIVE_END_DT COMMENT 'This is the date the Record Expires. Default is 12/31/2099',
	LOAD_ID COMMENT 'This is the User Identifier of the Person or Process that Loaded the Table',
	LAST_UPDATED_DTTM COMMENT 'The Date/Datetime the Record was Inserted',
	CURRENT_IND
) COMMENT='This table contains Objective/ad information.'
 as 
SELECT STRATEGY.STRATEGY_KEY,STRATEGY.STRATEGY_NM,STRATEGY.STRATEGY_TYPE_ID,STRATEGY.BRAND_ID,STRATEGY.SOURCE_SYSTEM_NM,STRATEGY.STRATEGY_TYPE_NM,STRATEGY.STRATEGY_TYPE_DESC,STRATEGY.EFFECTIVE_BEGIN_DT,STRATEGY.EFFECTIVE_END_DT,STRATEGY.LOAD_ID,STRATEGY.LAST_UPDATED_DTTM,STRATEGY.CURRENT_IND
FROM IDM_DEV.MEDIA.STRATEGY ;