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
SELECT STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.BRAND_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.BUSINESS_DT,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.STORE_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.ORDER_CHANNEL_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.MDM_PRODUCT_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.PRODUCT_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.DMA_CD,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.DMA_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.OWNERSHIP_TYP,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.PRODUCT_STANDARD_PRICE_AMT,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.SOURCE_SYSTEM_NM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.LOAD_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.LOAD_DTTM,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.UPDATE_ID,STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV.UPDATE_DTTM
FROM IDS_DEV.INT_REF_BV.STORE_CHANNEL_PRODUCT_DAILY_REFERENCE_PRICE_BV ;
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
SELECT ZIP_TO_DMA_BV.BRAND_ID,ZIP_TO_DMA_BV.ZIP_CD,ZIP_TO_DMA_BV.CITY_NM,ZIP_TO_DMA_BV.STATE_CD,ZIP_TO_DMA_BV.CNTRY_CD,ZIP_TO_DMA_BV.DMA_CD,ZIP_TO_DMA_BV.ZIP_DMA_NM,ZIP_TO_DMA_BV.SOURCE_SYSTEM_NM,ZIP_TO_DMA_BV.LOAD_ID,ZIP_TO_DMA_BV.LOAD_DTTM,ZIP_TO_DMA_BV.UPDATE_ID,ZIP_TO_DMA_BV.UPDATE_DTTM
FROM IDS_DEV.INT_REF_BV.ZIP_TO_DMA_BV ;