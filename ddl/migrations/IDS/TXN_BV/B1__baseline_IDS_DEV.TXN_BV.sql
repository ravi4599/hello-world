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
create TABLE IF NOT EXISTS MY_TABLE_VP (
	MY_COLUMN VARCHAR(16777216) COMMENT 'this is comment3'
);
create TABLE IF NOT EXISTS TESTCOMMENT (
	ID NUMBER(38,0),
	NAME VARCHAR(16777216),
	LOCATION VARCHAR(16777216) COMMENT 'Location in table'
);
create view IF NOT EXISTS AUDITED_DAILY_SALES_BV(
	STORE_ID COMMENT 'Number that uniquely identifies the store',
	BUSINESS_DT COMMENT 'Date of sales',
	BRAND_ID COMMENT 'Identifier for store brand',
	TRANSACTION_CNT COMMENT 'Count of sales transactions',
	NET_SALES_AMT COMMENT 'Net amount of sales',
	LOAD_TYP COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.'
) as 
SELECT AUDITED_DAILY_SALES.STORE_ID,AUDITED_DAILY_SALES.BUSINESS_DT,AUDITED_DAILY_SALES.BRAND_ID,AUDITED_DAILY_SALES.TRANSACTION_CNT,AUDITED_DAILY_SALES.NET_SALES_AMT,AUDITED_DAILY_SALES.LOAD_TYP,AUDITED_DAILY_SALES.SOURCE_SYSTEM_NM,AUDITED_DAILY_SALES.LOAD_ID,AUDITED_DAILY_SALES.LOAD_DTTM,AUDITED_DAILY_SALES.UPDATE_ID,AUDITED_DAILY_SALES.UPDATE_DTTM
FROM IDS_DEV.TXN.AUDITED_DAILY_SALES ;
create view IF NOT EXISTS DAILY_AUDIT_SALES_MAPPING_BV(
	STORE_ID COMMENT 'Number that uniquely identifies the store',
	BUSINESS_DT COMMENT 'Date of sales',
	BRAND_ID COMMENT 'Identifier for store brand',
	TRANSACTION_CNT COMMENT 'Count of sales transactions',
	NET_SALES_AMT COMMENT 'Net amount of sales',
	LOAD_TYP COMMENT 'Indicates whether data is part of a large historical data pull or was added from a smaller data pull (daily, weekly etc.)',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. ',
	LOAD_ID COMMENT 'Load Batch Identifier specifies the Batch that inserted the record into the table',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. ',
	UPDATE_ID COMMENT 'Update Identifier specifies the User that updated data. For batch load, it is service account. For a manual insert, it is individual user. Specifies the load program or account that last modified the record.\t',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.'
) as 
SELECT DAILY_AUDIT_SALES_MAPPING.STORE_ID,DAILY_AUDIT_SALES_MAPPING.BUSINESS_DT,DAILY_AUDIT_SALES_MAPPING.BRAND_ID,DAILY_AUDIT_SALES_MAPPING.TRANSACTION_CNT,DAILY_AUDIT_SALES_MAPPING.NET_SALES_AMT,DAILY_AUDIT_SALES_MAPPING.LOAD_TYP,DAILY_AUDIT_SALES_MAPPING.SOURCE_SYSTEM_NM,DAILY_AUDIT_SALES_MAPPING.LOAD_ID,DAILY_AUDIT_SALES_MAPPING.LOAD_DTTM,DAILY_AUDIT_SALES_MAPPING.UPDATE_ID,DAILY_AUDIT_SALES_MAPPING.UPDATE_DTTM
FROM IDS_DEV.TXN.DAILY_AUDIT_SALES_MAPPING ;
create view IF NOT EXISTS DAILY_DAY_PART_FLASH_SALES_FACT_BV(
	LOAD_TYP,
	NET_SALES_AMT,
	BRAND_DAY_PART_NM,
	UPDATE_DTTM,
	TRX_CNT,
	BUSINESS_DT_KEY,
	SOURCE_SYSTEM_NM,
	LOAD_DTTM,
	IRB_DAY_PART_NM,
	BUSINESS_DT,
	CHANNEL_ID,
	STORE_ID,
	CHANNEL_KEY,
	LOAD_ID,
	STORE_KEY,
	UPDATE_ID,
	BRAND_ID,
	CLOSED_HR
) as SELECT
                               LOAD_TYP, NET_SALES_AMT, BRAND_DAY_PART_NM, UPDATE_DTTM, TRX_CNT, BUSINESS_DT_KEY, SOURCE_SYSTEM_NM, LOAD_DTTM, IRB_DAY_PART_NM, BUSINESS_DT, CHANNEL_ID, STORE_ID, CHANNEL_KEY, LOAD_ID, STORE_KEY, UPDATE_ID, BRAND_ID, CLOSED_HR
                            FROM IDS_DEV.DATASTORE."DAILY_DAY_PART_FLASH_SALES_FACT";
create view IF NOT EXISTS DAILY_FLASH_SALES_FACT_BKP_20211018_BV(
	UPDATE_ID,
	BUSINESS_DT,
	LOAD_TYP,
	STORE_ID,
	STORE_KEY,
	BRAND_ID,
	NET_SALES_AMT,
	LOAD_DTTM,
	LOAD_ID,
	UPDATE_DTTM,
	TRANSACTION_CNT,
	BUSINESS_DATE_KEY,
	SOURCE_SYSTEM_NM
) as SELECT
                               UPDATE_ID, BUSINESS_DT, LOAD_TYP, STORE_ID, STORE_KEY, BRAND_ID, NET_SALES_AMT, LOAD_DTTM, LOAD_ID, UPDATE_DTTM, TRANSACTION_CNT, BUSINESS_DATE_KEY, SOURCE_SYSTEM_NM
                            FROM IDS_DEV.DATASTORE."DAILY_FLASH_SALES_FACT_BKP_20211018";
create view IF NOT EXISTS DAILY_FLASH_SALES_FACT_BV(
	STORE_ID,
	STORE_KEY,
	UPDATE_ID,
	BUSINESS_DT,
	LOAD_TYP,
	UPDATE_DTTM,
	BRAND_ID,
	NET_SALES_AMT,
	BUSINESS_DATE_KEY,
	SOURCE_SYSTEM_NM,
	LOAD_ID,
	LOAD_DTTM,
	TRANSACTION_CNT
) as SELECT
                               STORE_ID, STORE_KEY, UPDATE_ID, BUSINESS_DT, LOAD_TYP, UPDATE_DTTM, BRAND_ID, NET_SALES_AMT, BUSINESS_DATE_KEY, SOURCE_SYSTEM_NM, LOAD_ID, LOAD_DTTM, TRANSACTION_CNT
                            FROM IDS_DEV.DATASTORE."DAILY_FLASH_SALES_FACT";
create view IF NOT EXISTS DAILY_FLASH_SALES_FACT_TST1_BV(
	TRANSACTION_CNT,
	LOAD_TYP,
	NET_SALES_AMT,
	STORE_ID,
	UPDATE_ID,
	BUSINESS_DT,
	UPDATE_DTTM,
	BUSINESS_DATE_KEY,
	SOURCE_SYSTEM_NM,
	BRAND_ID,
	LOAD_ID,
	STORE_KEY,
	LOAD_DTTM
) as SELECT
                               TRANSACTION_CNT, LOAD_TYP, NET_SALES_AMT, STORE_ID, UPDATE_ID, BUSINESS_DT, UPDATE_DTTM, BUSINESS_DATE_KEY, SOURCE_SYSTEM_NM, BRAND_ID, LOAD_ID, STORE_KEY, LOAD_DTTM
                            FROM IDS_DEV.DATASTORE."DAILY_FLASH_SALES_FACT_TST1";
create view IF NOT EXISTS DAILY_FLASH_SALES_FACT_TST2_BV(
	LOAD_ID,
	TRANSACTION_CNT,
	UPDATE_ID,
	STORE_ID,
	SOURCE_SYSTEM_NM,
	UPDATE_DTTM,
	BUSINESS_DATE_KEY,
	LOAD_DTTM,
	LOAD_TYP,
	NET_SALES_AMT,
	BRAND_ID,
	BUSINESS_DT,
	STORE_KEY
) as SELECT
                               LOAD_ID, TRANSACTION_CNT, UPDATE_ID, STORE_ID, SOURCE_SYSTEM_NM, UPDATE_DTTM, BUSINESS_DATE_KEY, LOAD_DTTM, LOAD_TYP, NET_SALES_AMT, BRAND_ID, BUSINESS_DT, STORE_KEY
                            FROM IDS_DEV.DATASTORE."DAILY_FLASH_SALES_FACT_TST2";
create view IF NOT EXISTS DAILY_FLASH_SALES_FACT_TST_BV(
	BUSINESS_DT,
	TRANSACTION_CNT,
	LOAD_DTTM,
	UPDATE_ID,
	LOAD_ID,
	STORE_KEY,
	UPDATE_DTTM,
	STORE_ID,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_TYP,
	NET_SALES_AMT,
	BUSINESS_DATE_KEY
) as SELECT
                               BUSINESS_DT, TRANSACTION_CNT, LOAD_DTTM, UPDATE_ID, LOAD_ID, STORE_KEY, UPDATE_DTTM, STORE_ID, BRAND_ID, SOURCE_SYSTEM_NM, LOAD_TYP, NET_SALES_AMT, BUSINESS_DATE_KEY
                            FROM IDS_DEV.DATASTORE."DAILY_FLASH_SALES_FACT_TST";
create view IF NOT EXISTS DIGITAL_ORDER_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BUSINESS_DTTM COMMENT 'Business Datetime is the day and time that location has received the order.',
	ORDER_CHANNEL_ID COMMENT 'Order Channel Identifier is a unique identifier that specifies a unique brand and channel combination. The data is sourced from MDM. For example, Order Channel Id=cc9d59ae59ba8477b59b220163520c4f, Source Order Channel Code=curbside, Fulfillment Channel Code=curbside.',
	ORDER_CHANNEL_CD COMMENT 'Source Order Channel Code specifies how an order was submitted to a location. For example, WEB for browser based orders or IOSAPP for order submitted via iphone or ipad.',
	FULFILLMENT_CHANNEL_CD COMMENT 'Source Fulfillment Channel Code specifies how an orderfulfilled.For example, Curbside or Pickup.',
	FULFILLMENT_CHANNEL_SUB_CD COMMENT 'Fulfillment Channel Type Code adds additional context on the delivery of an order. Sample values, ASAP, Now, and Future.',
	ORDER_IDEMPOTENT_ID COMMENT 'Order Idempotent Identifier . Sample Value: c8880f45-0bd3-4198-bed0-49c18ec9d780.',
	ORDER_STATUS_CD COMMENT 'Order Status Code . Sample Value: POS_SUBMITTED.',
	ORDER_DISPLAY_STATUS_TXT COMMENT 'Order Display Status Text. Sample Value: Order Received.',
	ORDER_NM COMMENT 'Order Name . Sample Value: PHONE NOT SPECIFIED .',
	ORDER_EXPECTED_PICKUP_DTTM COMMENT 'Order Expected Pickup Datetime. Sample Value: 2021-07-09T11:00:00-05:00.',
	ORDER_FULFILLMENT_DTTM COMMENT 'Order Fulfillment Datetime . Sample Value: 2021-07-09T11:00:00-05:00.',
	POS_ORDER_ID COMMENT 'Pos Order Identifier is used to link a digitial order to the pos within a store. Sample Value: 3730.',
	POS_STATUS_CD COMMENT 'Pos Status Code. Sample Value: Success.',
	POS_RESULT_CD COMMENT 'Pos Result Code . Sample Value: 0.',
	POS_MESSAGE_TXT COMMENT 'Pos Message Text. Sample Value: Success.',
	ORDER_SUB_TOTAL_AMT COMMENT 'Order Sub Total Amount. Sample Value: 9.99.',
	ORDER_TOTAL_TAX_AMT COMMENT 'Order Total Tax Amount. Sample Value: 0.71.',
	ORDER_TIP_AMT COMMENT 'Order Tip Amount. Sample Value: 0.',
	ORDER_DISCOUNT_AMT COMMENT 'Order Discount Amount. Sample Value: 2.79.',
	ORDER_DISCOUNT_PRICE_AMT,
	ORDER_TOTAL_AMT COMMENT 'Order Total Price Amount is the summation of the Order Sub Total, Tax, and Tip Amount. Sample Value: 10.70.',
	ORDER_DETAIL_TXT COMMENT 'Order Detail Text . Sample Value: Confirmed Order.',
	DELIVERY_FEE_AMOUUNT,
	SERVER_EMPLOYEE_ID,
	TABLE_NBR,
	GUEST_CNT,
	CHECK_NBR,
	DELIVERY_DTTM,
	PAYMENT_URL,
	TALLY_TM,
	IDP_CUST_ID COMMENT 'Customer Identifier uniquely identifies on an order. Values may vary based on channel. Sample Value: ad90c54a-06cc-4dd9-b3cc-734e63a2b021.',
	CUST_MEMBERSHIP_NBR,
	CUST_FIRST_NM COMMENT 'Customer First Name . Sample Value: Mikhail.',
	CUST_LAST_NM COMMENT 'Customer Last Name . Sample Value: Krestelev.',
	CUST_PHONE_NBR COMMENT 'Customer Phone Number. Sample Value: 2233222232.',
	CUST_EMAIL COMMENT 'Customer Email . Sample Value: mkrestelev@inspirebrands.com.',
	CUST_ADR_LINE_1_TXT COMMENT 'Customer Address Line 1 Text. Sample Value: 5500 Wayzata Blvd..',
	CUST_ADR_LINE_2_TXT COMMENT 'Customer Address Line 2 Text. Sample Value: Ste 13.',
	CUST_CTY_NM COMMENT 'Customer City Name. Sample Value: Minneapolis.',
	CUST_ST_CD COMMENT 'Customer State Code. Sample Value: MN.',
	CUST_ZIP_CD COMMENT 'Customer Zip Code . Sample Value: 55416.',
	LOC_NM COMMENT 'Location Name . Sample Value: Buzztime QA 6.7.',
	LOC_PHONE_NBR COMMENT 'Location Phone Number. Sample Value: 612-866-9316.',
	LOC_EMAIL COMMENT 'Customer Email . Sample Value: mkrestelev@inspirebrands.com.',
	LOC_ADR_LINE_1_TXT COMMENT 'Location Address Line 1 Text. Sample Value: 5500 Wayzata Blvd..',
	LOC_ADR_LINE_2_TXT COMMENT 'Location Address Line 2 Text. Sample Value: Ste 13.',
	LOC_CTY_NM COMMENT 'Location City Name. Sample Value: Minneapolis.',
	LOC_ST_CD COMMENT 'Location State Code. Sample Value: MN.',
	LOC_ZIP_CD COMMENT 'Location Zip Code . Sample Value: 55416.',
	LOC_FRAUD_CHECK_IND,
	ORDER_CORRELATION_ID COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	FULLFILLMENT_UNIQUE_IDENTIFER,
	FULFILLMENT_ID,
	FULFILLMENT_TYP_CD,
	FULFILLMENT_STATUS_URL,
	DELIVERY_CONTACT_FIRST_NM COMMENT 'Delivery Contact First Name . Sample Value: John.',
	DELIVERY_CONTACT_LAST_NM COMMENT 'Delivery Contact Last Name . Sample Value: Wick.',
	DELIVERY_CONTACT_PHONE_NBR COMMENT 'Delivery Contact Phone Number. Sample Value: 15012323767.',
	DELIVERY_CONTACT_EMAIL COMMENT 'Delivery Contact Email . Sample Value: mkrestelev@inspirebrands.com.',
	DELIVERY_ADR_LINE_1_TXT COMMENT 'Delivery Address Line 1 Text. Sample Value: 5500 Wayzata Blvd..',
	DELIVERY_ADR_LINE_2_TXT COMMENT 'Delivery Address Line 2 Text. Sample Value: Ste 13.',
	DELIVERY_CTY_NM COMMENT 'Delivery City Name. Sample Value: Minneapolis.',
	DELIVERY_ST_CD COMMENT 'Delivery State Code. Sample Value: MN.',
	DELIVERY_ZIP_CD COMMENT 'Delivery Zip Code . Sample Value: 55416.',
	DELIVERY_PARTNER_NM COMMENT 'Delivery Partner Name. Sample Value: DoorDash.',
	DELIVERY_VEHICLE_DESC,
	DELIVERY_STATUS_TXT,
	CUST_FULFILLMENT_INSTRUCTION_TXT,
	ORDER_METADATA,
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CUST_PICKUP_INSTRUCTION_TXT
) COMMENT='Digital Order contains an order for a location that was placed via a digital means, such as phone or web browser.'
 as 
SELECT DIGITAL_ORDER.BRAND_ID,DIGITAL_ORDER.BUSINESS_DT,DIGITAL_ORDER.DIGITAL_ORDER_ID,DIGITAL_ORDER.LOC_ID,DIGITAL_ORDER.SOURCE_SYSTEM_NM,DIGITAL_ORDER.BUSINESS_DTTM,DIGITAL_ORDER.ORDER_CHANNEL_ID,DIGITAL_ORDER.ORDER_CHANNEL_CD,DIGITAL_ORDER.FULFILLMENT_CHANNEL_CD,DIGITAL_ORDER.FULFILLMENT_CHANNEL_SUB_CD,DIGITAL_ORDER.ORDER_IDEMPOTENT_ID,DIGITAL_ORDER.ORDER_STATUS_CD,DIGITAL_ORDER.ORDER_DISPLAY_STATUS_TXT,DIGITAL_ORDER.ORDER_NM,DIGITAL_ORDER.ORDER_EXPECTED_PICKUP_DTTM,DIGITAL_ORDER.ORDER_FULFILLMENT_DTTM,DIGITAL_ORDER.POS_ORDER_ID,DIGITAL_ORDER.POS_STATUS_CD,DIGITAL_ORDER.POS_RESULT_CD,DIGITAL_ORDER.POS_MESSAGE_TXT,DIGITAL_ORDER.ORDER_SUB_TOTAL_AMT,DIGITAL_ORDER.ORDER_TOTAL_TAX_AMT,DIGITAL_ORDER.ORDER_TIP_AMT,DIGITAL_ORDER.ORDER_DISCOUNT_AMT,DIGITAL_ORDER.ORDER_DISCOUNT_PRICE_AMT,DIGITAL_ORDER.ORDER_TOTAL_AMT,DIGITAL_ORDER.ORDER_DETAIL_TXT,DIGITAL_ORDER.DELIVERY_FEE_AMOUUNT,DIGITAL_ORDER.SERVER_EMPLOYEE_ID,DIGITAL_ORDER.TABLE_NBR,DIGITAL_ORDER.GUEST_CNT,DIGITAL_ORDER.CHECK_NBR,DIGITAL_ORDER.DELIVERY_DTTM,DIGITAL_ORDER.PAYMENT_URL,DIGITAL_ORDER.TALLY_TM,DIGITAL_ORDER.IDP_CUST_ID,DIGITAL_ORDER.CUST_MEMBERSHIP_NBR,DIGITAL_ORDER.CUST_FIRST_NM,DIGITAL_ORDER.CUST_LAST_NM,DIGITAL_ORDER.CUST_PHONE_NBR,DIGITAL_ORDER.CUST_EMAIL,DIGITAL_ORDER.CUST_ADR_LINE_1_TXT,DIGITAL_ORDER.CUST_ADR_LINE_2_TXT,DIGITAL_ORDER.CUST_CTY_NM,DIGITAL_ORDER.CUST_ST_CD,DIGITAL_ORDER.CUST_ZIP_CD,DIGITAL_ORDER.LOC_NM,DIGITAL_ORDER.LOC_PHONE_NBR,DIGITAL_ORDER.LOC_EMAIL,DIGITAL_ORDER.LOC_ADR_LINE_1_TXT,DIGITAL_ORDER.LOC_ADR_LINE_2_TXT,DIGITAL_ORDER.LOC_CTY_NM,DIGITAL_ORDER.LOC_ST_CD,DIGITAL_ORDER.LOC_ZIP_CD,DIGITAL_ORDER.LOC_FRAUD_CHECK_IND,DIGITAL_ORDER.ORDER_CORRELATION_ID,DIGITAL_ORDER.FULLFILLMENT_UNIQUE_IDENTIFER,DIGITAL_ORDER.FULFILLMENT_ID,DIGITAL_ORDER.FULFILLMENT_TYP_CD,DIGITAL_ORDER.FULFILLMENT_STATUS_URL,DIGITAL_ORDER.DELIVERY_CONTACT_FIRST_NM,DIGITAL_ORDER.DELIVERY_CONTACT_LAST_NM,DIGITAL_ORDER.DELIVERY_CONTACT_PHONE_NBR,DIGITAL_ORDER.DELIVERY_CONTACT_EMAIL,DIGITAL_ORDER.DELIVERY_ADR_LINE_1_TXT,DIGITAL_ORDER.DELIVERY_ADR_LINE_2_TXT,DIGITAL_ORDER.DELIVERY_CTY_NM,DIGITAL_ORDER.DELIVERY_ST_CD,DIGITAL_ORDER.DELIVERY_ZIP_CD,DIGITAL_ORDER.DELIVERY_PARTNER_NM,DIGITAL_ORDER.DELIVERY_VEHICLE_DESC,DIGITAL_ORDER.DELIVERY_STATUS_TXT,DIGITAL_ORDER.CUST_FULFILLMENT_INSTRUCTION_TXT,DIGITAL_ORDER.ORDER_METADATA,DIGITAL_ORDER.LOAD_ID,DIGITAL_ORDER.LOAD_DTTM,DIGITAL_ORDER.UPDATE_ID,DIGITAL_ORDER.UPDATE_DTTM,DIGITAL_ORDER.CUST_PICKUP_INSTRUCTION_TXT
FROM IDS_DEV.TXN.DIGITAL_ORDER ;
create view IF NOT EXISTS DIGITAL_ORDER_CHANNEL_MAPPING_BV(
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	SOURCE_BRAND_ID,
	SOURCE_ORDER_CHANNEL_CD COMMENT 'Source Order Channel Code specifies how an order was submitted to a location. For example, WEB for browser based orders or IOSAPP for order submitted via iphone or ipad.',
	SOURCE_FULFILLMENT_CHANNEL_CD COMMENT 'Source Fulfillment Channel Code specifies how an orderfulfilled.For example, Curbside or Pickup.',
	TARGET_ORDER_CHANNEL_ID COMMENT 'Order Channel Identifier is a unique identifier that specifies a unique brand and channel combination.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Digitanl Order Channel Mapping contains record to map from a digital order to a UDP Order Channel. A Order Channel is the combination of the Soure of the Order as well as the Fulfillment Type of an Order.'
 as 
SELECT DIGITAL_ORDER_CHANNEL_MAPPING.SOURCE_SYSTEM_NM,DIGITAL_ORDER_CHANNEL_MAPPING.SOURCE_BRAND_ID,DIGITAL_ORDER_CHANNEL_MAPPING.SOURCE_ORDER_CHANNEL_CD,DIGITAL_ORDER_CHANNEL_MAPPING.SOURCE_FULFILLMENT_CHANNEL_CD,DIGITAL_ORDER_CHANNEL_MAPPING.TARGET_ORDER_CHANNEL_ID,DIGITAL_ORDER_CHANNEL_MAPPING.LOAD_ID,DIGITAL_ORDER_CHANNEL_MAPPING.LOAD_DTTM,DIGITAL_ORDER_CHANNEL_MAPPING.UPDATE_ID,DIGITAL_ORDER_CHANNEL_MAPPING.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_ORDER_CHANNEL_MAPPING ;
create view IF NOT EXISTS DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	DISCOUNT_ID COMMENT 'Discount Identifier . Sample Value: 1018131.',
	ITEM_DISCOUNT_AMT COMMENT 'Item Discount Amount . Sample Value: 0.',
	MENU_ITEM_ID COMMENT 'Menu Item Identifier . Sample Value: arb-itm-003-085. This is also know as Product Identifier',
	DISCOUNT_ITEM_QTY COMMENT 'Quantity . Sample Value: 1.',
	LINE_ITEM_NBR,
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.'
) as 
SELECT DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.BRAND_ID,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.BUSINESS_DT,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.DIGITAL_ORDER_ID,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.LOC_ID,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.DISCOUNT_ID,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.ITEM_DISCOUNT_AMT,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.MENU_ITEM_ID,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.DISCOUNT_ITEM_QTY,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.LINE_ITEM_NBR,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.LOAD_ID,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.LOAD_DTTM,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.UPDATE_ID,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.UPDATE_DTTM,DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM.SOURCE_SYSTEM_NM
FROM IDS_DEV.TXN.DIGITAL_ORDER_DISCOUNT_APPLIED_ITEM ;
create view IF NOT EXISTS DIGITAL_ORDER_DISCOUNT_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	OMS_OFFER_CD COMMENT 'Oms Offer Code . Sample Value: ca6b31cc-4849-493d-a6a0-12abb89a0757.',
	DISCOUNT_CERTIFICATE_ID COMMENT 'Certificate Identifier . Sample Value: null.',
	DISCOUNT_CD COMMENT 'Discount Code . Sample Value: null.',
	COMPLIMENTARY_CD COMMENT 'Compensation Code . Sample Value: null.',
	DISCOUNT_SKU COMMENT 'Sku . Sample Value: null.',
	COMPLIMENTARY_APPLIED_IND COMMENT 'Compensation Applied Indicator. Sample Value: null.',
	DISCOUNT_NM COMMENT 'Discount Name . Sample Value: DEV TEST -- Free Small Shake.',
	DISCOUNT_ID COMMENT 'Discount Identifier . Sample Value: 1018131.',
	LOYALTY_ID COMMENT 'Loyalty Identifier . Sample Value: null.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.'
) COMMENT='Order Type contains the various combinations how an order will be given to the customer.'
 as 
SELECT DIGITAL_ORDER_DISCOUNT.BRAND_ID,DIGITAL_ORDER_DISCOUNT.BUSINESS_DT,DIGITAL_ORDER_DISCOUNT.DIGITAL_ORDER_ID,DIGITAL_ORDER_DISCOUNT.LOC_ID,DIGITAL_ORDER_DISCOUNT.OMS_OFFER_CD,DIGITAL_ORDER_DISCOUNT.DISCOUNT_CERTIFICATE_ID,DIGITAL_ORDER_DISCOUNT.DISCOUNT_CD,DIGITAL_ORDER_DISCOUNT.COMPLIMENTARY_CD,DIGITAL_ORDER_DISCOUNT.DISCOUNT_SKU,DIGITAL_ORDER_DISCOUNT.COMPLIMENTARY_APPLIED_IND,DIGITAL_ORDER_DISCOUNT.DISCOUNT_NM,DIGITAL_ORDER_DISCOUNT.DISCOUNT_ID,DIGITAL_ORDER_DISCOUNT.LOYALTY_ID,DIGITAL_ORDER_DISCOUNT.LOAD_ID,DIGITAL_ORDER_DISCOUNT.LOAD_DTTM,DIGITAL_ORDER_DISCOUNT.UPDATE_ID,DIGITAL_ORDER_DISCOUNT.UPDATE_DTTM,DIGITAL_ORDER_DISCOUNT.SOURCE_SYSTEM_NM
FROM IDS_DEV.TXN.DIGITAL_ORDER_DISCOUNT ;
create view IF NOT EXISTS DIGITAL_ORDER_LINE_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LINE_ITEM_ID COMMENT 'Line Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	LINE_ITEM_DESC COMMENT 'Item Description . Sample Value: Large Cheddar Cheese Curds.',
	LINE_ITEM_PRICE_AMT COMMENT 'Item Price Amount. Sample Value: 9.99.',
	LINE_ITEM_DISCOUNTED_AMT,
	LINE_ITEM_PRODUCT_ID COMMENT 'Product Identifier . Sample Value: SalesItem-3032.',
	LINE_ITEM_DESTINATION_CD COMMENT 'Order Destination Code. Sample Value: 1.',
	LINE_ITEM_QTY COMMENT 'Order Quantity. Sample Value: 1.',
	SUGGESTED_SELL_RECOMMENDATION_ID COMMENT 'Suggested Sell Recommendation Identifier uniquely identifies the recommendation that determine a set of products. The recommendation is based on a backet of goods and a set of recommendations from a recommendation file. Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	LINE_ITEM_PRODUCT_CATEGORY_NM COMMENT 'Line Item Product Category Name specifies a grouping for an item. For example, Shake is the categorization for a Small Vanilla Shake.',
	LINE_ITEM_POS_ID COMMENT 'Line Item POS Identifier.  Sample value 640243388.',
	LINE_ITEM_COMPONENT_ID,
	LINE_ITEM_AVAILABLE_IND,
	LINE_ITEM_AVAILABLE_POS_ID,
	LINE_ITEM_AVAILABLE_QTY,
	LINE_ITEM_NOTE_TXT,
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Digital Order Line specifies a particualr item for an order.'
 as 
SELECT DIGITAL_ORDER_LINE.BRAND_ID,DIGITAL_ORDER_LINE.BUSINESS_DT,DIGITAL_ORDER_LINE.DIGITAL_ORDER_ID,DIGITAL_ORDER_LINE.LOC_ID,DIGITAL_ORDER_LINE.LINE_ITEM_NBR,DIGITAL_ORDER_LINE.SOURCE_SYSTEM_NM,DIGITAL_ORDER_LINE.LINE_ITEM_ID,DIGITAL_ORDER_LINE.LINE_ITEM_DESC,DIGITAL_ORDER_LINE.LINE_ITEM_PRICE_AMT,DIGITAL_ORDER_LINE.LINE_ITEM_DISCOUNTED_AMT,DIGITAL_ORDER_LINE.LINE_ITEM_PRODUCT_ID,DIGITAL_ORDER_LINE.LINE_ITEM_DESTINATION_CD,DIGITAL_ORDER_LINE.LINE_ITEM_QTY,DIGITAL_ORDER_LINE.SUGGESTED_SELL_RECOMMENDATION_ID,DIGITAL_ORDER_LINE.LINE_ITEM_PRODUCT_CATEGORY_NM,DIGITAL_ORDER_LINE.LINE_ITEM_POS_ID,DIGITAL_ORDER_LINE.LINE_ITEM_COMPONENT_ID,DIGITAL_ORDER_LINE.LINE_ITEM_AVAILABLE_IND,DIGITAL_ORDER_LINE.LINE_ITEM_AVAILABLE_POS_ID,DIGITAL_ORDER_LINE.LINE_ITEM_AVAILABLE_QTY,DIGITAL_ORDER_LINE.LINE_ITEM_NOTE_TXT,DIGITAL_ORDER_LINE.LOAD_ID,DIGITAL_ORDER_LINE.LOAD_DTTM,DIGITAL_ORDER_LINE.UPDATE_ID,DIGITAL_ORDER_LINE.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_ORDER_LINE ;
create view IF NOT EXISTS DIGITAL_ORDER_LINE_CHILD_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CHILD_ITEM_ID COMMENT 'Child Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	CHILD_ITEM_DESC COMMENT 'Child Description . Sample Value: Large Cheddar Cheese Curds.',
	CHILD__ITEM_PRICE_AMT COMMENT 'Child Price Amount. Sample Value: 9.99.',
	CHILD_ITEM_PRODUCT_ID COMMENT 'Child Product Identifier . Sample Value: SalesItem-3032.',
	CHILD_ITEM_DESTINATION_CD COMMENT 'Child Item Destination Code. Sample Value: 1.',
	CHILD_ITEM_QTY COMMENT 'Child Item Quantity. Sample Value: 1.',
	CHILD_SUGGESTED_SELL_RECOMMENDATION_ID COMMENT 'Child Suggested Sell Recommendation Identifier uniquely identifies the recommendation that determine a set of products. The recommendation is based on a backet of goods and a set of recommendations from a recommendation file. Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	CHILD_ITEM_PRODUCT_CATEGORY_NM COMMENT 'Child Item Product Category Name specifies a grouping for an item. For example, Shake is the categorization for a Small Vanilla Shake.',
	CHILD_ITEM_POS_ID COMMENT 'Child Item POS Identifier.  Sample value 640243388.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CHILD_ITEM_DISCOUNTED_AMT,
	CHILD_ITEM_COMPONENT_ID,
	CHILD_ITEM_AVAILABLE_IND,
	CHILD_ITEM_AVAILABLE_POS_ID,
	CHILD_ITEM_AVAILABLE_QTY,
	CHILD_ITEM_NOTE_TXT,
	CHILD_PARENT_ITEM_ID COMMENT 'Child Parent Item Identifier identifies the parent Child Item of a nested Child Item. '
) as 
SELECT DIGITAL_ORDER_LINE_CHILD.BRAND_ID,DIGITAL_ORDER_LINE_CHILD.BUSINESS_DT,DIGITAL_ORDER_LINE_CHILD.DIGITAL_ORDER_ID,DIGITAL_ORDER_LINE_CHILD.LOC_ID,DIGITAL_ORDER_LINE_CHILD.LINE_ITEM_NBR,DIGITAL_ORDER_LINE_CHILD.SOURCE_SYSTEM_NM,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_ID,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_DESC,DIGITAL_ORDER_LINE_CHILD.CHILD__ITEM_PRICE_AMT,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_PRODUCT_ID,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_DESTINATION_CD,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_QTY,DIGITAL_ORDER_LINE_CHILD.CHILD_SUGGESTED_SELL_RECOMMENDATION_ID,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_PRODUCT_CATEGORY_NM,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_POS_ID,DIGITAL_ORDER_LINE_CHILD.LOAD_ID,DIGITAL_ORDER_LINE_CHILD.LOAD_DTTM,DIGITAL_ORDER_LINE_CHILD.UPDATE_ID,DIGITAL_ORDER_LINE_CHILD.UPDATE_DTTM,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_DISCOUNTED_AMT,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_COMPONENT_ID,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_AVAILABLE_IND,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_AVAILABLE_POS_ID,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_AVAILABLE_QTY,DIGITAL_ORDER_LINE_CHILD.CHILD_ITEM_NOTE_TXT,DIGITAL_ORDER_LINE_CHILD.CHILD_PARENT_ITEM_ID
FROM IDS_DEV.TXN.DIGITAL_ORDER_LINE_CHILD ;
create view IF NOT EXISTS DIGITAL_ORDER_LINE_CHILD_MODIFIER_BV(
	CHILD_MODIFIER_DESC COMMENT 'Modifier Description . Sample Value: Southwestern Ranch.',
	CHILD_MODIFIER_PRODUCT_ID COMMENT 'Modifier Product Identifier . Sample Value: Modifier-5161.',
	CHILD_MODIFIER_LINE_ITEM_NBR COMMENT 'Modifier Line Item Number specifies the line the modifier is displayed on',
	CHILD_MODIFIER_PRICE_AMT COMMENT 'Modifier Price Amount. Sample Value: 0.00.',
	CHILD_MODIFIER_DISCOUNTED_PRICE_AMT COMMENT 'Modifier Discounted Price Amount is the price of the item after it has been discounted.',
	CHILD_MODIFIER_QTY COMMENT 'Modifier Quantity. Sample Value: 1.',
	CHILD_MODIFIER_DISPLAY_NM COMMENT 'Modifier Display Name . Sample Value: Southwestern Ranch.',
	CHILD_MODIFIER_POS_ID COMMENT 'Modifier Pos Identifier. Sample Value: 640212495.',
	CHILD_MODIFIER_AVAILABLE_IND,
	CHILD_MODIFIER_AVAILABLE_POS_ID,
	CHILD_MODIFIER_AVAILABLE_QTY,
	CHILD_MODIFIER_ACTION_CD COMMENT 'Modifier Action Code',
	CHILD_MODIFIER_ACTION_DESC COMMENT 'Modifier Action Description',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CHILD_ITEM_ID COMMENT 'Child Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	CHILD_MODIFIER_GROUP_ID COMMENT 'Modifier Group Id . Sample Value: 21521.',
	CHILD_MODIFIER_ID COMMENT 'Modifier Id . Sample Value: 21551.'
) as 
SELECT DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_DESC,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_PRODUCT_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_LINE_ITEM_NBR,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_PRICE_AMT,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_DISCOUNTED_PRICE_AMT,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_QTY,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_DISPLAY_NM,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_POS_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_AVAILABLE_IND,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_AVAILABLE_POS_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_AVAILABLE_QTY,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_ACTION_CD,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_ACTION_DESC,DIGITAL_ORDER_LINE_CHILD_MODIFIER.LOAD_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.LOAD_DTTM,DIGITAL_ORDER_LINE_CHILD_MODIFIER.UPDATE_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.UPDATE_DTTM,DIGITAL_ORDER_LINE_CHILD_MODIFIER.BRAND_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.BUSINESS_DT,DIGITAL_ORDER_LINE_CHILD_MODIFIER.DIGITAL_ORDER_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.LOC_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.LINE_ITEM_NBR,DIGITAL_ORDER_LINE_CHILD_MODIFIER.SOURCE_SYSTEM_NM,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_ITEM_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_GROUP_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER.CHILD_MODIFIER_ID
FROM IDS_DEV.TXN.DIGITAL_ORDER_LINE_CHILD_MODIFIER ;
create view IF NOT EXISTS DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP_BV(
	CHILD_MODIFIER_GROUP_POS_ID COMMENT 'Modifier Group Pos Identifier . Sample Value: 8937.',
	CHILD_MODIFIER_GROUP_PRODUCT_ID COMMENT 'Modifier Group Product Identifier . Sample Value: ModifierGroup-8937.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	CHILD_MODIFIER_GROUP_DESC COMMENT 'Modifier Group Description.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	CHILD_ITEM_ID COMMENT 'Child Item Identifier specifies a specific product that was ordered . Sample Value: 21503.',
	CHILD_MODIFIER_GROUP_ID COMMENT 'Modifier Group Id . Sample Value: 21521.'
) as 
SELECT DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.CHILD_MODIFIER_GROUP_POS_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.CHILD_MODIFIER_GROUP_PRODUCT_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.LOAD_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.LOAD_DTTM,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.UPDATE_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.UPDATE_DTTM,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.CHILD_MODIFIER_GROUP_DESC,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.BRAND_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.BUSINESS_DT,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.DIGITAL_ORDER_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.LOC_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.LINE_ITEM_NBR,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.SOURCE_SYSTEM_NM,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.CHILD_ITEM_ID,DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP.CHILD_MODIFIER_GROUP_ID
FROM IDS_DEV.TXN.DIGITAL_ORDER_LINE_CHILD_MODIFIER_GROUP ;
create view IF NOT EXISTS DIGITAL_ORDER_LINE_MODIFIER_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	MODIFIER_GROUP_ID COMMENT 'Modifier Group Id . Sample Value: 21521.',
	MODIFIER_ID COMMENT 'Modifier Id . Sample Value: 21551.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	MODIFIER_DESC COMMENT 'Modifier Description . Sample Value: Southwestern Ranch.',
	MODIFIER_PRODUCT_ID COMMENT 'Modifier Product Identifier . Sample Value: Modifier-5161.',
	MODIFIER_LINE_ITEM_NBR COMMENT 'Modifier Line Item Number specifies the line the modifier is displayed on',
	MODIFIER_PRICE_AMT COMMENT 'Modifier Price Amount. Sample Value: 0.00.',
	MODIFIER_DISCOUNTED_PRICE_AMT COMMENT 'Modifier Discounted Price Amount is the price of the item after it has been discounted.',
	MODIFIER_QTY COMMENT 'Modifier Quantity. Sample Value: 1.',
	MODIFIER_DISPLAY_NM COMMENT 'Modifier Display Name . Sample Value: Southwestern Ranch.',
	MODIFIER_POS_ID COMMENT 'Modifier Pos Identifier. Sample Value: 640212495.',
	MODIFIER_AVAILABLE_IND,
	MODIFIER_AVAILABLE_POS_ID,
	MODIFIER_AVAILABLE_QTY,
	MODIFIER_ACTION_CD COMMENT 'Modifier Action Code',
	MODIFIER_ACTION_DESC COMMENT 'Modifier Action Description',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT DIGITAL_ORDER_LINE_MODIFIER.BRAND_ID,DIGITAL_ORDER_LINE_MODIFIER.BUSINESS_DT,DIGITAL_ORDER_LINE_MODIFIER.DIGITAL_ORDER_ID,DIGITAL_ORDER_LINE_MODIFIER.LOC_ID,DIGITAL_ORDER_LINE_MODIFIER.LINE_ITEM_NBR,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_GROUP_ID,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_ID,DIGITAL_ORDER_LINE_MODIFIER.SOURCE_SYSTEM_NM,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_DESC,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_PRODUCT_ID,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_LINE_ITEM_NBR,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_PRICE_AMT,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_DISCOUNTED_PRICE_AMT,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_QTY,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_DISPLAY_NM,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_POS_ID,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_AVAILABLE_IND,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_AVAILABLE_POS_ID,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_AVAILABLE_QTY,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_ACTION_CD,DIGITAL_ORDER_LINE_MODIFIER.MODIFIER_ACTION_DESC,DIGITAL_ORDER_LINE_MODIFIER.LOAD_ID,DIGITAL_ORDER_LINE_MODIFIER.LOAD_DTTM,DIGITAL_ORDER_LINE_MODIFIER.UPDATE_ID,DIGITAL_ORDER_LINE_MODIFIER.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_ORDER_LINE_MODIFIER ;
create view IF NOT EXISTS DIGITAL_ORDER_LINE_MODIFIER_GROUP_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	LINE_ITEM_NBR COMMENT 'Line Item Number identifes a specific entry on an order. Sample Value: 1.',
	MODIFIER_GROUP_ID COMMENT 'Modifier Group Id . Sample Value: 21521.',
	MODIFIER_GROUP_POS_ID COMMENT 'Modifier Group Pos Identifier . Sample Value: 8937.',
	MODIFIER_GROUP_PRODUCT_ID COMMENT 'Modifier Group Product Identifier . Sample Value: ModifierGroup-8937.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	MODIFIER_GROUP_DESC COMMENT 'Modifier Group Description.'
) as 
SELECT DIGITAL_ORDER_LINE_MODIFIER_GROUP.BRAND_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.BUSINESS_DT,DIGITAL_ORDER_LINE_MODIFIER_GROUP.DIGITAL_ORDER_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.LOC_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.LINE_ITEM_NBR,DIGITAL_ORDER_LINE_MODIFIER_GROUP.MODIFIER_GROUP_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.MODIFIER_GROUP_POS_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.MODIFIER_GROUP_PRODUCT_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.LOAD_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.LOAD_DTTM,DIGITAL_ORDER_LINE_MODIFIER_GROUP.UPDATE_ID,DIGITAL_ORDER_LINE_MODIFIER_GROUP.UPDATE_DTTM,DIGITAL_ORDER_LINE_MODIFIER_GROUP.SOURCE_SYSTEM_NM,DIGITAL_ORDER_LINE_MODIFIER_GROUP.MODIFIER_GROUP_DESC
FROM IDS_DEV.TXN.DIGITAL_ORDER_LINE_MODIFIER_GROUP ;
create view IF NOT EXISTS DIGITAL_ORDER_PAYMENT_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	PAYMENT_SEQ_NBR COMMENT 'Payment Sequence Number is the order in which objects are listed in the message or source of the file. Values are 1, 2, 3, 4, etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	PAYMENT_TYP_CD COMMENT 'Payment Type Code is a unique code specifying how a settlement occured. Examples: TOKEN -  a credit card transaction through a third party processor, CASH - monies were exchanged.',
	PAYMENT_CARD_ISSUER_NM COMMENT 'Card Issuer Name. Sample Value: Visa.',
	PAYMENT_CARD_NBR COMMENT 'Card Number . Sample Value: 411111xxxxxx1111.',
	PAYMENT_AMT COMMENT 'Payment Amount. Sample Value: 10.70.',
	PAYMENT_CARD_HOLDER_NM COMMENT 'Payment Card Holder Name. Sample Value: Dasha.',
	PAYMENT_PROCESS_CD COMMENT 'Payment Process Code is a unique code reflecting payment on order. Sample value: 01Z6JF4VB001U6A6VSG0M3LBSFTC8TGO.',
	PAYMENT_STATUS_TXT COMMENT 'Payment Status Text',
	PAYMENT_AUTHORIZATION_CD,
	PAYMENT_PROCESS_MESSAGE_TXT,
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT DIGITAL_ORDER_PAYMENT.BRAND_ID,DIGITAL_ORDER_PAYMENT.BUSINESS_DT,DIGITAL_ORDER_PAYMENT.DIGITAL_ORDER_ID,DIGITAL_ORDER_PAYMENT.LOC_ID,DIGITAL_ORDER_PAYMENT.PAYMENT_SEQ_NBR,DIGITAL_ORDER_PAYMENT.SOURCE_SYSTEM_NM,DIGITAL_ORDER_PAYMENT.PAYMENT_TYP_CD,DIGITAL_ORDER_PAYMENT.PAYMENT_CARD_ISSUER_NM,DIGITAL_ORDER_PAYMENT.PAYMENT_CARD_NBR,DIGITAL_ORDER_PAYMENT.PAYMENT_AMT,DIGITAL_ORDER_PAYMENT.PAYMENT_CARD_HOLDER_NM,DIGITAL_ORDER_PAYMENT.PAYMENT_PROCESS_CD,DIGITAL_ORDER_PAYMENT.PAYMENT_STATUS_TXT,DIGITAL_ORDER_PAYMENT.PAYMENT_AUTHORIZATION_CD,DIGITAL_ORDER_PAYMENT.PAYMENT_PROCESS_MESSAGE_TXT,DIGITAL_ORDER_PAYMENT.LOAD_ID,DIGITAL_ORDER_PAYMENT.LOAD_DTTM,DIGITAL_ORDER_PAYMENT.UPDATE_ID,DIGITAL_ORDER_PAYMENT.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_ORDER_PAYMENT ;
create view IF NOT EXISTS DIGITAL_ORDER_PICKUP_INSTRUCTION_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	BUSINESS_DT COMMENT 'Business Date is the day, month, year of when an order was placed.',
	DIGITAL_ORDER_ID COMMENT 'Digital Order Identifier. Sample Value: 21462.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	PICKUP_INSTRUCTION_SEQ_NBR COMMENT 'Pickup Instruction Sequence Number is the order in which objects are listed in the message or source of the file. Values are 1, 2, 3, 4, etc.',
	INSTRUCTION_TXT COMMENT 'Instruction Text contains customer specified instructions for delivery.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) as 
SELECT DIGITAL_ORDER_PICKUP_INSTRUCTION.BRAND_ID,DIGITAL_ORDER_PICKUP_INSTRUCTION.BUSINESS_DT,DIGITAL_ORDER_PICKUP_INSTRUCTION.DIGITAL_ORDER_ID,DIGITAL_ORDER_PICKUP_INSTRUCTION.LOC_ID,DIGITAL_ORDER_PICKUP_INSTRUCTION.SOURCE_SYSTEM_NM,DIGITAL_ORDER_PICKUP_INSTRUCTION.PICKUP_INSTRUCTION_SEQ_NBR,DIGITAL_ORDER_PICKUP_INSTRUCTION.INSTRUCTION_TXT,DIGITAL_ORDER_PICKUP_INSTRUCTION.LOAD_ID,DIGITAL_ORDER_PICKUP_INSTRUCTION.LOAD_DTTM,DIGITAL_ORDER_PICKUP_INSTRUCTION.UPDATE_ID,DIGITAL_ORDER_PICKUP_INSTRUCTION.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_ORDER_PICKUP_INSTRUCTION ;
create view IF NOT EXISTS DIGITAL_SUGGESTED_SELL_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ORDER_CORRELATION_ID COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	SUGGESTED_SELL_RECOMMENDATION_ID,
	TARGET_SALE_SEGMENT_CD COMMENT 'Target Sale Segment Code is used to determine which suggestions are offered to the customer. There are three segmenets: Original - control group where no suggestions are shown; this variant does not have the customerType cookie,  Learning - the Learning Challenger group which sets the customerType to �LC�, and Polaris - the Polaris Challenger group which sets the customerType to �PC�.  Individuals are dynamically assigned a segment for the transaction.',
	LOC_ID COMMENT 'Location Identifier . Sample Value: 13.',
	RECOMMENDATION_DTTM COMMENT 'Recommendation Datetime  is the day, month, year and time of when a the recommendation occured.',
	BASKET_ID COMMENT 'Basket Identifier specified a basket of goods based on its characteristics that an individual is looking to purchase. The characteristics that are used in deriving a basiket identifier are Day of Week, Part of Day(breakfast, lunch, dinner), and number of entres, sides and drinks. Basket Identifier is currently only applicable to a customer segement of Polaris Challenger (PC).',
	ITEM_RECOMMENDATION_FILENAME COMMENT 'Item Recommendation Filename containes the set of recommnedations for items that were used to generate the recommendation set.',
	CUST_SOURCE_CHANNEL_CD COMMENT 'SOURCE_CHANNEL: the selling channel where the Customer originated. Example values: [STORE|WEBOA|MOBILE|KIOSK|PHONE|�].',
	CUST_SOURCE_SUB_CHANNEL_CD COMMENT 'provides additional/support information about the SOURCE_CHANNEL property. Example values: [PREFERENCES|�|.',
	EVENT_TYP_CD COMMENT 'this property indicates the type of event, which corresponds to the IDP domain. Sample values: [CUSTOMER|�]',
	EVENT_SUB_TYP_CD COMMENT 'this property provides additional information on the event type. For example, it could indicate that the event is about a change of the order status for an Order event. Sample values: [PREFERENCE_TYPE|�]',
	EVENT_STATUS_CD COMMENT '(optional): this property may indicate the actual change that trigged this event. For example, it could include the actual value of the Customer Status of �EMAIL_SIGNUP� so that only the consumers that are interested in Customer email signup events will take actions on this event. This property may not apply to every Customer event and thus is optional. But Customer domain services encourage to include this property where applicable.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Digital Suggested Sell Base View provides recommendations for additional items to purchase. For a Polaris Challenger a basket of goods is used to provide a recommendation of additional items. For a Learning Challender it is based on day of week and time of day.'
 as 
SELECT DIGITAL_SUGGESTED_SELL.BRAND_ID,DIGITAL_SUGGESTED_SELL.SOURCE_SYSTEM_NM,DIGITAL_SUGGESTED_SELL.ORDER_CORRELATION_ID,DIGITAL_SUGGESTED_SELL.SUGGESTED_SELL_RECOMMENDATION_ID,DIGITAL_SUGGESTED_SELL.TARGET_SALE_SEGMENT_CD,DIGITAL_SUGGESTED_SELL.LOC_ID,DIGITAL_SUGGESTED_SELL.RECOMMENDATION_DTTM,DIGITAL_SUGGESTED_SELL.BASKET_ID,DIGITAL_SUGGESTED_SELL.ITEM_RECOMMENDATION_FILENAME,DIGITAL_SUGGESTED_SELL.CUST_SOURCE_CHANNEL_CD,DIGITAL_SUGGESTED_SELL.CUST_SOURCE_SUB_CHANNEL_CD,DIGITAL_SUGGESTED_SELL.EVENT_TYP_CD,DIGITAL_SUGGESTED_SELL.EVENT_SUB_TYP_CD,DIGITAL_SUGGESTED_SELL.EVENT_STATUS_CD,DIGITAL_SUGGESTED_SELL.LOAD_ID,DIGITAL_SUGGESTED_SELL.LOAD_DTTM,DIGITAL_SUGGESTED_SELL.UPDATE_ID,DIGITAL_SUGGESTED_SELL.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_SUGGESTED_SELL ;
create view IF NOT EXISTS DIGITAL_SUGGESTED_SELL_CART_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ORDER_CORRELATION_ID COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	SUGGESTED_SELL_RECOMMENDATION_ID,
	CART_ITEM_ID COMMENT 'Cart Item Identifier specifies a product that is in a cart prior to checkout and used in the Suggested Sell process. Sample Value(s): arb-itm-000-001, arb-itm-003-001.',
	CART_ITEM_SEQ_NBR COMMENT 'Cart Item Sequence Number is the order of the items listed in the cart from the Suggested Sell message.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Digital Suggested Sell Cart Base View  is the list of items the user is looking to purchase that are part of the Suggested Sell process.'
 as 
SELECT DIGITAL_SUGGESTED_SELL_CART.BRAND_ID,DIGITAL_SUGGESTED_SELL_CART.SOURCE_SYSTEM_NM,DIGITAL_SUGGESTED_SELL_CART.ORDER_CORRELATION_ID,DIGITAL_SUGGESTED_SELL_CART.SUGGESTED_SELL_RECOMMENDATION_ID,DIGITAL_SUGGESTED_SELL_CART.CART_ITEM_ID,DIGITAL_SUGGESTED_SELL_CART.CART_ITEM_SEQ_NBR,DIGITAL_SUGGESTED_SELL_CART.LOAD_ID,DIGITAL_SUGGESTED_SELL_CART.LOAD_DTTM,DIGITAL_SUGGESTED_SELL_CART.UPDATE_ID,DIGITAL_SUGGESTED_SELL_CART.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_SUGGESTED_SELL_CART ;
create view IF NOT EXISTS DIGITAL_SUGGESTED_SELL_RECOMMENDATION_BV(
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	ORDER_CORRELATION_ID COMMENT 'Order Correlation Identifier is a unique value that can be utilized to associate multiple messages together. An identifier to stitch Measurement data between IDP and Google Analytics for capturing the sequence of user flow and user interaction unique to each session, from the point where customer adds first item to bag and sees recommendations through addition/removal of more items until checkout, capturing which recommended items the customer viewed and added to bag  Sample value d3b39702-001e-11ec-9a03-0242ac130003.',
	SUGGESTED_SELL_RECOMMENDATION_ID,
	RECOMMENDATION_ITEM_ID COMMENT 'Recommendation Item Identifier specifies a recommended product for an order as part of the Suggested Sell process . Sample Value(s): arb-itm-000-001, arb-itm-003-001.',
	RECOMMENDATION_ITEM_SEQ_NBR COMMENT 'Recommendation Item Sequence Number is the order of the recommended items listed in the suggested sell message.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='Digital Suggested Sell Recommendation Base View is the list of items that are recommended to the user for purchase as part of the Suggested Sell process.'
 as 
SELECT DIGITAL_SUGGESTED_SELL_RECOMMENDATION.BRAND_ID,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.SOURCE_SYSTEM_NM,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.ORDER_CORRELATION_ID,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.SUGGESTED_SELL_RECOMMENDATION_ID,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.RECOMMENDATION_ITEM_ID,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.RECOMMENDATION_ITEM_SEQ_NBR,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.LOAD_ID,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.LOAD_DTTM,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.UPDATE_ID,DIGITAL_SUGGESTED_SELL_RECOMMENDATION.UPDATE_DTTM
FROM IDS_DEV.TXN.DIGITAL_SUGGESTED_SELL_RECOMMENDATION ;
create view IF NOT EXISTS FN_DAILY_REV_MEASURE_BV(
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
) COMMENT='FN Daily Revenue Measure contains data from different sources that has been related to a common measure at the daily level.'
 as 
SELECT FN_DAILY_REV_MEASURE.BRAND_ID,FN_DAILY_REV_MEASURE.BUSINESS_DATE,FN_DAILY_REV_MEASURE.REST_ID,FN_DAILY_REV_MEASURE.FN_SYSTEM_ID,FN_DAILY_REV_MEASURE.FN_MEASURE_ID,FN_DAILY_REV_MEASURE.GL_ACCOUNT_CODE,FN_DAILY_REV_MEASURE.GL_COST_CTR,FN_DAILY_REV_MEASURE.SALE_USD_AMOUNT,FN_DAILY_REV_MEASURE.SALE_AMOUNT,FN_DAILY_REV_MEASURE.SALE_COUNT,FN_DAILY_REV_MEASURE.COUNTRY_CODE,FN_DAILY_REV_MEASURE.CURRENCY_CODE,FN_DAILY_REV_MEASURE.SOURCE_SYSTEM_NAME,FN_DAILY_REV_MEASURE.LOAD_ID,FN_DAILY_REV_MEASURE.LOAD_DTTM,FN_DAILY_REV_MEASURE.UPDATE_ID,FN_DAILY_REV_MEASURE.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_DAILY_REV_MEASURE ;
create view IF NOT EXISTS FN_MEASURE_BV(
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	MEASURE_NAME COMMENT 'Measure Name is a label utilized to specify a value. Sample values Tax Exempt Sales, Non-Taxable Sales, Paid Outs, and Over Short.\n',
	MEASURE_DESC COMMENT 'Measure Description provides additional information about a measure. For example, Total Absolute Variance is taking the absolute value of a variance and aggregating it with other variances to provide the scope of the variance.',
	CREDIT_DEBIT_CODE COMMENT 'Credit Debit Code specifies if a measure is typically a credit or debit. Sample values, D or C.',
	DEFAULT_SORT_ID COMMENT 'Default Sort Identifier is a reporting sort order for measures when they appear in a report.',
	MEASURE_CATEGORY_NAME COMMENT 'Measure Category Name allows for a higher level grouping of measures. Samle values, Net Sales, Sales Tax,  Deposit, and Credit Cards.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='FN Measure contains the measure that can be utilized for reconciliation analysis. For example: Net Sales, Sales Tax, Cash Deposit, and Visa Card.'
 as 
SELECT FN_MEASURE.FN_MEASURE_ID,FN_MEASURE.MEASURE_NAME,FN_MEASURE.MEASURE_DESC,FN_MEASURE.CREDIT_DEBIT_CODE,FN_MEASURE.DEFAULT_SORT_ID,FN_MEASURE.MEASURE_CATEGORY_NAME,FN_MEASURE.SOURCE_SYSTEM_NAME,FN_MEASURE.LOAD_ID,FN_MEASURE.LOAD_DTTM,FN_MEASURE.UPDATE_ID,FN_MEASURE.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_MEASURE ;
create view IF NOT EXISTS FN_MEASURE_CALCULATION_BV(
	FN_MEASURE_CALCULATION_ID COMMENT 'FN Measure Calculation Identifier uniquely identifies a calculation for a measure.',
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	FN_MEASURE_CALCULATION_CATEGORY_CODE COMMENT 'FN Measure Calculation Category Code uniquely identifies a calculation type that can be performed to create a new measure.For example, AGG or ABSAGG.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='FN Measure Calculation specificies for a given measure what type of calculation will be utilized such as Aggregation or Absolute Aggregation.'
 as 
SELECT FN_MEASURE_CALCULATION.FN_MEASURE_CALCULATION_ID,FN_MEASURE_CALCULATION.FN_MEASURE_ID,FN_MEASURE_CALCULATION.FN_MEASURE_CALCULATION_CATEGORY_CODE,FN_MEASURE_CALCULATION.SOURCE_SYSTEM_NAME,FN_MEASURE_CALCULATION.LOAD_ID,FN_MEASURE_CALCULATION.LOAD_DTTM,FN_MEASURE_CALCULATION.UPDATE_ID,FN_MEASURE_CALCULATION.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_MEASURE_CALCULATION ;
create view IF NOT EXISTS FN_MEASURE_CALCULATION_CATEGORY_BV(
	FN_MEASURE_CALCULATION_CATEGORY_CODE COMMENT 'FN Measure Calculation Category Code uniquely identifies a calculation type that can be performed to create a new measure.For example, AGG or ABSAGG.',
	MEASURE_CALCULATION_CATEGORY_NAME COMMENT 'FN Measure Calculation Category Code specifies a calculation type that can be performed to create a new measure.For example, Aggregation or Absolute Aggregation.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='FN Measure Calculation Category specificies a method to derive a measure. For example, Aggregation or Absolute Aggregation.'
 as 
SELECT FN_MEASURE_CALCULATION_CATEGORY.FN_MEASURE_CALCULATION_CATEGORY_CODE,FN_MEASURE_CALCULATION_CATEGORY.MEASURE_CALCULATION_CATEGORY_NAME,FN_MEASURE_CALCULATION_CATEGORY.SOURCE_SYSTEM_NAME,FN_MEASURE_CALCULATION_CATEGORY.LOAD_ID,FN_MEASURE_CALCULATION_CATEGORY.LOAD_DTTM,FN_MEASURE_CALCULATION_CATEGORY.UPDATE_ID,FN_MEASURE_CALCULATION_CATEGORY.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_MEASURE_CALCULATION_CATEGORY ;
create view IF NOT EXISTS FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION_BV(
	FN_MEASURE_CALCULATION_ID COMMENT 'FN Measure Calculation Identifier uniquely identifies a calculation for a measure.',
	FN_INPUT_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='FN Measure Calculation Measure Association specificies which measures are utilized in the calculation of another measure.'
 as 
SELECT FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION.FN_MEASURE_CALCULATION_ID,FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION.FN_INPUT_MEASURE_ID,FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION.SOURCE_SYSTEM_NAME,FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION.LOAD_ID,FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION.LOAD_DTTM,FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION.UPDATE_ID,FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_MEASURE_CALCULATION_MEASURE_ASSOCIATION ;
create view IF NOT EXISTS FN_SYSTEM_BV(
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
) COMMENT='FN Systems specifies the different systems that are utilized for reconciliation analysis. For example: PAR Brink Data, Oracle General Ledger, Sales System, and Altametrics. '
 as 
SELECT FN_SYSTEM.FN_SYSTEM_ID,FN_SYSTEM.FN_SYSTEM_CATEGORY_CODE,FN_SYSTEM.SYSTEM_NAME,FN_SYSTEM.SYSTEM_DESC,FN_SYSTEM.BRAND_ID,FN_SYSTEM.SOURCE_SYSTEM_NAME,FN_SYSTEM.LOAD_ID,FN_SYSTEM.LOAD_DTTM,FN_SYSTEM.UPDATE_ID,FN_SYSTEM.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_SYSTEM ;
create view IF NOT EXISTS FN_SYSTEM_CATEGORY_BV(
	FN_SYSTEM_CATEGORY_CODE COMMENT 'FN System Category Code uniquely identifies a grouping for multiple systems in order to be analyzed by category. Sample values are POS, BO, SS and GL.',
	FN_SYSTEM_CATEGORY_NAME COMMENT 'FN System Category Name is a label for a grouping for multiple systems in order to be analyzed by category. Sample values are Point of Sale, Back Office, Sales System, and General Ledger.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='FN System Category allows for aggregation of data across multiple sources of similar data to a general system category.  For example: POS - Point of Sale, BO - Back Office, SS - Sales System, and GL - General Ledger. '
 as 
SELECT FN_SYSTEM_CATEGORY.FN_SYSTEM_CATEGORY_CODE,FN_SYSTEM_CATEGORY.FN_SYSTEM_CATEGORY_NAME,FN_SYSTEM_CATEGORY.SOURCE_SYSTEM_NAME,FN_SYSTEM_CATEGORY.LOAD_ID,FN_SYSTEM_CATEGORY.LOAD_DTTM,FN_SYSTEM_CATEGORY.UPDATE_ID,FN_SYSTEM_CATEGORY.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_SYSTEM_CATEGORY ;
create view IF NOT EXISTS FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE_BV(
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
) COMMENT='FN System GL Account To Measure Reference is utilized to map an account from source date to a measure.'
 as 
SELECT FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.FN_SYSTEM_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.BRAND_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.SOURCE_SYSTEM_NAME,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.GL_ACCOUNT_CODE,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.FN_MEASURE_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.GL_ACCOUNT_DEFAULT_MEASURE_IND,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.GL_ACCOUNT_PATTERN_IND,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.ACTIVE_IND,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.LOAD_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.LOAD_DTTM,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.UPDATE_ID,FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_SYSTEM_GL_ACCOUNT_TO_MEASURE_REFERENCE ;
create view IF NOT EXISTS FN_SYSTEM_TO_MEASURE_REFERENCE_BV(
	FN_SYSTEM_ID COMMENT 'FN System Identifier uniquely identifies a system that provides data for analysis.',
	SOURCE_SALES_SYSTEM_MEASURE_TEXT COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	BRAND_ID COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	FN_MEASURE_ID COMMENT 'FN Measure Identifier uniquely identifies a measure. ',
	VALUE_ADJUSTMENT_AMOUNT COMMENT 'Value Adjustment Amount is a factor that is applied to an incoming value in order to translate it. If Value Adjustment Amount is null then no factor is being applied to the value such as multiplying by 1. To adjust the sign of a value the factor would be -1. If dollar and cents are coming in as an integer than the factor of .01 could be utilized to convert the amount into a decimal number.',
	LOAD_ID COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
) COMMENT='FN System To Measure Reference is a generic mapping table from source data to measure.'
 as 
SELECT FN_SYSTEM_TO_MEASURE_REFERENCE.FN_SYSTEM_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.SOURCE_SALES_SYSTEM_MEASURE_TEXT,FN_SYSTEM_TO_MEASURE_REFERENCE.BRAND_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.SOURCE_SYSTEM_NAME,FN_SYSTEM_TO_MEASURE_REFERENCE.FN_MEASURE_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.VALUE_ADJUSTMENT_AMOUNT,FN_SYSTEM_TO_MEASURE_REFERENCE.LOAD_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.LOAD_DTTM,FN_SYSTEM_TO_MEASURE_REFERENCE.UPDATE_ID,FN_SYSTEM_TO_MEASURE_REFERENCE.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_SYSTEM_TO_MEASURE_REFERENCE ;
create view IF NOT EXISTS FN_WEEKLY_REV_MEASURE_BV(
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
) COMMENT='FN Weekly Revenue Measure contains data from different sources that has been related to a common measure at the weekly level.'
 as 
SELECT FN_WEEKLY_REV_MEASURE.BRAND_ID,FN_WEEKLY_REV_MEASURE.FISC_WK_END_DATE,FN_WEEKLY_REV_MEASURE.REST_ID,FN_WEEKLY_REV_MEASURE.FN_SYSTEM_ID,FN_WEEKLY_REV_MEASURE.FN_MEASURE_ID,FN_WEEKLY_REV_MEASURE.GL_ACCOUNT_CODE,FN_WEEKLY_REV_MEASURE.GL_COST_CTR,FN_WEEKLY_REV_MEASURE.SALE_USD_AMOUNT,FN_WEEKLY_REV_MEASURE.SALE_AMOUNT,FN_WEEKLY_REV_MEASURE.SALE_COUNT,FN_WEEKLY_REV_MEASURE.COUNTRY_CODE,FN_WEEKLY_REV_MEASURE.CURRENCY_CODE,FN_WEEKLY_REV_MEASURE.SOURCE_SYSTEM_NAME,FN_WEEKLY_REV_MEASURE.LOAD_ID,FN_WEEKLY_REV_MEASURE.LOAD_DTTM,FN_WEEKLY_REV_MEASURE.UPDATE_ID,FN_WEEKLY_REV_MEASURE.UPDATE_DTTM
FROM IDS_DEV.TXN.FN_WEEKLY_REV_MEASURE ;
create view IF NOT EXISTS INSPIRE_COMPARABLE_SALES_BV(
	AGG_LEVEL_TYP,
	BUSINESS_DT,
	BRAND_ID,
	STORE_NBR,
	DMA_NM,
	DMA_CD,
	LEVEL1_NM,
	LEVEL2_NM,
	LEVEL3_NM,
	LEVEL4_NM,
	LEVEL5_NM,
	OWNERSHIP_TYP,
	STATE_CD,
	COUNTRY_CD,
	DAY_OF_WEEK_NM,
	CURRENT_YEAR_START_DT,
	CURRENT_YEAR_END_DT,
	CURRENT_YEAR_NET_SALES_AMT,
	CURRENT_YEAR_TRANSACTION_CNT,
	CURRENT_YEAR_COMPARABLE_SALES_AMT,
	CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT,
	FISCAL_LAST_YEAR_DT,
	FISCAL_LAST_YEAR_START_DT,
	FISCAL_LAST_YEAR_END_DT,
	FISCAL_LAST_YEAR_NET_SALES_AMT,
	FISCAL_LAST_YEAR_TRANSACTION_CNT,
	FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT,
	FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT,
	CALENDAR_LAST_YEAR_COMPARABLE_DT,
	CALENDAR_LAST_YEAR_COMPARABLE_START_DT,
	CALENDAR_LAST_YEAR_COMPARABLE_END_DT,
	CALENDAR_LAST_YEAR_NET_SALES_AMT,
	CALENDAR_LAST_YEAR_TRANSACTION_CNT,
	CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT,
	CALENDAR_LAST_YEAR_COMPARABLE_TRANSACTION_CNT,
	CALENDAR_2YEAR_COMPARABLE_DT,
	CALENDAR_2YEAR_COMPARABLE_START_DT,
	CALENDAR_2YEAR_COMPARABLE_END_DT,
	CALENDAR_2YEAR_NET_SALES_AMT,
	CALENDAR_2YEAR_TRANSACTION_CNT,
	CALENDAR_2YEAR_COMPARABLE_SALES_AMT,
	CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT,
	CALENDAR_3YEAR_COMPARABLE_DT,
	CALENDAR_3YEAR_COMPARABLE_START_DT,
	CALENDAR_3YEAR_COMPARABLE_END_DT,
	CALENDAR_3YEAR_NET_SALES_AMT,
	CALENDAR_3YEAR_TRANSACTION_CNT,
	CALENDAR_3YEAR_COMPARABLE_SALES_AMT,
	CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT,
	LOAD_TYP,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as 
SELECT 
AGG_LEVEL_TYP,
BUSINESS_DT,
BRAND_ID,
STORE_NBR,
DMA_NM,
DMA_CD,
LEVEL1_NM,
LEVEL2_NM,
LEVEL3_NM,
LEVEL4_NM,
LEVEL5_NM,
OWNERSHIP_TYP,
STATE_CD,
COUNTRY_CD,
DAY_OF_WEEK_NM,
CURRENT_YEAR_START_DT,
CURRENT_YEAR_END_DT,
CURRENT_YEAR_NET_SALES_AMT,
CURRENT_YEAR_TRANSACTION_CNT,
CURRENT_YEAR_COMPARABLE_SALES_AMT,
CURRENT_YEAR_COMPARABLE_TRANSACTION_CNT,
FISCAL_LAST_YEAR_DT,
FISCAL_LAST_YEAR_START_DT,
FISCAL_LAST_YEAR_END_DT, 
FISCAL_LAST_YEAR_NET_SALES_AMT,
FISCAL_LAST_YEAR_TRANSACTION_CNT,
FISCAL_LAST_YEAR_COMPARABLE_SALES_AMT,
FISCAL_LAST_YEAR_COMPARABLE_TRANSACTION_CNT,
CALENDAR_LAST_YEAR_COMPARABLE_DT,
CALENDAR_LAST_YEAR_COMPARABLE_START_DT,
CALENDAR_LAST_YEAR_COMPARABLE_END_DT,
CALENDAR_LAST_YEAR_NET_SALES_AMT,
CALENDAR_LAST_YEAR_TRANSACTION_CNT,
CALENDAR_LAST_YEAR_COMPARABLE_SALES_AMT,
CALENDAR_LAST_YEAR_COMPARABLE_TRANSACTION_CNT,
CALENDAR_2YEAR_COMPARABLE_DT,
CALENDAR_2YEAR_COMPARABLE_START_DT,
CALENDAR_2YEAR_COMPARABLE_END_DT,
CALENDAR_2YEAR_NET_SALES_AMT,
CALENDAR_2YEAR_TRANSACTION_CNT,
CALENDAR_2YEAR_COMPARABLE_SALES_AMT,
CALENDAR_2YEAR_COMPARABLE_TRANSACTION_CNT,
CALENDAR_3YEAR_COMPARABLE_DT,
CALENDAR_3YEAR_COMPARABLE_START_DT,
CALENDAR_3YEAR_COMPARABLE_END_DT,
CALENDAR_3YEAR_NET_SALES_AMT,
CALENDAR_3YEAR_TRANSACTION_CNT,
CALENDAR_3YEAR_COMPARABLE_SALES_AMT,
CALENDAR_3YEAR_COMPARABLE_TRANSACTION_CNT,
LOAD_TYP,
LOAD_ID,
LOAD_DTTM,
UPDATE_ID,
UPDATE_DTTM
from IDS_DEV.TXN.INSPIRE_COMPARABLE_SALES;
create view IF NOT EXISTS LOYALTY_ORDER_BV(
	TRANSACTION_ID,
	MEMBER_ID,
	EMPLOYEE_ID,
	EMPLOYEE_NM,
	CHECK_NBR,
	ORDER_ID,
	MEMBER_CARD_NBR,
	DOLLAR_NET_VALUE_AMT,
	ELIGIBLE_REVENUE_AMT,
	STORE_ID,
	ATTACHMENT_METHOD_TYP,
	TRANSACTION_STATUS_TYP,
	MEMBER_PHONE_NBR,
	REASON_CD,
	MIN_DAY_PART_ID,
	MAX_DAY_PART_ID,
	SUSPENDED_TRANSACTION_ID,
	SUSPENDED_TRANSACTION_STATUS_IND,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	BUSINESS_DT,
	LOAD_TYP,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	FILENAME
) as SELECT
                               "TRANSACTION_ID", "MEMBER_ID", "EMPLOYEE_ID", "EMPLOYEE_NM", "CHECK_NBR", "ORDER_ID", "MEMBER_CARD_NBR", "DOLLAR_NET_VALUE_AMT", "ELIGIBLE_REVENUE_AMT", "STORE_ID", "ATTACHMENT_METHOD_TYP", "TRANSACTION_STATUS_TYP", "MEMBER_PHONE_NBR", "REASON_CD", "MIN_DAY_PART_ID", "MAX_DAY_PART_ID", "SUSPENDED_TRANSACTION_ID", "SUSPENDED_TRANSACTION_STATUS_IND", "BRAND_ID", "SOURCE_SYSTEM_NM", "BUSINESS_DT", "LOAD_TYP", "LOAD_ID", "LOAD_DTTM", "UPDATE_ID", "UPDATE_DTTM", "FILENAME"
                            FROM CDMSYNC_DEV.IRB."LOYALTY_ORDER";
create view IF NOT EXISTS LOYALTY_TRANSACTION_BV(
	TRANS_ID,
	ORDER_ID,
	EMPLOYEE_ID,
	PROFILE_ID,
	REST_ID,
	BRAND_ID,
	BUSINESS_DATE,
	EMPLOYEE_NAME,
	CHECK_NBR,
	MEMBER_CARD_NBR,
	DOLLAR_NET_VALUE,
	ELIGIBLE_REVENUE,
	METHOD_OF_ATTACHMENT,
	TRANS_STATUS,
	MEMBER_PHONE_NBR,
	REASON_CODE,
	MIN_DAY_PART_ID,
	MAX_DAY_PART_ID,
	SUSPEND_TRANS_ID,
	SUSPEND_TRANS_STATUS,
	SOURCE_SYSTEM_NM,
	LOAD_TYPE,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	FILE_NAME
) as 
SELECT c.TRANSACTION_ID AS TRANS_ID
, c.ORDER_ID AS ORDER_ID
, c.EMPLOYEE_ID AS EMPLOYEE_ID
, c.MEMBER_ID AS PROFILE_ID
, c.STORE_ID AS REST_ID
, c.BRAND_ID AS BRAND_ID
, try_to_date(c.BUSINESS_DT,'YYYYMMDD') AS BUSINESS_DATE -- cast added 2022-07-14
, c.EMPLOYEE_NM AS EMPLOYEE_NAME
, c.CHECK_NBR AS CHECK_NBR
, c.MEMBER_CARD_NBR AS MEMBER_CARD_NBR
, c.DOLLAR_NET_VALUE_AMT AS DOLLAR_NET_VALUE
, c.ELIGIBLE_REVENUE_AMT AS ELIGIBLE_REVENUE
, c.ATTACHMENT_METHOD_TYP AS METHOD_OF_ATTACHMENT
, c.TRANSACTION_STATUS_TYP AS TRANS_STATUS
, c.MEMBER_PHONE_NBR AS MEMBER_PHONE_NBR
, c.REASON_CD AS REASON_CODE
, c.MIN_DAY_PART_ID AS MIN_DAY_PART_ID
, c.MAX_DAY_PART_ID AS MAX_DAY_PART_ID
, c.SUSPENDED_TRANSACTION_ID AS SUSPEND_TRANS_ID
, c.SUSPENDED_TRANSACTION_STATUS_IND AS SUSPEND_TRANS_STATUS
, c.SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NM
, c.LOAD_TYP AS LOAD_TYPE
,try_to_number(c.UPDATE_ID) AS LOAD_ID -- cast added 2022-07-14
,try_to_timestamp(c.LOAD_DTTM) AS LOAD_DTTM -- cast added 2022-07-14
, try_to_number(c.UPDATE_ID) AS UPDATE_ID -- cast added 2022-07-14
, try_to_timestamp(c.UPDATE_DTTM) AS UPDATE_DTTM -- cast added 2022-07-14
, c.FILENAME AS FILE_NAME
FROM CDMSYNC_DEV.IRB.LOYALTY_ORDER c;
create view IF NOT EXISTS MY_TABLE_VP_BV(
	MY_COLUMN
) as SELECT
       my_column
    FROM my_table_vp;
create view IF NOT EXISTS ORDER_BV(
	ORDER_ID,
	EMPLOYEE_ID,
	BUSINESS_DT,
	STORE_ID,
	CHECK_NBR,
	OPENED_TM,
	CLOSED_TM,
	ORDER_NM,
	ITEM_CNT,
	TRANSACTION_CNT,
	SOURCE_GROSS_AMT,
	SOURCE_DISCOUNT_AMT,
	SOURCE_NET_AMT,
	SURCHARGE_AMT,
	TAX_AMT,
	SOURCE_PAYMENT_AMT,
	GRATUITY_AMT,
	CUSTOMER_NM,
	CUSTOMER_ID,
	LOYALTY_NBR,
	FIRST_SEND_TM,
	CLOSED_IND,
	FUTURE_ORDER_IND,
	VOID_IND,
	COMBO_TRANSACTION_IND,
	REFUND_IND,
	TAX_EXEMPT_IND,
	GUEST_CNT,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	TIME_KEY,
	LOAD_TYP,
	DERIVED_GROSS_AMT,
	DERIVED_DISCOUNT_AMT,
	DERIVED_NET_AMT,
	DERIVED_PAYMENT_AMT,
	MISC_CHARGE_AMT,
	CHANNEL_ID,
	ALTERNATE1_ORDER_ID,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	FILENAME
) as
                       (
                              SELECT
                                     ORDER_ID
                                   , EMPLOYEE_ID
                                   , TRY_TO_DATE( BUSINESS_DT, 'YYYYMMDD') as BUSINESS_DT
                                   , STORE_ID
                                   , CHECK_NBR
                                   , OPENED_TM
                                   , CLOSED_TM
                                   , ORDER_NM
                                   , ITEM_CNT
                                   , TRANSACTION_CNT
                                   , SOURCE_GROSS_AMT
                                   , SOURCE_DISCOUNT_AMT
                                   , SOURCE_NET_AMT
                                   , SURCHARGE_AMT
                                   , TAX_AMT
                                   , SOURCE_PAYMENT_AMT
                                   , GRATUITY_AMT
                                   , CUSTOMER_NM
                                   , CUSTOMER_ID
                                   , LOYALTY_NBR
                                   , FIRST_SEND_TM
                                   , CLOSED_IND
                                   , FUTURE_ORDER_IND
                                   , VOID_IND
								   , COMBO_TRANSACTION_IND
                                   , REFUND_IND
                                   , TAX_EXEMPT_IND
                                   , GUEST_CNT
                                   , BRAND_ID
                                   , SOURCE_SYSTEM_NM
                                   , TRY_TO_NUMBER(TIME_KEY) as TIME_KEY
                                   , LOAD_TYP
                                   , DERIVED_GROSS_AMT
                                   , DERIVED_DISCOUNT_AMT
                                   , DERIVED_NET_AMT
                                   , DERIVED_PAYMENT_AMT
                                   , MISC_CHARGE_AMT
                                   , CHANNEL_ID
                                   , ALTERNATE1_ORDER_ID
                                   , TRY_TO_NUMBER(REPLACE(LOAD_ID, '-', '')) LOAD_ID
                                   , LOAD_DTTM
                                   , TRY_TO_NUMBER(REPLACE(UPDATE_ID, '-', '')) UPDATE_ID
                                   , UPDATE_DTTM
                                   , FILENAME
                              FROM
                                     CDMSYNC_dev.IRB."ORDER"
                              UNION ALL
                              SELECT
                                     ORDER_ID
                                   , EMPLOYEE_ID
                                   , BUSINESS_DT as  BUSINESS_DT
                                   , STORE_ID
                                   , CHECK_NBR
                                   , TO_VARCHAR(OPENED_TM, 'YYYY-MM-DD HH:MI:SS') OPENED_TM
                                   , TO_VARCHAR(CLOSED_TM, 'YYYY-MM-DD HH:MI:SS') CLOSED_TM
                                   , ORDER_NM
                                   , ITEM_CNT
                                   , TRANSACTION_CNT
                                   , SOURCE_GROSS_AMT
                                   , SOURCE_DISCOUNT_AMT
                                   , SOURCE_NET_AMT
                                   , SURCHARGE_AMT
                                   , TAX_AMT
                                   , SOURCE_PAYMENT_AMT
                                   , GRATUITY_AMT
                                   , CUSTOMER_NM
                                   , CUSTOMER_ID
                                   , LOYALTY_NBR
                                   , TO_VARCHAR(FIRST_SEND_TM, 'YYYY-MM-DD HH:MI:SS') FIRST_SEND_TM
                                   , CASE
                                            WHEN CLOSED_IND = 'FALSE'
                                                   THEN 'N'
                                            WHEN CLOSED_IND = 'TRUE'
                                                   THEN 'Y'
                                     END AS CLOSED_IND
                                   , CASE
                                            WHEN FUTURE_ORDER_IND = 'FALSE'
                                                   THEN 'N'
                                            WHEN FUTURE_ORDER_IND = 'TRUE'
                                                   THEN 'Y'
                                     END AS FUTURE_ORDER_IND
                                   , CASE
                                            WHEN VOID_IND = 'FALSE'
                                                   THEN 'N'
                                            WHEN VOID_IND = 'TRUE'
                                                   THEN 'Y'
                                     END AS VOID_IND
									, NULL COMBO_TRANSACTION_IND 
                                   , CASE
                                            WHEN REFUND_IND = 'FALSE'
                                                   THEN 'N'
                                            WHEN REFUND_IND = 'TRUE'
                                                   THEN 'Y'
                                     END AS REFUND_IND
                                   , CASE
                                            WHEN TAX_EXEMPT_IND = 'FALSE'
                                                   THEN 'N'
                                            WHEN TAX_EXEMPT_IND = 'TRUE'
                                                   THEN 'Y'
                                     END AS TAX_EXEMPT_IND
                                   , GUEST_CNT
                                   , BRAND_ID
                                   , SOURCE_SYSTEM_NM
                                   , TIME_KEY
                                   , LOAD_TYP
                                   , DERIVED_GROSS_AMT
                                   , DERIVED_DISCOUNT_AMT
                                   , DERIVED_NET_AMT
                                   , DERIVED_PAYMENT_AMT
                                   , MISC_CHARGE_AMT
                                   , CHANNEL_ID
                                   , ALTERNATE1_ORDER_ID
                                   , LOAD_ID
                                   , TO_VARCHAR(LOAD_DTTM, 'YYYY-MM-DD HH:MI:SS') LOAD_DTTM
                                   , UPDATE_ID
                                   , TO_VARCHAR(UPDATE_DTTM, 'YYYY-MM-DD HH:MI:SS')    UPDATE_DTTM
                                   , NULL                                           AS FILENAME
                              FROM
                                     IDS_dev.TXN."ORDER"
                       )
;
create view IF NOT EXISTS ORDER_LINE_BV(
	ORDER_LINE_ID,
	ORDER_ID,
	ORDER_LINE_TYP,
	CHANNEL_ID,
	MDM_ITEM_ID,
	MDM_PARENT_ITEM_ID,
	BUSINESS_DT,
	TIME_KEY,
	STORE_ID,
	EMPLOYEE_ID,
	REGISTER_ID,
	TAX_ID,
	SEAT_NBR,
	SOURCE_GROSS_QTY,
	PRICE_AMT,
	SOURCE_GROSS_AMT,
	SOURCE_NET_AMT,
	TAX_AMT,
	CLEARED_IND,
	DELETED_IND,
	VOID_IND,
	INVENTORY_IND,
	MODIFIER_IND,
	TAX_EXEMPT_ID,
	MANAGER_ID,
	DERIVED_GROSS_QTY,
	DERIVED_GROSS_AMT,
	DERIVED_DISCOUNT_AMT,
	DERIVED_NET_AMT,
	SOURCE_ITEM_ID,
	SOURCE_ITEM_PLU_ID,
	SOURCE_ITEM_DESC,
	SOURCE_PARENT_ITEM_ID,
	DISCOUNT_CD,
	DISCOUNT_NM,
	SOURCE_DISCOUNT_AMT,
	SOURCE_DISCOUNT_TYP,
	ORDER_LINE_PARENT_ID,
	MISC_CHARGE_AMT,
	ITEM_TYPE_CD,
	ALTERNATE1_ORDER_ID,
	BRAND_ID,
	SOURCE_SYSTEM_NM,
	LOAD_TYP,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	FILENAME
) as
                       
                              SELECT
                                     ORDER_LINE_ID
                                   , ORDER_ID
                                   , ORDER_LINE_TYP
                                   , CHANNEL_ID
                                   , TRY_TO_NUMBER(MDM_ITEM_ID) as MDM_ITEM_ID
                                   , TRY_TO_NUMBER(MDM_PARENT_ITEM_ID) as MDM_PARENT_ITEM_ID
                                   , TRY_TO_DATE(BUSINESS_DT,'YYYYMMDD') AS BUSINESS_DT
                                   , TRY_TO_NUMBER(TIME_KEY) as TIME_KEY
                                   , STORE_ID
                                   , EMPLOYEE_ID
                                   , REGISTER_ID
                                   , TAX_ID
                                   , SEAT_NBR
                                   , TRY_TO_NUMBER(SOURCE_GROSS_QTY) AS SOURCE_GROSS_QTY
                                   , TRY_TO_DOUBLE(PRICE_AMT) AS PRICE_AMT
                                   , TRY_TO_DOUBLE(SOURCE_GROSS_AMT) AS SOURCE_GROSS_AMT
                                   , TRY_TO_DOUBLE(SOURCE_NET_AMT) AS SOURCE_NET_AMT
                                   , TRY_TO_DOUBLE(TAX_AMT) AS TAX_AMT
                                   , TRY_TO_BOOLEAN(CLEARED_IND) AS CLEARED_IND
                                   , TRY_TO_BOOLEAN(DELETED_IND) AS DELETED_IND
                                   , TRY_TO_BOOLEAN(VOID_IND) AS VOID_IND
                                   , TRY_TO_BOOLEAN(INVENTORY_IND) AS INVENTORY_IND
                                   , TRY_TO_BOOLEAN(MODIFIER_IND) AS MODIFIER_IND
                                   , TAX_EXEMPT_ID
                                   , MANAGER_ID
                                   , TRY_TO_NUMBER(DERIVED_GROSS_QTY) AS DERIVED_GROSS_QTY
                                   , TRY_TO_DOUBLE(DERIVED_GROSS_AMT) AS DERIVED_GROSS_AMT
                                   , TRY_TO_DOUBLE(DERIVED_DISCOUNT_AMT) AS DERIVED_DISCOUNT_AMT
                                   , TRY_TO_DOUBLE(DERIVED_NET_AMT) AS DERIVED_NET_AMT
                                   , SOURCE_ITEM_ID
                                   , SOURCE_ITEM_PLU_ID
                                   , SOURCE_ITEM_DESC
                                   , SOURCE_PARENT_ITEM_ID
                                   , DISCOUNT_CD
                                   , DISCOUNT_NM
                                   , TRY_TO_DOUBLE(SOURCE_DISCOUNT_AMT) AS SOURCE_DISCOUNT_AMT
                                   , SOURCE_DISCOUNT_TYP
                                   , ORDER_LINE_PARENT_ID
                                   , TRY_TO_DOUBLE(MISC_CHARGE_AMT) AS MISC_CHARGE_AMT
                                   , ITEM_TYPE_CD
                                   , ALTERNATE1_ORDER_ID
                                   , BRAND_ID
                                   , SOURCE_SYSTEM_NM
                                   , LOAD_TYP
                                   , TRY_TO_NUMBER(REPLACE(LOAD_ID, '-', '')) LOAD_ID
                                   , TRY_TO_TIMESTAMP(LOAD_DTTM) as LOAD_DTTM
                                   , TRY_TO_NUMBER(REPLACE(UPDATE_ID, '-', '')) UPDATE_ID
                                   , TRY_TO_TIMESTAMP(UPDATE_DTTM) as UPDATE_DTTM
                                   , FILENAME
                              FROM
                                     CDMSYNC_DEV.IRB.ORDER_LINE
                              UNION ALL
                              SELECT
                                     ORDER_LINE_ID
                                   , ORDER_ID
                                   , ORDER_LINE_TYP
                                   , CHANNEL_ID
                                   , MDM_ITEM_ID
                                   , MDM_PARENT_ITEM_ID
                                   , BUSINESS_DT BUSINESS_DT
                                   , TIME_KEY
                                   , STORE_ID
                                   , EMPLOYEE_ID
                                   , REGISTER_ID
                                   , TAX_ID
                                   , SEAT_NBR
                                   , SOURCE_GROSS_QTY
                                   , PRICE_AMT
                                   , SOURCE_GROSS_AMT
                                   , SOURCE_NET_AMT
                                   , TAX_AMT
                                   , CLEARED_IND AS CLEARED_IND
                                   , DELETED_IND AS DELETED_IND
                                   , VOID_IND AS VOID_IND
                                   , INVENTORY_IND AS INVENTORY_IND
                                   , MODIFIER_IND AS MODIFIER_IND
                                   , TAX_EXEMPT_ID
                                   , MANAGER_ID
                                   , DERIVED_GROSS_QTY
                                   , DERIVED_GROSS_AMT
                                   , DERIVED_DISCOUNT_AMT
                                   , DERIVED_NET_AMT
                                   , SOURCE_ITEM_ID
                                   , SOURCE_ITEM_PLU_ID
                                   , SOURCE_ITEM_DESC
                                   , SOURCE_PARENT_ITEM_ID
                                   , DISCOUNT_CD
                                   , DISCOUNT_NM
                                   , SOURCE_DISCOUNT_AMT
                                   , SOURCE_DISCOUNT_TYP
                                   , ORDER_LINE_PARENT_ID
                                   , MISC_CHARGE_AMT
                                   , ITEM_TYPE_CD
                                   , ALTERNATE1_ORDER_ID
                                   , BRAND_ID
                                   , SOURCE_SYSTEM_NM
                                   , LOAD_TYP
                                   , LOAD_ID
                                   , LOAD_DTTM as LOAD_DTTM
                                   , UPDATE_ID
                                   , UPDATE_DTTM as UPDATE_DTTM
                                   , NULL FILENAME
                              FROM
                                     IDS_DEV.TXN.ORDER_LINE
                       
;
create view IF NOT EXISTS ORDER_PAYMENT_BV(
	PAYMENT_ID,
	ORDER_ID,
	EMPLOYEE_ID,
	PAYMENT_TYP,
	BRAND_ID,
	STORE_ID,
	BUSINESS_DT,
	VOID_IND,
	INSPIRE_ID,
	ACCOUNT_NM,
	CARD_TYP,
	CARD_ISSUER_NM,
	PAYMENT_FIRST6_NBR,
	PAYMENT_LAST4_NBR,
	PAYMENT_AUTH_CD,
	EXPIRATION_DT,
	PAYMENT_AMT,
	AMT,
	TRANSACTION_TM,
	SOURCE_SYSTEM_NM,
	LOAD_TYP,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	FILENAME
) as
                       (
                              SELECT
                                     PAYMENT_ID
                                   , ORDER_ID
                                   , EMPLOYEE_ID
                                   , PAYMENT_TYP
                                   , BRAND_ID
                                   , STORE_ID
                                   , TRY_TO_DATE(BUSINESS_DT,'YYYYMMDD') AS BUSINESS_DT
                                   , TRY_TO_BOOLEAN(VOID_IND) AS VOID_IND
                                   , INSPIRE_ID
                                   , ACCOUNT_NM
                                   , CARD_TYP
                                   , CARD_ISSUER_NM
                                   , PAYMENT_FIRST6_NBR
                                   , PAYMENT_LAST4_NBR
                                   , PAYMENT_AUTH_CD
                                   , TRY_TO_DATE(EXPIRATION_DT) AS EXPIRATION_DT
                                   , TRY_TO_DOUBLE(PAYMENT_AMT) AS PAYMENT_AMT
                                   , TRY_TO_DOUBLE(AMT) AS AMT
                                   , TRANSACTION_TM
                                   , SOURCE_SYSTEM_NM
                                   , LOAD_TYP
                                   , REPLACE(LOAD_ID, '-', '') LOAD_ID
                                   , TRY_TO_TIMESTAMP(LOAD_DTTM) as load_dttm
                                   , REPLACE(UPDATE_ID, '-', '') UPDATE_ID
                                   , TRY_TO_TIMESTAMP(UPDATE_DTTM) as UPDATE_DTTM
                                   , FILENAME
                              FROM
                                     CDMSYNC_DEV.IRB."ORDER_PAYMENT"
                              UNION
                              SELECT
                                     PAYMENT_ID
                                   , ORDER_ID
                                   , EMPLOYEE_ID
                                   , PAYMENT_TYP
                                   , BRAND_ID
                                   , STORE_ID
                                   , BUSINESS_DT BUSINESS_DT
                                   , VOID_IND AS VOID_IND
                                   , INSPIRE_ID
                                   , ACCOUNT_NM
                                   , CARD_TYP
                                   , CARD_ISSUER_NM
                                   , PAYMENT_FIRST6_NBR
                                   , PAYMENT_LAST4_NBR
                                   , PAYMENT_AUTH_CD
                                   , EXPIRATION_DT AS EXPIRATION_DT
                                   , PAYMENT_AMT
                                   , AMT
                                   , TRANSACTION_TM
                                   , SOURCE_SYSTEM_NM
                                   , LOAD_TYP
                                   , LOAD_ID
                                   , LOAD_DTTM
                                   , UPDATE_ID
                                   , UPDATE_DTTM
                                   , NULL                                           AS FILENAME
                              FROM
                                     IDS_DEV.TXN."ORDER_PAYMENT"
                       )
;
create view IF NOT EXISTS TESTCOMMENT_BV(
	ID,
	NAME,
	LOCATION COMMENT 'Location in table'
) as SELECT
                               ID, NAME, LOCATION
                            FROM CDMSYNC_DEV.IRB."TESTCOMMENT";
create view IF NOT EXISTS TESTCOMMENT_V(
	V_ID,
	V_LOCATION COMMENT 'Location in view'
) as select id, location from testcomment;
create view IF NOT EXISTS TESTCOMMENT_V_AS_EXAMPLE(
	ID,
	LOCATION
) as select id as id, location as location from testcomment;
create view IF NOT EXISTS TRANS_LINE_ITEM_PREP_BV(
	REST_ID,
	BUMP_DTTM,
	SEND_DTTM,
	FIRE_DTTM,
	ORDER_LINE_PREP_LOC_ID,
	ORDER_LINE_ID,
	ORDER_ID,
	CDM_LOAD_DATE,
	SOURCE_SYSTEM_NAME,
	BRAND_ID,
	LOAD_ID,
	LOAD_DTTM
) as 
SELECT c.STOREID AS REST_ID
,try_to_timestamp(c.BUMPTIME) AS BUMP_DTTM
,try_to_timestamp(c.SENDTIME) AS SEND_DTTM
,try_to_timestamp(case when c.FIRETIME = '' then null else c.FIRETIME end) AS FIRE_DTTM
,c.ORDERLINEPREPLOCATIONID AS ORDER_LINE_PREP_LOC_ID
,c.ORDERLINEID AS ORDER_LINE_ID
,c.ORDERID AS ORDER_ID
,try_to_date(c.CDMLOADDATE,'YYYYMMDD') AS CDM_LOAD_DATE
,c.SOURCE AS SOURCE_SYSTEM_NAME
,c.BRANDID AS BRAND_ID
,try_to_number(c.CDMLOADDATE) AS LOAD_ID
,try_to_timestamp(c.CDMLOADDATE,'YYYYMMDD') AS LOAD_DTTM
FROM CDMSYNC_DEV.IRB.TRAN_ORDERLINEPREP_PLR c;
create view IF NOT EXISTS TRAN_CHANNEL_PLR_BV(
	MDMORDERCHANNELHIERARCHYID,
	SOURCE,
	ORDERCHANNELNAME,
	BRANDID,
	FULFILLMENTCHANNELNAME,
	FILENAME,
	MDMFULFILLMENTCHANNELHIERARCHYID,
	CHANNELID,
	PARTITIONCREATIONBRAND,
	CDMLOADDATE,
	CDMUPDATEDATE
) as SELECT
                               MDMORDERCHANNELHIERARCHYID, SOURCE, ORDERCHANNELNAME, BRANDID, FULFILLMENTCHANNELNAME, FILENAME, MDMFULFILLMENTCHANNELHIERARCHYID, CHANNELID, PARTITIONCREATIONBRAND, CDMLOADDATE, CDMUPDATEDATE
                            FROM CDMSYNC_DEV.IRB."TRAN_CHANNEL_PLR";
create view IF NOT EXISTS TRAN_DESTINATION_PLR_BV(
	FILENAME,
	BRANDID,
	DESTINATIONID,
	DESTINATIONNAME,
	SOURCE,
	BUSINESSDATE,
	CDMLOADDATE,
	ORDERID,
	STOREID
) as SELECT
                               FILENAME, BRANDID, DESTINATIONID, DESTINATIONNAME, SOURCE, BUSINESSDATE, CDMLOADDATE, ORDERID, STOREID
                            FROM CDMSYNC_DEV.IRB."TRAN_DESTINATION_PLR";
create view IF NOT EXISTS TRAN_DISCOUNT_PLR_BV(
	DISCOUNTCODE,
	DISCOUNTNAME,
	STOREID,
	ISACTIVE,
	ORDERLINEID,
	BRANDID,
	SOURCE,
	CDMLOADDATE,
	DISCOUNTTYPE,
	DISCOUNTAMOUNT,
	COMP,
	ORDERID,
	FILENAME
) as SELECT
                               DISCOUNTCODE, DISCOUNTNAME, STOREID, ISACTIVE, ORDERLINEID, BRANDID, SOURCE, CDMLOADDATE, DISCOUNTTYPE, DISCOUNTAMOUNT, COMP, ORDERID, FILENAME
                            FROM CDMSYNC_DEV.IRB."TRAN_DISCOUNT_PLR";
create view IF NOT EXISTS TRAN_EMPLOYEE_PLR_BV(
	CDMLOADDATE,
	FIRSTNAME,
	STATE,
	MANAGERID,
	BRANDID,
	SOURCE,
	LASTNAME,
	ZIPCODE,
	EMPLOYEEID,
	CITY,
	FILENAME,
	DATEOFHIRE,
	STOREID
) as SELECT
                               CDMLOADDATE, FIRSTNAME, STATE, MANAGERID, BRANDID, SOURCE, LASTNAME, ZIPCODE, EMPLOYEEID, CITY, FILENAME, DATEOFHIRE, STOREID
                            FROM CDMSYNC_DEV.IRB."TRAN_EMPLOYEE_PLR";
create view IF NOT EXISTS TRAN_FULFILLMENTCHANNEL_HIERARCHY_PLR_BV(
	CDMLOADDATE,
	CDMUPDATEDATE,
	MDMFULFILLMENTCHANNELHIERARCHYID,
	FULFILLMENTCHANNELHIERARCHYTYPE,
	SOURCE,
	FULFILLMENTCHANNELHIERARCHYNAME,
	FILENAME,
	PARENTFULFILLMENTCHANNELHIERARCHYID,
	BRANDID
) as SELECT
                               CDMLOADDATE, CDMUPDATEDATE, MDMFULFILLMENTCHANNELHIERARCHYID, FULFILLMENTCHANNELHIERARCHYTYPE, SOURCE, FULFILLMENTCHANNELHIERARCHYNAME, FILENAME, PARENTFULFILLMENTCHANNELHIERARCHYID, BRANDID
                            FROM CDMSYNC_DEV.IRB."TRAN_FULFILLMENTCHANNEL_HIERARCHY_PLR";
create view IF NOT EXISTS TRAN_LOYALTY_DISCOUNT_PLR_BV(
	SOURCE,
	TRANDISCOUNTID,
	TRANSACTIONID,
	BRANDID,
	CDMLOADDATE,
	OFFERCODE,
	DISCOUNTAMOUNT,
	SOURCEITEMID,
	FILENAME,
	DISCOUNTDESC
) as SELECT
                               SOURCE, TRANDISCOUNTID, TRANSACTIONID, BRANDID, CDMLOADDATE, OFFERCODE, DISCOUNTAMOUNT, SOURCEITEMID, FILENAME, DISCOUNTDESC
                            FROM CDMSYNC_DEV.IRB."TRAN_LOYALTY_DISCOUNT_PLR";
create view IF NOT EXISTS TRAN_LOYALTY_TRANSACTIONS_PLR_BV(
	RESTAURANTNUMBER,
	BUSINESSDATE,
	MEMBERCARDNUMBER,
	MINDAYPARTID,
	CDMLOADDATE,
	ELIGIBLEREVENUE,
	MAXDAYPARTID,
	MEMBERID,
	TRANSSTATUS,
	CHECKNUMBER,
	FILENAME,
	MEMBERPHONENUMBER,
	ORDERID,
	SUSPENDTXNSTATUS,
	SUSPENDTXNID,
	METHODOFATTACHMENT,
	REASONCODE,
	EMPLOYEENAME,
	TRANSACTIONID,
	EMPLOYEEID,
	BRANDID,
	DOLLARNETVALUE,
	SOURCE
) as SELECT
                               RESTAURANTNUMBER, BUSINESSDATE, MEMBERCARDNUMBER, MINDAYPARTID, CDMLOADDATE, ELIGIBLEREVENUE, MAXDAYPARTID, MEMBERID, TRANSSTATUS, CHECKNUMBER, FILENAME, MEMBERPHONENUMBER, ORDERID, SUSPENDTXNSTATUS, SUSPENDTXNID, METHODOFATTACHMENT, REASONCODE, EMPLOYEENAME, TRANSACTIONID, EMPLOYEEID, BRANDID, DOLLARNETVALUE, SOURCE
                            FROM CDMSYNC_DEV.IRB."TRAN_LOYALTY_TRANSACTIONS_PLR";
create view IF NOT EXISTS TRAN_ORDERCHANNEL_HIERARCHY_PLR_BV(
	CDMUPDATEDATE,
	ORDERCHANNELHIERARCHYNAME,
	MDMORDERCHANNELHIERARCHYID,
	ORDERCHANNELHIERARCHYTYPE,
	SOURCE,
	FILENAME,
	PARENTORDERCHANNELHIERARCHYID,
	BRANDID,
	CDMLOADDATE
) as SELECT
                               CDMUPDATEDATE, ORDERCHANNELHIERARCHYNAME, MDMORDERCHANNELHIERARCHYID, ORDERCHANNELHIERARCHYTYPE, SOURCE, FILENAME, PARENTORDERCHANNELHIERARCHYID, BRANDID, CDMLOADDATE
                            FROM CDMSYNC_DEV.IRB."TRAN_ORDERCHANNEL_HIERARCHY_PLR";
create view IF NOT EXISTS TRAN_ORDERLINEPREPLOOKUP_PLR_BV(
	CDMLOADDATE,
	FILENAME,
	ORDERLINEPREPLOCATIONID,
	ORDERPREPLOCATIONDESC,
	SOURCE,
	BRANDID
) as SELECT
                               CDMLOADDATE, FILENAME, ORDERLINEPREPLOCATIONID, ORDERPREPLOCATIONDESC, SOURCE, BRANDID
                            FROM CDMSYNC_DEV.IRB."TRAN_ORDERLINEPREPLOOKUP_PLR";
create view IF NOT EXISTS TRAN_ORDERLINEPREP_PLR_BV(
	ORDERLINEPREPLOCATIONID,
	FILENAME,
	ORDERLINEID,
	STOREID,
	BRANDID,
	FIRETIME,
	ORDERID,
	CDMLOADDATE,
	SENDTIME,
	BUMPTIME,
	SOURCE
) as SELECT
                               ORDERLINEPREPLOCATIONID, FILENAME, ORDERLINEID, STOREID, BRANDID, FIRETIME, ORDERID, CDMLOADDATE, SENDTIME, BUMPTIME, SOURCE
                            FROM CDMSYNC_DEV.IRB."TRAN_ORDERLINEPREP_PLR";
create view IF NOT EXISTS TRAN_ORDERLINE_PLR_BV(
	CDMLOADDATE,
	ORDERLINEID,
	TIMEKEY,
	LOADTYPE,
	TAXID,
	MDMPARENTITEMID,
	TAXEXEMPTID,
	MDMITEMID,
	BUSINESSDATE,
	TAXAMOUNT,
	DISCOUNTPRICE,
	MANAGERID,
	NETAMOUNT,
	BRANDID,
	GROSSQUANTITY,
	ISVOIDED,
	SOURCE,
	ISINVENTORY,
	MODIFIERID,
	STOREID,
	CHANNELID,
	EMPLOYEEID,
	INCLUSIVETAX,
	ISCLEARED,
	SEATNUMBER,
	PRICE,
	REGISTERID,
	GROSSAMOUNT,
	ISDISCOUNTED,
	REVENUECENTER,
	FILENAME,
	ISDELETED,
	ORDERID
) as SELECT
                               CDMLOADDATE, ORDERLINEID, TIMEKEY, LOADTYPE, TAXID, MDMPARENTITEMID, TAXEXEMPTID, MDMITEMID, BUSINESSDATE, TAXAMOUNT, DISCOUNTPRICE, MANAGERID, NETAMOUNT, BRANDID, GROSSQUANTITY, ISVOIDED, SOURCE, ISINVENTORY, MODIFIERID, STOREID, CHANNELID, EMPLOYEEID, INCLUSIVETAX, ISCLEARED, SEATNUMBER, PRICE, REGISTERID, GROSSAMOUNT, ISDISCOUNTED, REVENUECENTER, FILENAME, ISDELETED, ORDERID
                            FROM CDMSYNC_DEV.IRB."TRAN_ORDERLINE_PLR";
create view IF NOT EXISTS TRAN_ORDERLOYALTY_REL_PLR_BV(
	ORDERID,
	STOREID,
	BUSINESSDATE,
	FILENAME,
	CDMLOADDATE,
	BRANDID,
	CDMUPDATEDATE,
	SOURCE,
	IDENCODED,
	LOADTYPE
) as SELECT
                               ORDERID, STOREID, BUSINESSDATE, FILENAME, CDMLOADDATE, BRANDID, CDMUPDATEDATE, SOURCE, IDENCODED, LOADTYPE
                            FROM CDMSYNC_DEV.IRB."TRAN_ORDERLOYALTY_REL_PLR";
create view IF NOT EXISTS TRAN_ORDER_PLR_BV(
	ORDERID,
	EMPLOYEEID,
	BUSINESSDATE,
	STOREID,
	CHECKNUMBER,
	OPENEDTIME,
	CLOSEDTIME,
	ORDERNAME,
	GROSSQUANTITY,
	GROSSAMOUNT,
	DISCOUNTAMOUNT,
	NETAMOUNT,
	SURCHARGEAMOUNT,
	TAXAMOUNT,
	PAYMENTAMOUNT,
	GRATUITY,
	CUSTOMERNAME,
	CUSTOMERID,
	LOYALTYNUMBER,
	FIRSTSENDTIME,
	ISCLOSED,
	ISFUTUREORDER,
	ISVOID,
	ISREFUND,
	ISTAXEXEMPT,
	GUESTCOUNT,
	CDMLOADDATE,
	BRANDID,
	SOURCE,
	TIMEKEY,
	RESTAURANTKEY,
	LOADTYPE,
	INSPIREID,
	FILENAME
) as SELECT
                               ORDERID, EMPLOYEEID, BUSINESSDATE, STOREID, CHECKNUMBER, OPENEDTIME, CLOSEDTIME, ORDERNAME, GROSSQUANTITY, GROSSAMOUNT, DISCOUNTAMOUNT, NETAMOUNT, SURCHARGEAMOUNT, TAXAMOUNT, PAYMENTAMOUNT, GRATUITY, CUSTOMERNAME, CUSTOMERID, LOYALTYNUMBER, FIRSTSENDTIME, ISCLOSED, ISFUTUREORDER, ISVOID, ISREFUND, ISTAXEXEMPT, GUESTCOUNT, CDMLOADDATE, BRANDID, SOURCE, TIMEKEY, RESTAURANTKEY, LOADTYPE, INSPIREID, FILENAME
                            FROM CDMSYNC_DEV.IRB."TRAN_ORDER_PLR";
create view IF NOT EXISTS TRAN_ORDER_PLR_MV_BV(
	ORDERID,
	EMPLOYEEID,
	BUSINESSDATE,
	STOREID,
	CHECKNUMBER,
	OPENEDTIME,
	CLOSEDTIME,
	ORDERNAME,
	GROSSQUANTITY,
	GROSSAMOUNT,
	DISCOUNTAMOUNT,
	NETAMOUNT,
	SURCHARGEAMOUNT,
	TAXAMOUNT,
	PAYMENTAMOUNT,
	GRATUITY,
	CUSTOMERNAME,
	CUSTOMERID,
	LOYALTYNUMBER,
	FIRSTSENDTIME,
	ISCLOSED,
	ISFUTUREORDER,
	ISVOID,
	ISREFUND,
	ISTAXEXEMPT,
	GUESTCOUNT,
	CDMLOADDATE,
	BRANDID,
	SOURCE,
	TIMEKEY,
	RESTAURANTKEY,
	LOADTYPE,
	INSPIREID
) as SELECT
                               ORDERID, EMPLOYEEID, BUSINESSDATE, STOREID, CHECKNUMBER, OPENEDTIME, CLOSEDTIME, ORDERNAME, GROSSQUANTITY, GROSSAMOUNT, DISCOUNTAMOUNT, NETAMOUNT, SURCHARGEAMOUNT, TAXAMOUNT, PAYMENTAMOUNT, GRATUITY, CUSTOMERNAME, CUSTOMERID, LOYALTYNUMBER, FIRSTSENDTIME, ISCLOSED, ISFUTUREORDER, ISVOID, ISREFUND, ISTAXEXEMPT, GUESTCOUNT, CDMLOADDATE, BRANDID, SOURCE, TIMEKEY, RESTAURANTKEY, LOADTYPE, INSPIREID
                            FROM CDMSYNC_DEV.IRB."TRAN_ORDER_PLR_MV";
create view IF NOT EXISTS TRAN_PARTY_PLR_BV(
	FILENAME,
	REVENUECENTER,
	TABLEID,
	DEPARTTIME,
	EMPLOYEEID,
	JOBCODEID,
	TABLEDESCRIPTION,
	BRANDID,
	CDMLOADDATE,
	DATEOFBUSINESS,
	NUMBEROFCHECKS,
	TIMEARRIVED,
	NUMBEROFGUESTS,
	TABLESALES,
	TIMECLOSED,
	TIMEOPENED,
	SOURCE,
	BUSINESSDATE,
	CHECKID,
	LOADTYPE,
	ORDERID,
	STOREID,
	TIMESEATED,
	BUSTIME
) as SELECT
                               FILENAME, REVENUECENTER, TABLEID, DEPARTTIME, EMPLOYEEID, JOBCODEID, TABLEDESCRIPTION, BRANDID, CDMLOADDATE, DATEOFBUSINESS, NUMBEROFCHECKS, TIMEARRIVED, NUMBEROFGUESTS, TABLESALES, TIMECLOSED, TIMEOPENED, SOURCE, BUSINESSDATE, CHECKID, LOADTYPE, ORDERID, STOREID, TIMESEATED, BUSTIME
                            FROM CDMSYNC_DEV.IRB."TRAN_PARTY_PLR";
create view IF NOT EXISTS TRAN_PAYMENTTYPE_PLR_BV(
	CDMLOADDATE,
	PAYMENTTYPE,
	FILENAME,
	BRANDID,
	PAYMENTTYPEDESC,
	SOURCE,
	PAYMENTTYPEID
) as SELECT
                               CDMLOADDATE, PAYMENTTYPE, FILENAME, BRANDID, PAYMENTTYPEDESC, SOURCE, PAYMENTTYPEID
                            FROM CDMSYNC_DEV.IRB."TRAN_PAYMENTTYPE_PLR";
create view IF NOT EXISTS TRAN_PAYMENT_PLR_BV(
	PAYMENTID,
	ORDERID,
	BUSINESSDATE,
	EMPLOYEEID,
	PAYMENTAUTHID,
	CARDTYPE,
	CARDISSUER,
	ACCOUNTNAME,
	AMOUNT,
	PAYMENTAMOUNT,
	PAYMENTFIRST6,
	PAYMENTLAST4,
	EXPIRATIONDATE,
	STOREID,
	PAYMENTTYPE,
	ISVOID,
	INSPIREID,
	BRANDID,
	SOURCE,
	CDMLOADDATE,
	LOADTYPE,
	TRANSACTIONTIME,
	FILENAME
) as SELECT
                               PAYMENTID, ORDERID, BUSINESSDATE, EMPLOYEEID, PAYMENTAUTHID, CARDTYPE, CARDISSUER, ACCOUNTNAME, AMOUNT, PAYMENTAMOUNT, PAYMENTFIRST6, PAYMENTLAST4, EXPIRATIONDATE, STOREID, PAYMENTTYPE, ISVOID, INSPIREID, BRANDID, SOURCE, CDMLOADDATE, LOADTYPE, TRANSACTIONTIME, FILENAME
                            FROM CDMSYNC_DEV.IRB."TRAN_PAYMENT_PLR";
create view IF NOT EXISTS TRAN_PAYMENT_PLR_MV_BV(
	PAYMENTID,
	ORDERID,
	BUSINESSDATE,
	EMPLOYEEID,
	PAYMENTAUTHID,
	CARDTYPE,
	CARDISSUER,
	ACCOUNTNAME,
	AMOUNT,
	PAYMENTAMOUNT,
	PAYMENTFIRST6,
	PAYMENTLAST4,
	EXPIRATIONDATE,
	STOREID,
	PAYMENTTYPE,
	ISVOID,
	INSPIREID,
	BRANDID,
	SOURCE,
	CDMLOADDATE,
	LOADTYPE,
	TRANSACTIONTIME
) as SELECT
                               PAYMENTID, ORDERID, BUSINESSDATE, EMPLOYEEID, PAYMENTAUTHID, CARDTYPE, CARDISSUER, ACCOUNTNAME, AMOUNT, PAYMENTAMOUNT, PAYMENTFIRST6, PAYMENTLAST4, EXPIRATIONDATE, STOREID, PAYMENTTYPE, ISVOID, INSPIREID, BRANDID, SOURCE, CDMLOADDATE, LOADTYPE, TRANSACTIONTIME
                            FROM CDMSYNC_DEV.IRB."TRAN_PAYMENT_PLR_MV";
create view IF NOT EXISTS TRAN_PRODUCT_PLR_BV(
	SOURCE,
	PRODUCTPLU,
	BRANDID,
	PRODUCTPLUDESC,
	CDMLOADDATE,
	PRODUCTID,
	FILENAME
) as SELECT
                               SOURCE, PRODUCTPLU, BRANDID, PRODUCTPLUDESC, CDMLOADDATE, PRODUCTID, FILENAME
                            FROM CDMSYNC_DEV.IRB."TRAN_PRODUCT_PLR";
create view IF NOT EXISTS TRAN_PROMOTION_PLR_BV(
	ISACTIVE,
	PROMONAME,
	STOREID,
	PROMOTIONTYPE,
	FILENAME,
	PROMOID,
	SOURCE,
	CDMLOADDATE,
	BRANDID
) as SELECT
                               ISACTIVE, PROMONAME, STOREID, PROMOTIONTYPE, FILENAME, PROMOID, SOURCE, CDMLOADDATE, BRANDID
                            FROM CDMSYNC_DEV.IRB."TRAN_PROMOTION_PLR";
create view IF NOT EXISTS TRAN_REGISTER_PLR_BV(
	CDMLOADDATE,
	REGISTERID,
	SOURCE,
	STOREID,
	REGISTERNAME,
	BRANDID,
	FILENAME
) as SELECT
                               CDMLOADDATE, REGISTERID, SOURCE, STOREID, REGISTERNAME, BRANDID, FILENAME
                            FROM CDMSYNC_DEV.IRB."TRAN_REGISTER_PLR";
create view IF NOT EXISTS TRAN_TXNMENU_PLR_BV(
	BRANDID,
	POSNAME,
	PLUNUMBER,
	SOURCE,
	ORDERLINEID,
	DESC,
	PRODUCTID,
	STOREID,
	FILENAME,
	NAME,
	CDMLOADDATE
) as SELECT
                               BRANDID, POSNAME, PLUNUMBER, SOURCE, ORDERLINEID, DESC, PRODUCTID, STOREID, FILENAME, NAME, CDMLOADDATE
                            FROM CDMSYNC_DEV.IRB."TRAN_TXNMENU_PLR";
CREATE PROCEDURE IF NOT EXISTS SP_BASE_VIEW_CREATION_IMPLEMENTATION("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216), "SRC_TB_PREFIXES_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    function prepareSelectTableNames(src_tb_prefixes) {

      var where_table_name_stmt = "";

      if (src_tb_prefixes) {
         var src_db_prefixes_array = src_tb_prefixes.split('','')

         if (src_db_prefixes_array.length > 1){
             where_table_name_stmt += ` AND TABLE_NAME LIKE ''${src_db_prefixes_array.shift()}%''`
             for (const prefix of src_db_prefixes_array) {
                 where_table_name_stmt += ` OR table_name LIKE ''${prefix}%''`;
             }
         } else {
             where_table_name_stmt += ` AND TABLE_NAME LIKE ''${SRC_TB_PREFIXES_PARAM}%''`;
         }
      }
      return `SELECT table_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_TYPE = ''BASE TABLE'' ${where_table_name_stmt};`;
    }

    var select_table_names = prepareSelectTableNames(SRC_TB_PREFIXES_PARAM);

    var select_column_names = `SELECT column_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_NAME = :3 order by ordinal_position;`

    var select_table_names_stmt = snowflake.createStatement(
                {
                sqlText: select_table_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM]
                }
        );

    var table_names = select_table_names_stmt.execute();

    var table_name_array=[];
    while(table_names.next())
    {
        table_name_array.push(table_names.getColumnValue(1));
    }

    for (const table_name of table_name_array) {

        var select_column_names_stmt = snowflake.createStatement(
                {
                sqlText: select_column_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM, table_name]
                }
        );

        var column_names = select_column_names_stmt.execute();
        var column_name_array=[];
        while(column_names.next())
        {
            column_name_array.push(column_names.getColumnValue(1));
        }
        var column_name_array = column_name_array.map(x => `${x} AS ${x}`);

        var source_tb_columns = column_name_array.join('', '');

        var view_template = `CREATE VIEW IF NOT EXISTS ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}_BV COPY GRANTS
                            AS SELECT
                               ${source_tb_columns}
                            FROM ` + SRC_DB_PARAM + `.` + SRC_SCHEMA_PARAM + `."${table_name}";`
//        var execute_base_view_stmt = snowflake.createStatement(
//        {
//            sqlText: view_template
//        });
//        execute_base_view_stmt.execute();
    }
    try {

        return view_template;
        }
    catch (err)  {
        throw err;
        }
    ';