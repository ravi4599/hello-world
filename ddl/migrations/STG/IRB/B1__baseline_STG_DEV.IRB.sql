
create TABLE IF NOT EXISTS BACKUP_CUSTOMER_INTAKE (
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	MIDDLE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	PHONE_NBR VARCHAR(16777216),
	ADDRESS_LINE1_TXT VARCHAR(16777216),
	ADDRESS_LINE2_TXT VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	PROCESS_STATUS VARCHAR(16777216),
	PROCESS_DTTM TIMESTAMP_NTZ(9),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_TYPE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS BACKUP_CUSTOMER_INTAKE_HIST_LOAD (
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	MIDDLE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	PHONE_NBR VARCHAR(16777216),
	ADDRESS_LINE1_TXT VARCHAR(16777216),
	ADDRESS_LINE2_TXT VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	PROCESS_STATUS VARCHAR(16777216),
	PROCESS_DTTM TIMESTAMP_NTZ(9),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS BACKUP_UC2_SEND_TO_EPSILON_HIST_LOAD (
	SRC_JOB_ID VARCHAR(255),
	SRC_RECORD_ID VARCHAR(255),
	FULL_NAME VARCHAR(255),
	PREFIX_NAME VARCHAR(255),
	GIVEN_NAME VARCHAR(255),
	MIDDLE_NAME VARCHAR(255),
	FAMILY_NAME VARCHAR(255),
	GENERATIONAL_SUFFIX VARCHAR(255),
	GENDER VARCHAR(255),
	COMPANY_NAME VARCHAR(255),
	ADDRESS_LINE1 VARCHAR(255),
	ADDRESS_LINE2 VARCHAR(255),
	ADDRESS_LINE3 VARCHAR(255),
	ADDRESS_LINE4 VARCHAR(255),
	LOCALITY1 VARCHAR(255),
	LOCALITY2 VARCHAR(255),
	LOCALITY3 VARCHAR(255),
	REGION1 VARCHAR(255),
	REGION2 VARCHAR(255),
	POSTAL_CODE VARCHAR(255),
	COUNTRY_CODE VARCHAR(255),
	EMAIL_ADDRESS1 VARCHAR(255),
	EMAIL_ADDRESS2 VARCHAR(255),
	EMAIL_ADDRESS3 VARCHAR(255),
	PHONE1 VARCHAR(255),
	PHONE2 VARCHAR(255),
	PHONE3 VARCHAR(255),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS CDM_SYNC_TASK_RUN (
	TASK_NAME VARCHAR(16777216),
	MODIFICATION VARCHAR(16777216)
);
create TABLE IF NOT EXISTS CIP_DIGITAL_ORDER (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL,
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL
);
create TABLE IF NOT EXISTS CIP_IDP_FIRST_PARTY (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL,
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL
);
create TABLE IF NOT EXISTS CIP_SOURCES (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NM VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	USECASE VARCHAR(16777216) NOT NULL COMMENT 'CIP usecase: UC1 - unknown customers (POS transactions); UC2 - known customers (first party, loyalty, guests)',
	SOURCE_DATA_FLOW VARCHAR(16777216) COMMENT 'Source identifier specifies the data flow which is used to bring data into CIP.',
	ACTIVE_IND BOOLEAN NOT NULL COMMENT 'Active Indicator specifies whether the configuration is active or inactive.',
	COMMENT VARCHAR(16777216) COMMENT 'Comment is a description of the configuration.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1CUSTOMER_INTAKE_CONF unique (BRAND_ID, SOURCE_SYSTEM_NM, USECASE, SOURCE_DATA_FLOW)
);
create TABLE IF NOT EXISTS CI_TEST (
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	MIDDLE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	PHONE_NBR VARCHAR(16777216),
	ADDRESS_LINE1_TXT VARCHAR(16777216),
	ADDRESS_LINE2_TXT VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	PROCESS_STATUS VARCHAR(16777216),
	PROCESS_DTTM TIMESTAMP_NTZ(9),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_TYPE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS COPY_INTO_TEST_LOYALTY_SEND_TO_EPSILON (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	constraint XAK1COPY_INTO_TEST_LOYALTY_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS CUSTOMER (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	MBR_INSPIRE_ID VARCHAR(16777216) NOT NULL,
	MBR_ID VARCHAR(16777216),
	CLOSEST_STORE_ID VARCHAR(16777216),
	HOUSEHOLD_ID VARCHAR(16777216),
	EMAIL_ID VARCHAR(16777216),
	MOBILE_DEVICE_ID VARCHAR(16777216),
	EXPERIAN_ID VARCHAR(16777216),
	EXPERIAN_STATUS_TYP VARCHAR(16777216),
	INSPIRE_CUST_TYP VARCHAR(16777216),
	MDM_ID VARCHAR(16777216),
	MDM_ID_DELETED_IND BOOLEAN,
	LOYALTY_CARD_NBR VARCHAR(16777216),
	FIRST_NM VARCHAR(16777216),
	LAST_NM VARCHAR(16777216),
	MIDDLE_INITIAL_TXT VARCHAR(16777216),
	DOB_DT DATE,
	BIRTH_MNTH_ID NUMBER(38,0),
	BIRTH_YEAR_ID NUMBER(38,0),
	BIRTH_DT_IMPLIED_IND BOOLEAN,
	GENDER_TYP VARCHAR(16777216),
	ENROLLMENT_CHANNEL_TYP VARCHAR(16777216),
	MBR_STATUS_CD VARCHAR(16777216),
	POINT_BALANCE_QTY NUMBER(38,0),
	MEMBERSHIP_STATUS_CD VARCHAR(16777216),
	PROFILE_COMPLETED_STATUS_IND BOOLEAN,
	DELIVERABILITY_STATUS_IND VARCHAR(16777216),
	MOBILE_NBR VARCHAR(16777216),
	ADR_LINE_1_TXT VARCHAR(16777216),
	ADR_LINE_2_TXT VARCHAR(16777216),
	CTY_NM VARCHAR(16777216),
	ST_CD VARCHAR(16777216),
	ZIP_CD VARCHAR(16777216),
	CNTRY_CD VARCHAR(16777216),
	EMAIL_OPT_OUT_IND BOOLEAN,
	PUSH_NOTIFICATION_OPT_IN_IND BOOLEAN,
	SMS_OPT_IN_IND BOOLEAN,
	PRIVACY_IND BOOLEAN,
	UNSUBSCRIBE_DTTM TIMESTAMP_NTZ(9),
	POINT_EXPIRE_DTTM TIMESTAMP_NTZ(9),
	ENROLL_START_DTTM TIMESTAMP_NTZ(9),
	LAST_LOGIN_DTTM TIMESTAMP_NTZ(9),
	LAST_STATUS_CHANGE_DTTM TIMESTAMP_NTZ(9),
	PROFILE_COMPLETION_DTTM TIMESTAMP_NTZ(9),
	SUBSCRIBER_KEY VARCHAR(200),
	SUBSCRIBER_SOURCE_NM VARCHAR(100),
	LOYALTY_TIER_CHANGE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The date/time the tier change occurred. Format: YYYY-MM-DD HH:MI:SS EX. 2020-12-18 06:07:24',
	LOYALTY_TIER_NM VARCHAR(16777216) COMMENT 'The tier code for the member. Valid values are: CLUB_DUNKIN_BOOSTED',
	LOYALTY_TIER_EXPIRATION_DT DATE COMMENT 'Date the Boosted tier is set to expire or ended. Will be Expire Date for Boosted tiers expiring naturally, or End Date of Boosted tiers if CSR downgraded. YYYYMMDD Format.EX. 20210331',
	LOYALTY_ELITE_VISIT_CNT NUMBER(38,0) COMMENT 'Number of qualifying Visits in calendar month for the member related to Elite Tier status. Number will be an integer between 0 and 99,999.',
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	CDM_LOAD_DT DATE,
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	PUSH_DEVICE_ID VARCHAR(16777216),
	ADDRESS_OPT_IN_IND BOOLEAN,
	IGNORE_FRAUD_SUSPEND_IND BOOLEAN,
	EMAIL_OPT_OUT_STATUS_IND BOOLEAN,
	CUSTOMER_AUTHENTICATION_STATUS VARCHAR(16777216),
	CUSTOMER_EXTERNAL_CIP_ID VARCHAR(16777216),
	CUSTOMER_CIP_ID VARCHAR(16777216) COMMENT 'Inspire Generated Unique Customer Identification at Brand Level. Based on Inspire Internal Business/Natural key attributes available during customer signup and/or customer transactions (orders).',
	constraint XAK1CUSTOMER unique (BRAND_ID, MBR_ID),
	constraint XPKCUSTOMER primary key (BRAND_ID, MBR_INSPIRE_ID)
);
create TABLE IF NOT EXISTS CUSTOMER_ARBITRATION_ENRICHMENT (
	CUSTOMER_EXTERNAL_CIP_ID VARCHAR(16777216) NOT NULL COMMENT 'Core ID, a unique Cross Brand Customer Id for an Individual. Sourced from a third-Party Agency.',
	MATCH_LEVEL VARCHAR(16777216) NOT NULL COMMENT 'Indicates whether the match was done at an Individual or Household level.',
	PERSON_SEQ_NBR VARCHAR(16777216) COMMENT 'Person Seq No.',
	HH_AGE_UNKNOWN_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults in the household.',
	HH_AGE_75_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 75+ in the household.',
	HH_AGE_65_74_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 65-74 in the household.',
	HH_AGE_55_64_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 55-64 in the household.',
	HH_AGE_45_54_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 45-54 in the household.',
	HH_AGE_35_44_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 35-44 in the household.',
	HH_AGE_25_34_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 25-34 in the household.',
	HH_AGE_18_24_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 18-24 in the household.',
	HH_AGE_0_2_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 0 and 2.',
	HH_AGE_3_5_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 3 and 5.',
	HH_AGE_6_10_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 6 and 10.',
	HH_AGE_11_15_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 11 and 15.',
	HH_AGE_16_17_PRESENCE_CODE VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 15 and 17.',
	HH_HOUSEHOLD_CHILD_CNT VARCHAR(16777216) COMMENT 'The number of known children in the household; Unknown should not be interpreted as No children.',
	HH_HOUSEHOLD_TYPE_CODE VARCHAR(16777216) COMMENT 'Represents the makeup of the household, such as married couple with children, married couple with no children or single female with no children.',
	HH_HOUSEHOLD_GENERATION_CNT VARCHAR(16777216) COMMENT 'Number of generations living within a household.',
	HH_DWELLING_TYPE VARCHAR(16777216) COMMENT 'Type of structure of the building in which the household resides. Based upon Record Type Code from USPS and property tax/deed data.',
	HH_DWELLING_TYPE_SOURCE VARCHAR(16777216) COMMENT 'Specific (S = household specific, H = household inferred and A = area inferred); A value only offered for DE.',
	HH_HOME_OWNER VARCHAR(16777216) COMMENT 'Classification of household head as an owner/renter. Under each category there is a definite and probable classification.',
	HH_HOME_OWNER_SOURCE VARCHAR(16777216) COMMENT 'Specific (S = household specific, H = household inferred and A = area inferred); A value only offered for DE.',
	HH_HOUSEHOLD_MARITAL_STATUS VARCHAR(16777216) COMMENT 'Married or single.',
	HH_HOUSEHOLD_MARITAL_STATUS_SOURCE VARCHAR(16777216) COMMENT 'Advantage Household Marital Status Indicator.',
	HH_HOUSEHOLD_ADULT_CNT VARCHAR(16777216) COMMENT 'Number of adults in the household.',
	HH_HOUSEHOLD_ADULT_CNT_SOURCE VARCHAR(16777216) COMMENT 'Advantage Number of Adults Indicator.',
	HH_LENGTH_OF_RESIDENCE VARCHAR(16777216) COMMENT 'Dervied from both Assessor data and the origination date of record on the file at the current address. A one-digit code indicating the length of time a household has been identified at this address.',
	HH_LENGTH_OF_RESIDENCE_SOURCE VARCHAR(16777216) COMMENT 'Length of time that the surname has been at this particular address; A value only offered for DE.',
	HH_HOUSEHOLD_AGE VARCHAR(16777216) COMMENT 'Age range of current head of household will be specific if an age source was received for household head, or age is inferred if specific age source is not available.',
	HH_HOUSEHOLD_AGE_SOURCE VARCHAR(16777216) COMMENT 'Age range of current head of household will be specific if an age source was received for household head, or age is inferred if specific age source is not available.',
	HH_PRESENCE_CHILDREN VARCHAR(16777216) COMMENT 'Child present in household; Unknown should not be interpreted as No children.',
	HH_PRESENCE_CHILDREN_SOURCE VARCHAR(16777216) COMMENT 'Advantage Presence of Children Indicator (Enhanced).',
	HH_HOUSEHOLD_SIZE VARCHAR(16777216) COMMENT 'Number of persons in the household.',
	HH_HOUSEHOLD_SIZE_SOURCE VARCHAR(16777216) COMMENT 'Advantage Household Size Indicator (Enhanced).',
	HH_HOUSEHOLD_MEDIAN_EDUCATION VARCHAR(16777216) COMMENT 'Median school years completed by adults age 18 or older.',
	HH_HOUSEHOLD_MEDIAN_EDUCATION_SOURCE VARCHAR(16777216) COMMENT 'Advantage Household Education Indicator (Enhanced).',
	HH_OCCUPATION_CODE VARCHAR(16777216) COMMENT 'Most prominent known profession of everybody in the household; self-reported data and occupation titles.',
	GENDER_CODE VARCHAR(16777216) COMMENT 'The gender of the individual.',
	MEMBER_CODE_OF_PERSON VARCHAR(16777216) COMMENT 'The year and month (YYYYMM) this surname and address were most recently verified by the most current source list.',
	DEMOGRAPHIC_VERIFICATION_DATE VARCHAR(16777216) COMMENT 'The year and month (YYYYMM) this surname and address were most recently verified by the most current source list.',
	INDIVIDUAL_MARITAL_STATUS VARCHAR(16777216) COMMENT 'Marital status of individual.',
	INDIVIDUAL_MARITAL_STATUS_SOURCE VARCHAR(16777216) COMMENT 'Specific (S = household specific, H = household inferred and A = area inferred).',
	INDIVIDUAL_EXACT_AGE VARCHAR(16777216) COMMENT 'Individual exact age.',
	INDIVIDUAL_EXACT_SOURCE VARCHAR(16777216) COMMENT 'Birthday - Month Indicator (Enhanced).',
	LANGUAGE_CODE VARCHAR(16777216) COMMENT 'Indicates whether or not Person 2 speaks a specific language.',
	HH_HOBBIES_BAKING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_HOBBIES_COOKING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_HOBBIES_CRAFTS VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_HOBBIES_FOOD VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_HOBBIES_WINES VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_MAIL_ORDER_FOOD VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_DIET_NUTRITION VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_DIET_WEIGHT VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_DIET_FOODS VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_DIET_VITAMINS VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_BOOKS_COOKING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_BOOKS_SPORTS VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_BOOKS_WORLDNEWS VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_ALL_SPORTS VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_CYCLING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_BOATING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_CAMPING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_FISHING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_FITNESS VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_GOLF VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_HUNTING_SHOOTING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_HUNTING_GAME VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_NASCAR VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_RUNNING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_SKIING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_WALKING VARCHAR(16777216) COMMENT 'Interest present in Household.',
	HH_SPORTS_YOGA VARCHAR(16777216) COMMENT 'Interest present in Household.',
	ALCOHOL_BEV_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Alcohol Beverage Switchers model, which ranks consumers based on their likelihood to switch brands.',
	BEER_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Beer Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	BRAND_LOYALTY_RANK VARCHAR(16777216) COMMENT 'Develop shopping list that includes specific brand preferences (Chobani vs. yogurt) and focus on brands vs. price.',
	BREAKFAST_MEAT_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Breakfast Meat Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	BUDGET_MEAL_PLANNER_RANK VARCHAR(16777216) COMMENT 'Plan meals in advance to manage budgets and stretch dollars.',
	CANNED_SOUP_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Canned Soup Purchasers model which ranks consumers based upon their likelihood to purchase canned soup.',
	CHOCOLATE_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Chocolate Candy Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	CLICK_TO_CART_HOME_DELIVERY_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Click to Cart - Home Delivery Customers model which ranks consumers based upon their likelihood to order groceries online and have them delivered.',
	CLICK_TO_CART_PICKUP_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Click to Cart - Pick Up Customers model which rankd consumers based upon their likelihood to order groceries online and pick up at the store.',
	COKE_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Coca Cola Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	CEREAL_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Cold Cereal Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	COFFEE_BRAND_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Coffee Brand Switchers model which ranks consumers based upon their likelihood to switch coffee brands.',
	CONVENIENCE_COOK_RANK VARCHAR(16777216) COMMENT 'Cooks focused on deliver easier to prepare and faster meals.',
	CRAFT_BEER_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Craft Beer Enthusiasts model which ranks consumers based upon their likelihood to consume craft beer.',
	DOMESTIC_BEER_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Domestic Beer Enthusiasts model which ranks consumers based upon their likelihood to consume domestic beer.',
	ENERGY_DRINK_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Energy Drink Switchers model which ranks consumers based upon their likelihood to switch brands.',
	EVERY_DAY_LOW_PRICE_SHOPPER_RANK VARCHAR(16777216) COMMENT 'Greater likelihood to shop at or favor everyday low price format (example: Walmart).',
	EXPERIMENTAL_COOK_RANK VARCHAR(16777216) COMMENT 'Cooks who like to explore new recipes and meal ideas.',
	FLAVORED_DRINK_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Flavored Drink Purchasers model ranks consumers based upon their likelihood to purchase.',
	FREQUENT_TAKEOUT_FOOD_RANK VARCHAR(16777216) COMMENT 'Greater likelihood to purchase takeout food.',
	FRESH_FOOD_SEEKER_RANK VARCHAR(16777216) COMMENT 'Interested in and purchase fresh food (grocery perimeter items).',
	FROZEN_FOOD_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Frozen Food Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	FRUIT_ALCOHOLIC_BEVERAGE_DRINKER_RANK VARCHAR(16777216) COMMENT 'Epsilon Fruit Alcoholic Beverage Drinkers model ranks consumers based upon their likelihood to purchase.',
	GREEN_PRODUCT_BUYER_RANK VARCHAR(16777216) COMMENT 'Purchase or seek products that are non toxic, energy and water-efficient, and harmless to the environment.',
	GROCERY_LOYALTY_CARD_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Use 3+ grocery store loyalty/rewards cards regularly.',
	HARD_LEMONADE_DRINKER_RANK VARCHAR(16777216) COMMENT 'Epsilon Hard Lemonade Enthusiasts model ranks consumers based upon their likelihood to purchase.',
	HARD_TEA_DRINKER_RANK VARCHAR(16777216) COMMENT 'Epsilon Hard Tea Enthusiasts model ranks consumers based upon their likelihood to purchase.',
	HARD_SELTZER_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Hard Seltzer Enthusiasts model which ranks consumers based upon their likelihood to consume hard seltzer.',
	HEALTHY_CEREAL_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Healthy Cereals Purchasers model which ranks consumers based upon their likelihood to purchase healthy cereals.',
	HEAVY_FIBER_FOCUSED_FOOD_BUYER_RANK VARCHAR(16777216) COMMENT 'Greater likelihood to purchase product high in fiber.',
	HEAVY_COUPON_USER_RANK VARCHAR(16777216) COMMENT 'Increased likelihood to be active coupon clippers and redeemers.',
	HEAVY_GLUTEN_FREE_FOOD_BUYER_RANK VARCHAR(16777216) COMMENT 'Greater likelihood to product without gluten.',
	HEAVY_LOW_FAT_FOOD_BUYER_RANK VARCHAR(16777216) COMMENT 'Greater likelihood to purchase low fat foods.',
	IMPORT_BEER_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Import Beer Enthusiasts model which ranks consumers based upon their likelihood to consume import beer.',
	IMPULSE_BUYER_RANK VARCHAR(16777216) COMMENT 'Increased likelihood shoppers will purchase off list items during in-store shopping.',
	INCENTIVE_SEEKER_RANK VARCHAR(16777216) COMMENT 'Shoppers actively seeking price incentives (e.g. coupons) or researching what is on sale.',
	LAUNDRY_NEW_PRODUCT_SEEKER_RANK VARCHAR(16777216) COMMENT 'Actively seeks and interested in new laundry products.',
	LIQUOR_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Liquor Enthusiasts model which ranks consumers based upon their likelihood to consume liquor.',
	MASTER_COOK_RANK VARCHAR(16777216) COMMENT 'Skilled cooks that enjoy the experience of cooking.',
	MEAL_PLANNER_RANK VARCHAR(16777216) COMMENT 'Shoppers who develop weekly meal plans and shopping list to match their meal plans.',
	MEATLESS_PREFERENCE_RANK VARCHAR(16777216) COMMENT 'Epsilon Meatless Preference Consumers model which ranks consumers based upon their likelihood to prefer meatless products.',
	MILLER_LITE_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Miller Lite Switchers Consumers model which ranks consumers based upon their likelihood to switch to drinking miller lite.',
	MOBILE_SHOPPING_LIST_USER_RANK VARCHAR(16777216) COMMENT 'Greater likelihood to make a shopping list on their mobile phone.',
	MULTI_RETAILER_RANK VARCHAR(16777216) COMMENT 'Increased likelihood of going to multiple retailers to execute their shopping list and  fulfil shopping trip needs.',
	NEW_FOOD_AND_DRINK_SEEKER_RANK VARCHAR(16777216) COMMENT 'Epsilon New Product Seekers – Food and Drink model which ranks consumers based upon their likelihood to try new food and drinks.',
	HEALTH_BAR_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Nutritional Health Bar Brand Switchers.',
	ONE_STOP_SHOPPER_RANK VARCHAR(16777216) COMMENT 'Primarily shop at one retailer for each shopping trip.',
	ON_THE_GO_FOOD_DRINK_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon On-the-Go Food/Drink Consumers model which ranks consumers based upon their likelihood to eat food and drink on the go.',
	ON_THE_GO_SNACKER_RANK VARCHAR(16777216) COMMENT 'Epsilon On-the-Go Snackers (Snack Food) model which ranks consumers based upon their likelihood to eat snack food on the go.',
	ORGANIC_FOOD_BUYER_RANK VARCHAR(16777216) COMMENT 'Seek food made from organic raw materials and organic certification.',
	ORGANIC_PRODUCT_BUYER_RANK VARCHAR(16777216) COMMENT 'Seek products made from organic raw materials and organic certification.',
	PAPER_SHOPPING_LIST_USER_RANK VARCHAR(16777216) COMMENT 'Shoppers who make and use paper lists.',
	PASTA_SAUCE_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Pasta Sauce Purchasers model which ranks consumers based upon their likelihood to purchase pasta sauce.',
	REAL_INGREDIENT_COOK_RANK VARCHAR(16777216) COMMENT 'Cook ingredients that take additional preparation work (buy lettuce vs. bagged salad).',
	RED_WINE_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Red Wine Enthusiasts model which ranks consumers based upon their likelihood to consume red wine.',
	LUNCH_MEAT_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Refrigerated Lunch Meat Brand Switchers.',
	RETAILER_EMAIL_SUBSCRIBER_RANK VARCHAR(16777216) COMMENT 'Greater likelihood to subscribe to retailer emails.',
	SALTY_SNACK_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Salty Snacks Purchasers model which ranks consumers based upon their likelihood to purchase salty snacks.',
	GRANOLA_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Snack Bar/Granola Bar Brand Switchers.',
	SNACK_CHEESE_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Snack Cheese Purchasers model which ranks consumers based upon their likelihood to purchase snack cheese.',
	SOFT_DRINK_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Soft Drinks Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	SPIRITS_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Spirits Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	STOCK_UP_AT_GROCERY_STORE_RANK VARCHAR(16777216) COMMENT 'Shopper that complete larger dollar sales trips at traditional grocery stores.',
	SUGAR_FREE_FOOD_BEVERAGE_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Sugar Free Foods/Beverages Purchasers model ranks consumers based upon their likelihood to purchase.',
	WHITE_WINE_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon White Wine Enthusiasts model which ranks consumers based upon their likelihood to consume white wine.',
	YOGURT_SWITCHER_RANK VARCHAR(16777216) COMMENT 'Epsilon Yogurt Brand Switchers model which ranks consumers based upon their likelihood to switch brands.',
	APP_USER_RANK VARCHAR(16777216) COMMENT 'Epsilon App Users model which ranks consumers based upon their likelihood to use apps.',
	DIRECT_MEDIA_PREFERENCE_RANK VARCHAR(16777216) COMMENT 'Epsilon Direct Media Preference model ranks consumers based upon their likelihood to review media via postal mail.',
	FANTASY_SPORTS_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Fantasy Sports Enthusiasts model which ranks consumers based upon their likelihood to participate in fantasy sports.',
	SPORTS_READER_RANK VARCHAR(16777216) COMMENT 'Epsilon Sports Reader model which ranks consumers based upon their likelihood to read sports books/magazines.',
	AMERICAN_DINER_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon American Diner Enthusiasts  model ranks consumers based upon their likelihood to dine at American diners.',
	BAR_LOUNGE_FOOD_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Bar and Lounge Food Enthusiasts model which ranks consumers based upon their likelihood to go to bars, clubs or lounges 3x a week or more.',
	BARGAIN_SHOPPER_RANK VARCHAR(16777216) COMMENT 'Epsilon Bargain Shoppers model ranks consumers based upon their likelihood to shop on sale or use coupons.',
	BRAND_LOYAL_CONSUMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Brand Loyal Consumers model which ranks consumers based upon their likelihood to be brand loyal.',
	BREAKFAST_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Breakfast Dining Enthusiasts model ranks consumers based upon their likelihood to dine out frequently for breakfast.',
	BURGER_DINER_RANK VARCHAR(16777216) COMMENT 'Epsilon Burger Diners model which ranks consumers based upon their likelihood to be burger diners.',
	BUY_ONLINE_REST_PICKUP_RANK VARCHAR(16777216) COMMENT 'Epsilon Buy Online Pick Up In Store model ranks consumers based upon their likelihood to be buy online-pickup in store consumers.',
	CARRYOUT_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Carry Out Enthusiasts model which ranks consumers based upon their likelihood to go to carry out restaurants 3x a week or more.',
	CASUAL_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Casual Dining Enthusiasts model which ranks consumers based upon their likelihood to go to casual dining restaurants 3x a week or more.',
	CATERING_DELIVERY_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Ordered catered food online and had it delivered in the last 12 months.',
	CATERING_PICKUP_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Ordered catered food online 2+ times and picked it up in the last 12 months.',
	CHICKEN_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Chicken Dining Enthusiasts model which ranks consumers based upon their likelihood to choose chicken when dining out.',
	CHINESE_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Chinese Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Chinese restuarants.',
	CLEAN_EATING_DINING_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Clean Eating Dining Customers model ranks consumers based upon the likelihood to be clean eating diners.',
	COFFEE_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Coffee Enthusiasts model which ranks consumers based upon their likelihood to go to coffee houses/cafes and get coffee to go at least 3x a month.',
	DINNER_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Dinner Dining Enthusiasts model ranks consumers based upon their likelihood to dine out frequently for dinner.',
	DOORDASH_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon DoorDash Dining Enthusiasts model which ranks consumers based upon their likelihood to use DoorDash.',
	EXTREME_FITNESS_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Extreme Fitness Enthusiast model ranks consumers based upon their likelihood to participate in P90X, CrossFit or other fitness programs.',
	FINE_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Fine dining Enthusiasts model which ranks consumers based upon their likelihood to go to fine dining restaurants 3x a week or more.',
	FITNESS_EQUIPMENT_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Fitness Equipment Purchasers model which ranks consumers based upon their likelihood to purchase fitness equipment.',
	FOOD_TRUCK_CONSUMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Food Truck Consumers model which ranks consumers based upon their likelihood to purchase from food trucks.',
	FRESH_FOOD_DELIVERY_CONSUMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Fresh Food Delivery Consumers model ranks consumers based upon their likelihood to have fresh food delivered.',
	FRIED_CHICKEN_SANDWICH_DINER_RANK VARCHAR(16777216) COMMENT 'Epsilon Fried Chicken Sandwich Diners model which ranks consumers based upon their likelihood to be fried chicken sandwich diners.',
	FRIED_FISH_SANDWICH_DINER_RANK VARCHAR(16777216) COMMENT 'Epsilon Fried Fish Sandwich Diners model which ranks consumers based upon their likelihood to be fried fish diners.',
	GLUTEN_FREE_DINING_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Gluten Free Dining Customers model ranks consumers based on the likelihood to be gluten free diners.',
	GREEK_MIDDLE_EASTERN_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Greek/Middle Eastern Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Greek/Middle Eastern restuarants.',
	GRILLED_CHICKEN_SANDWICH_DINER_RANK VARCHAR(16777216) COMMENT 'Epsilon Fried Chicken Sandwich Diners model which ranks consumers based upon their likelihood to be fried chicken sandwich diners.',
	GROCERY_STORE_APP_USER_RANK VARCHAR(16777216) COMMENT 'Epsilon Grocery Store App Users model ranks consumers based upon their likelihood to use grocery apps when they shop.',
	GROCERY_STORE_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Grocery Store Frequenters model ranks consumers based upon their likelihood to shop frequently at grocery stores.',
	GRUBHUB_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Grubhub Dining Enthusiasts model which ranks consumers based upon their likelihood to use Grubhub.',
	HOT_DOG_DINER_RANK VARCHAR(16777216) COMMENT 'Epsilon Hot Dog Diners model which ranks consumers based upon their likelihood to be hot dog diners.',
	INTERNET_CONNECTED_FITNESS_RANK VARCHAR(16777216) COMMENT 'Epsilon Internet Connected Fitness Users model which ranks consumers based upon their likelihood to use internet connected fitness products.',
	ITALIAN_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Italian Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Italian restuarants.',
	JAPANESE_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Japanese Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Japanese restuarants.',
	LOW_CARB_DIET_DINING_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Low Carb Diet Dining Customers model ranks consumers based on their likelihood to be low carb diners.',
	LOW_SODIUM_DINING_RANK VARCHAR(16777216) COMMENT 'Epsilon Low Sodium Consumers model ranks consumers based upon their likelihood to have low sodium diets.',
	LUNCH_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Lunch Dining Enthusiasts model ranks consumers based upon their likelihood to dine out frequently for lunch.',
	MEAL_COMBO_RANK VARCHAR(16777216) COMMENT 'Epsilon Meal Combo Customers model ranks consumers based upon their likelihood to purchase meal combos at restaurants.',
	MEAL_KIT_DELIVERY_RANK VARCHAR(16777216) COMMENT 'Epsilon Meal Kit Delivery Consumers model which ranks consumers based upon their likelihood to have meal kits delivered.',
	MEXICAN_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Mexican Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Mexican restuarants.',
	ONLINE_PICKUP_REST_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Online - Pick Up Restaurant Customers model ranks consumers on their likelihood to order food online and pick up in the restaurant.',
	OTHER_DIRECT_DELIVERY_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Other Direct Delivery Dining Enthusiasts model which ranks consumers based upon their likelihood to use other direct delivery companies.',
	PANDEMIC_IN_REST_DINING_RANK VARCHAR(16777216) COMMENT 'Epsilon Pandemic - In Restaurant Diners model which ranks consumers based upon their likelihood to dine in restaurants during the pandemic.',
	PIZZA_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Pizza Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Pizza restuarants.',
	PLANT_BASED_DINING_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Plant Based Dining Customers model ranks consumers based on their likelihood to be plant based diners.',
	POSTMATES_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Postmates Dining Enthusiasts model which ranks consumers based upon their likelihood to use Postmates.',
	QSR_CASH_CONSUMER_RANK VARCHAR(16777216) COMMENT 'Epsilon QSR Cash Customers model ranks consumers based upon their likelihood to pay cash at QSRs.',
	QSR_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Quick Service Restaurant Enthusiasts model which ranks consumers based upon their likelihood to go to quick service restaurants 3x a week or more.',
	REST_APP_USER_RANK VARCHAR(16777216) COMMENT 'Regularly use 2+ apps for restaurants.',
	REST_GIFT_CARD_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Restaurant Gift Card Purchasers model which ranks consumers based upon their likelihood to purchase restaurant gift cards.',
	REST_LOYALTY_APP_USER_RANK VARCHAR(16777216) COMMENT 'Use restaurant apps that have a connection to a loyalty or rewards program.',
	REST_LOYALTY_CARD_USER_RANK VARCHAR(16777216) COMMENT 'Use 3+ restaurant loyalty cards regularly.',
	SANDWICH_DINER_RANK VARCHAR(16777216) COMMENT 'Epsilon Sandwich Diners model which ranks consumers based upon their likelihood to be sandwich diners.',
	STORE_GIFT_CARD_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Store Gift Card Purchasers model which ranks consumers based upon their likelihood to purchase store gift cards.',
	SUB_SANDWICH_DINER_RANK VARCHAR(16777216) COMMENT 'Epsilon Sub Sandwich Diners model which ranks consumers based upon their likelihood to be sub sandwich diners.',
	SUB_AUTO_SHIP_FOOD_BEV_CUSTOMER_RANK VARCHAR(16777216) COMMENT 'Epsilon Subscription or Auto Shipment-Food or Beverage model ranks consumers based upon their likelihood to be subscribers.',
	THAI_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Thai Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Thai restuarants.',
	UBER_EATS_DINING_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Uber Eats Dining Enthusiasts model which ranks consumers based upon their likelihood to use Uber Eats.',
	VEGETARIAN_RANK VARCHAR(16777216) COMMENT 'Epsilon Vegetarians model ranks consumers based upon their likelihood to be vegetarians.',
	BASKETBALL_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Basketball Enthusiast model  ranks consumers based upon their likelihood to plan to watch basketball for entertainment.',
	DIET_CONSCIOUS_HOUSEHOLD_RANK VARCHAR(16777216) COMMENT 'Epsilon Diet Conscious Households model ranks consumers based upon their likelihood to diet for health, eat low sugar, low fat or low calorie food diets, and have an interest in weight loss.',
	FOOTBALL_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Football Enthusiast model  ranks consumers based upon their likelihood to plan to watch football for entertainment.',
	GYM_MEMBER_RANK VARCHAR(16777216) COMMENT 'Epsilon Gym Members model which ranks consumers based upon their likelihood to belong to a gym.',
	HOCKEY_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Hockey Enthusiast model ranks consumers based upon their likelihood to be interested in watching hockey for entertainment.',
	HOME_GYM_OWNER_RANK VARCHAR(16777216) COMMENT 'Epsilon Home Gym Owners model which ranks consumers based upon their likelihood to have home gyms.',
	INTERMITTENT_FASTING_RANK VARCHAR(16777216) COMMENT 'Epsilon Intermittent Fasters model ranks consumers based upon their likelihood to practice intermittent fasting.',
	NASCAR_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon NASCAR Enthusiast model  ranks consumers based upon their likelihood to be interested in NASCAR.',
	ONLINE_BUYER_RANK VARCHAR(16777216) COMMENT 'Epsilon Online Transactor model ranks consumers based upon their likelihood to transact online.',
	PANDEMIC_DECREASE_SPENDING_RANK VARCHAR(16777216) COMMENT 'Epsilon Pandemic - Decreased Spenders model which ranks consumers based upon their likelihood to have decreased spending during the pandemic.',
	FITNESS_MEMBERSHIP_RANK VARCHAR(16777216) COMMENT 'Epsilon Fitness Membership model  ranks consumers based upon their likelihood to plan to get a membership in the  next 12 months.',
	PRO_WRESTLING_WWE_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Pro Wrestling (WWE) Enthusiasts model ranks consumers based upon their likelihood to be a WWE enthusiast.',
	PROACTIVE_HEALTH_MANAGER_RANK VARCHAR(16777216) COMMENT 'Epsilon Proactive Health Managers model ranks consumers based upon their likelihood to proactively manage their health.',
	PRO_SPORTS_EVENT_ATTENDEE_RANK VARCHAR(16777216) COMMENT 'Epsilon Professional Sports Events attendees model ranks consumers based upon their likelihood to attend these events.',
	SKIING_SNOWBOARD_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Skiing/Snowboarding Enthusiasts model which ranks consumers based upon their likelihood to own skis/snowboards.',
	SOCCER_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Soccer Enthusiast model which ranks consumers based upon their likelihood to be interested in watching soccer on tv.',
	HOUSEHOLD_WELLNESS_RANK VARCHAR(16777216) COMMENT 'Epsilon Wellness Household model ranks consumers based upon their likelihood to exercise on a regular basis, go to a doctor regularly, eat organic/healthy food diets and have an interest in natural foods.',
	YOGA_PILATES_INTEREST_RANK VARCHAR(16777216) COMMENT 'Epsilon Yoga/Pilates Enthusiasts model which ranks consumers based upon their likelihood to do yoga/pilates.',
	HH_INCOME_TIER VARCHAR(16777216) COMMENT 'Modeled household income with removal of protected-class data elements in build algorithm.  Model accesses eligible household data and geographic data.',
	HH_INCOME_TIER_SOURCE VARCHAR(16777216) COMMENT 'Indicates household inferred or area inferred.',
	NBR_ACTIVE_SOURCES_VERIFIED VARCHAR(16777216) COMMENT 'Number of Active Sources Verifying Household Information.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. For example, Epsilon or Salesforce Marketing App or Netsuite.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
);
create TABLE IF NOT EXISTS CUSTOMER_ARBITRATION_IDENTITY (
	CUSTOMER_CIP_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcJobId, this provides a unique identifier that links data back to the original data.  This is provided on input from the user.',
	CUSTOMER_EXTERNAL_CIP_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreId; Individual ID.',
	HOUSEHOLD_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreHHId.',
	MATCH_CONFIDENCE_CODE VARCHAR(16777216) COMMENT 'Indicates match location and confidence of match to known individuals',
	NAME_PREFIX VARCHAR(16777216) COMMENT 'Name prefix.',
	FIRST_NAME VARCHAR(16777216) COMMENT 'First (given) name.',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Middle name.',
	LAST_NAME VARCHAR(16777216) COMMENT 'Last (family) name.',
	NAME_SUFFIX VARCHAR(16777216) COMMENT 'Generational suffix.',
	PROFESSIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Professional Suffix.',
	GENDER VARCHAR(16777216) COMMENT 'Gender.',
	COMPANY_NAME VARCHAR(16777216) COMMENT 'Company or Business name.',
	PROFANITY_IND VARCHAR(16777216) COMMENT 'Profanity identified in the name.',
	FIRST_NAME_LAST_NAME_MATCH VARCHAR(16777216) COMMENT 'First name and last name match.',
	POSSIBLE_BUSINESS_NAME_IND VARCHAR(16777216) COMMENT 'The name on the record is a possible business name.',
	LACS_CONVERSION_NEEDED VARCHAR(16777216) COMMENT 'Locatable Address Conversion System indicator.',
	LACS_NEW_ADDRESS_AVAILABLE VARCHAR(16777216) COMMENT 'The type of Locatable Address Conversion address.',
	LACS_MATCH_RETURN_CODE VARCHAR(16777216) COMMENT 'Locatable Address Conversion footnote to indicate the validation of the address as LACS certified .',
	NCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective National Change Of Address move date.',
	NCOA_MOVE_TYPE VARCHAR(16777216) COMMENT 'Type of National Change Of Address move.',
	NCOA_LINK_RETURN_CODE VARCHAR(16777216) COMMENT 'National Change Of Address Link return codes.',
	PCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective Permanent Change Of Address move date.',
	PRIMARY_ADDRESS_CORRECTION_ACTION_CODE VARCHAR(16777216) COMMENT 'Primary Address Correction action code.',
	PRIMARY_ADDRESS_CORRECTION_FOOTNOTE VARCHAR(16777216) COMMENT 'Primary Address Correction footnote code which describes the corrections made.',
	BEST_ADDRESS_CODE VARCHAR(16777216) COMMENT 'The source of the best address.',
	CURBSIDE_ID VARCHAR(16777216) COMMENT 'Best value for curbside ID (addressLine1).',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'Standardized address line 1.',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Standardized address line 2.',
	ADDRESS_LINE3_TXT VARCHAR(16777216) COMMENT 'Standardized address line 3.',
	ADDRESS_LINE4_TXT VARCHAR(16777216) COMMENT 'Standardized address line 4.',
	CITY VARCHAR(16777216) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb).',
	LOCALITY_2 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	LOCALITY_3 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	STATE VARCHAR(16777216) COMMENT 'State, province, territory, or region.',
	REGION_2 VARCHAR(16777216) COMMENT 'Additional state, province, territory, or region.',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Standarized postal code.',
	ZIP_4_CODE VARCHAR(16777216) COMMENT 'ZIP+4.',
	SECONDARY_MATCH_RANGE VARCHAR(16777216) COMMENT 'Secondary range used for matching.',
	DELIVERY_POINT_BAR_CODE VARCHAR(16777216) COMMENT 'The two-digit Delivery Point Bar Code.',
	DELIVERY_POINT_BAR_CODE_CHECK_DIGIT VARCHAR(16777216) COMMENT 'Check digit for the Delivery-Point bar code, or for a five-digit bar code if a full postal code (ZIP+4) could not be assigned.',
	ISO_COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3).',
	CARRIER_ROUTE_NUMBER VARCHAR(16777216) COMMENT 'Carrier-route number.',
	LINE_OF_TRAVEL_NUMBER VARCHAR(16777216) COMMENT 'Line-of-travel number.',
	LINE_OF_TRAVEL_SORT_ORDER VARCHAR(16777216) COMMENT 'Line-of-travel sortation.',
	RECORD_TYPE_CODE VARCHAR(16777216) COMMENT 'The record-type indicator for the assigned address.',
	ADDRESS_LINE_REMAINDER VARCHAR(16777216) COMMENT 'Extraneous data found on the address line, which either can’t be identified by the parser or does not belong in a standardized address.',
	COMMERCIAL_MAIL_RECEIVING_AGENCY_CODE VARCHAR(16777216) COMMENT 'Commercial Mail Receiving Agency (CMRA) indicator.',
	DELIVERY_POINT_VALIDATION_FOOTNOTE VARCHAR(16777216) COMMENT 'Delivery Point Validation footnotes.',
	DELIVERY_POINT_VALIDATION_FALSE_POSITIVE VARCHAR(16777216) COMMENT 'Delivery Point Validation false-positive and triggered DPV.',
	NO_STAT_ADDRESS_IND VARCHAR(16777216) COMMENT 'No Stat indicator. No Stat means that the address is a vacant property, it receives mail as a part of a drop, or it does not have an established delivery yet.',
	DELIVERY_POINT_STATUS_CODE VARCHAR(16777216) COMMENT 'The Delivery Point Validation status component that is generated for this record.',
	FOREIGN_ADDRESS_CODE VARCHAR(16777216) COMMENT 'Foreign address code. F - Foreign, Y - Foreign, N - Domestic USA, BLANK - Processed as a Foreign Record.',
	LAST_ADDRESS_LINE_MATCH_IND VARCHAR(16777216) COMMENT 'Lastline match level indicator.',
	ADDRESS_MATCH_TO_ZIP_4_IND VARCHAR(16777216) COMMENT 'Addressline match level indicator.',
	UNSUITABLE_ADDRESS_IND VARCHAR(16777216) COMMENT 'Indicates whether the record is a deliverable address.',
	ZIP_CODE_REALIGNMENT_IND VARCHAR(16777216) COMMENT 'ZIP move indicator. The address is affected by a USPS ZIP Code realignment. ACE has assigned the new ZIP Code and, if applicable, the new city name.',
	ZIP_CODE_TYPE VARCHAR(16777216) COMMENT 'Type of ZIP Code assigned.',
	CONGRESSIONAL_DISTRICT_NUMBER VARCHAR(16777216) COMMENT 'District number for the U.S. House of Representatives.',
	FIPS_COUNTY_CODE VARCHAR(16777216) COMMENT 'Federal Information Processing Standard (FIPS) county code.',
	COUNTY_NAME VARCHAR(16777216) COMMENT 'The fully-spelled county name.',
	POSTAL_FACILITY_TYPE VARCHAR(16777216) COMMENT 'Type of postal facility.',
	FIPS_CODE VARCHAR(16777216) COMMENT 'FIPS code.',
	ADDRESS_ERROR_CODE VARCHAR(16777216) COMMENT 'Address Error Codes.',
	ADDRESS_STATUS_CODE VARCHAR(16777216) COMMENT 'Address Status Code.',
	GEOGRAPHIC_MATCH_CODE VARCHAR(16777216) COMMENT 'Match code indicating the precision of the latitude and longitude assignment.',
	LATITUDE VARCHAR(16777216) COMMENT 'Latitude (degrees north of the equator) in the format 12.123456.',
	LONGITUDE VARCHAR(16777216) COMMENT 'Longitude (degrees west of the Greenwich Meridian) in the format -12.123456.',
	FIPS_PLACE_CODE VARCHAR(16777216) COMMENT 'FIPS place code. A number assigned by the U.S. government to each incorporated municipality (city, village, town, etc.).',
	GEOGRAPHIC_BLOCK VARCHAR(16777216) COMMENT 'GEO block.',
	CENSUS_MCD_CCD_CODE VARCHAR(16777216) COMMENT 'U.S. Census Bureau minor civil division (MCD) data or, if MCD data is unavailable, census county division (CCD) data.',
	CBSA_CODE VARCHAR(16777216) COMMENT 'A Core-Based Statistical Area (CBSA).',
	MSA_CODE VARCHAR(16777216) COMMENT 'Metropolitan Statistical Area (MSA) number. 0000 indicates the address does not lie in any MSA; usually a rural area.',
	FIPS_STATE_CODE VARCHAR(16777216) COMMENT 'FIPS state code.',
	DELIVERY_POINT_DROP_IND VARCHAR(16777216) COMMENT 'Drop indicator. Delivery point serves multiple businesses or families. For example, delivery point may be a CMRA (Commercial Mail Receiving Agency).',
	THROWBACK_TO_POST_OFFICE_BOX_IND VARCHAR(16777216) COMMENT 'Throwback indicator. Customer with street address wants delivery at PO Box instead.',
	SEASONAL_ADDRESS_INDICATOR VARCHAR(16777216) COMMENT 'Indicates that the address is seasonally occupied.',
	VACANT_ADDRESS_INDICATOR VARCHAR(16777216) COMMENT 'Vacant address indicator.',
	DELIVERY_POINT_TYPE VARCHAR(16777216) COMMENT 'Delivery type. 1 - Curb-side delivery, 2 - NDCBU (Neighborhood Delivery Centralized Box Unit) delivery, 3 - Central delivery, 4 - Door-slot delivery.',
	DELIVERY_POINT_DROP_CNT VARCHAR(16777216) COMMENT 'Drop count indicates the number of businesses or families served by this delivery point.',
	LACS_CONVERTIBLE_INDICATOR VARCHAR(16777216) COMMENT 'LACS (Locatable Address Conversion System) indicator.',
	SEASONAL_EDUCATIONAL_ADDRESS_INDICATOR VARCHAR(16777216) COMMENT 'Indicates that the address is seasonal and related to an educational institution.',
	DELIVERY_SEQUENCE_FILE_RECORD_TYPE VARCHAR(16777216) COMMENT 'Delivery Sequence File Record type. B - Business address, R - Residential address, U - Unknown.',
	MAILABILITY_SCORE VARCHAR(16777216) COMMENT 'Mailability index.',
	RESIDENTIAL_DELIVERY_IND VARCHAR(16777216) COMMENT 'Best Residential Delivery Indicator.',
	ADDRESS_BLOCK_LINE_01 VARCHAR(16777216) COMMENT 'Mail ready address block line 1.',
	ADDRESS_BLOCK_LINE_02 VARCHAR(16777216) COMMENT 'Mail ready address block line 2.',
	ADDRESS_BLOCK_LINE_03 VARCHAR(16777216) COMMENT 'Mail ready address block line 3.',
	ADDRESS_BLOCK_LINE_04 VARCHAR(16777216) COMMENT 'Mail ready address block line 4.',
	ADDRESS_BLOCK_LINE_05 VARCHAR(16777216) COMMENT 'Mail ready address block line 5.',
	ADDRESS_BLOCK_LINE_06 VARCHAR(16777216) COMMENT 'Mail ready address block line 6.',
	ADDRESS_BLOCK_LINE_07 VARCHAR(16777216) COMMENT 'Mail ready address block line 7.',
	ADDRESS_BLOCK_LINE_08 VARCHAR(16777216) COMMENT 'Mail ready address block line 8.',
	ADDRESS_BLOCK_LINE_09 VARCHAR(16777216) COMMENT 'Mail ready address block line 9.',
	ADDRESS_BLOCK_LINE_10 VARCHAR(16777216) COMMENT 'Mail ready address block line 10.',
	OCCUPANCY_DURATION_SCORE VARCHAR(16777216) COMMENT 'Occupancy score - postal evidence within that timeframe that the person is/was at the input address. The lower the number the more recent the occupancy is verified.',
	DWELLING_TYPE VARCHAR(16777216) COMMENT 'Dwelling type.',
	DECEASED_IND VARCHAR(16777216) COMMENT 'Deceased Suppression Flag.',
	DECEASED_DATE_OF_BIRTH VARCHAR(16777216) COMMENT 'Date of Birth on Deceased.',
	DECEASED_DATE_OF_DEATH VARCHAR(16777216) COMMENT 'Date of Death.',
	DO_NOT_MAIL_IND VARCHAR(16777216) COMMENT 'Do Not Mail (DNM) Suppression Flag.',
	DO_NOT_CALL_IND VARCHAR(16777216) COMMENT 'Do Not Call (DNC) Suppression Flag.',
	DO_NOT_FAX_IND VARCHAR(16777216) COMMENT 'Do Not Fax (DNF) Suppression Flag.',
	PRISON_ADDRESS_IND VARCHAR(16777216) COMMENT 'Prison Address Suppression Flag.',
	NURSING_HOME_ADDRESS_IND VARCHAR(16777216) COMMENT 'Nursing Home Address Suppression Flag.',
	PHONE_NBR_1 VARCHAR(16777216) COMMENT 'Cleansed result for phone number.',
	PHONE_NBR_1_RETURN_CODE VARCHAR(16777216) COMMENT 'Return code from phone cleanse.',
	PHONE_NBR_2 VARCHAR(16777216) COMMENT 'Cleansed result for phone number.',
	PHONE_NBR_2_RETURN_CODE VARCHAR(16777216) COMMENT 'Return code from phone cleanse.',
	PHONE_NBR_3 VARCHAR(16777216) COMMENT 'Cleansed result for phone number.',
	PHONE_NBR_3_RETURN_CODE VARCHAR(16777216) COMMENT 'Return code from phone cleanse.',
	EMAIL_ADDRESS_1 VARCHAR(16777216) COMMENT 'Cleansed result for the email address.',
	EMAIL_ADDRESS_1_RETURN_CODE VARCHAR(16777216) COMMENT 'Return code from email cleanse.',
	EMAIL_ADDRESS_2 VARCHAR(16777216) COMMENT 'Cleansed result for the email address.',
	EMAIL_ADDRESS_2_RETURN_CODE VARCHAR(16777216) COMMENT 'Return code from email cleanse.',
	EMAIL_ADDRESS_3 VARCHAR(16777216) COMMENT 'Cleansed result for the email address.',
	EMAIL_ADDRESS_3_RETURN_CODE VARCHAR(16777216) COMMENT 'Return code from email cleanse.',
	NAME_HYGIENE_RETURN_CODE VARCHAR(16777216),
	ADDRESS_RETURN_CODE VARCHAR(16777216),
	IDENTITY_RETURN_CODE VARCHAR(16777216),
	QUALIFIED_CODE VARCHAR(16777216) COMMENT 'Indicates if the row is qualified for the Epsilon Contact Complete process.',
	MATCH_TYPE_CODE VARCHAR(16777216) COMMENT 'Indicates how the row was matched by the Epsilon Contact Complete process.',
	NAME_ENRICHED_CODE VARCHAR(16777216) COMMENT 'Indicates if the Name was enriched or not during the Epsilon Contact Complete process.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) COMMENT 'Source System Name specifies the origin of the data. For example, Epsilon or Salesforce Marketing App or Netsuite.',
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.'
);
create TABLE IF NOT EXISTS CUSTOMER_BACKUP (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	MBR_INSPIRE_ID VARCHAR(16777216) NOT NULL,
	MBR_ID VARCHAR(16777216),
	LOYALTY_PROGRAM_ID VARCHAR(16777216),
	CLOSEST_STORE_ID VARCHAR(16777216),
	HOUSEHOLD_ID VARCHAR(16777216),
	EMAIL_ID VARCHAR(16777216),
	MOBILE_DEVICE_ID VARCHAR(16777216),
	EXPERIAN_ID VARCHAR(16777216),
	EXPERIAN_STATUS_TYP VARCHAR(16777216),
	INSPIRE_CUST_TYP VARCHAR(16777216),
	MDM_ID VARCHAR(16777216),
	MDM_ID_DELETED_IND BOOLEAN,
	LOYALTY_CARD_NBR VARCHAR(16777216),
	FIRST_NM VARCHAR(16777216),
	LAST_NM VARCHAR(16777216),
	MIDDLE_INITIAL_TXT VARCHAR(16777216),
	DOB_DT DATE,
	BIRTH_MNTH_ID NUMBER(38,0),
	BIRTH_YEAR_ID NUMBER(38,0),
	BIRTH_DT_IMPLIED_IND BOOLEAN,
	GENDER_TYP VARCHAR(16777216),
	ENROLLMENT_CHANNEL_TYP VARCHAR(16777216),
	MBR_STATUS_CD VARCHAR(16777216),
	POINT_BALANCE_QTY NUMBER(38,0),
	MEMBERSHIP_STATUS_CD VARCHAR(16777216),
	PROFILE_COMPLETED_STATUS_IND BOOLEAN,
	DELIVERABILITY_STATUS_IND VARCHAR(16777216),
	MOBILE_NBR VARCHAR(16777216),
	ADR_LINE_1_TXT VARCHAR(16777216),
	ADR_LINE_2_TXT VARCHAR(16777216),
	CTY_NM VARCHAR(16777216),
	ST_CD VARCHAR(16777216),
	ZIP_CD VARCHAR(16777216),
	CNTRY_CD VARCHAR(16777216),
	EMAIL_OPT_OUT_IND BOOLEAN,
	PUSH_NOTIFICATION_OPT_IN_IND BOOLEAN,
	SMS_OPT_IN_IND BOOLEAN,
	PRIVACY_IND BOOLEAN,
	UNSUBSCRIBE_DTTM TIMESTAMP_NTZ(9),
	POINT_EXPIRE_DTTM TIMESTAMP_NTZ(9),
	ENROLL_START_DTTM TIMESTAMP_NTZ(9),
	LAST_LOGIN_DTTM TIMESTAMP_NTZ(9),
	LAST_STATUS_CHANGE_DTTM TIMESTAMP_NTZ(9),
	PROFILE_COMPLETION_DTTM TIMESTAMP_NTZ(9),
	SUBSCRIBER_KEY VARCHAR(200),
	SUBSCRIBER_SOURCE_NM VARCHAR(100),
	LOYALTY_TIER_CHANGE_DTTM TIMESTAMP_NTZ(9) COMMENT 'The date/time the tier change occurred. Format: YYYY-MM-DD HH:MI:SS EX. 2020-12-18 06:07:24',
	LOYALTY_TIER_NM VARCHAR(16777216) COMMENT 'The tier code for the member. Valid values are: CLUB_DUNKIN_BOOSTED',
	LOYALTY_TIER_EXPIRATION_DT DATE COMMENT 'Date the Boosted tier is set to expire or ended. Will be Expire Date for Boosted tiers expiring naturally, or End Date of Boosted tiers if CSR downgraded. YYYYMMDD Format.EX. 20210331',
	LOYALTY_ELITE_VISIT_CNT NUMBER(38,0) COMMENT 'Number of qualifying Visits in calendar month for the member related to Elite Tier status. Number will be an integer between 0 and 99,999.',
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	CDM_LOAD_DT DATE,
	LOAD_ID NUMBER(38,0) COMMENT 'Load Identifier specifies the Batch that inserted the record into the table.',
	LOAD_DTTM TIMESTAMP_NTZ(9) COMMENT 'Load Datetime is the date and time in UTC when the record was created. For example, 2021-08-11T15:02:24Z.',
	UPDATE_ID NUMBER(38,0) COMMENT 'Update Identifier specifies the batch id used for the update of the record.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) COMMENT 'Update Datetime is the date and time in UTC when the record was last updated. For example, 2021-08-11T15:02:24Z.',
	PUSH_DEVICE_ID VARCHAR(16777216),
	ADDRESS_OPT_IN_IND BOOLEAN,
	IGNORE_FRAUD_SUSPEND_IND BOOLEAN,
	EMAIL_OPT_OUT_STATUS_IND BOOLEAN,
	CUSTOMER_AUTHENTICATION_STATUS VARCHAR(16777216) COMMENT 'Customer Authentication Status Values will be CUSTOMER for authenticated users (loyalty) via a digital platform or E-LEGACY  for unauthenticated user (non-loyalty).',
	CUSTOMER_EXTERNAL_CIP_ID VARCHAR(16777216) COMMENT 'Core ID, a unique Cross Brand Customer Id for an Individual. Sourced from a third-Party Agency.',
	CUSTOMER_CIP_ID VARCHAR(16777216) COMMENT 'Inspire Generated Unique Customer Identification at Brand Level. Based on Inspire Internal Business/Natural key attributes available during customer signup and/or customer transactions (orders).',
	constraint XAK1CUSTOMER unique (BRAND_ID, MBR_ID),
	constraint XPKCUSTOMER primary key (BRAND_ID, MBR_INSPIRE_ID)
);
create TABLE IF NOT EXISTS CUSTOMER_INTAKE (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216) COMMENT 'Digital Customer Identifier, specific to IDP source system. This field is used for IDP sources only.',
	LOYALTY_ID VARCHAR(16777216) COMMENT 'Customer Profile Identifier, specific to Loyalty source systems. This field is used for Loyalty sources only.',
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Source record ID specific to brand customer - MD5(Brand,Email,Mobile). This field is used for internal CIP purposes only to identify unique customer record.',
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL COMMENT 'Process Status specifies the status of the record in the process. For example, `Ready for preprocessing`, `In Progress`, `Completed`, etc.',
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Process Datetime is the date and time in UTC when the record was processed by CIP process.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	LOAD_TYPE VARCHAR(16777216) NOT NULL COMMENT 'Type of current load: Incremental or historical',
	constraint XAK1CUSTOMER_INTAKE unique (BRAND_ID, SOURCE_SYSTEM_NAME, IDP_BRAND_CUSTOMER_ID, LOYALTY_ID, CIP_SRC_RECORD_ID, LOAD_DTTM, LOAD_TYPE)
);
create TABLE IF NOT EXISTS CUSTOMER_INTAKE_HIST_CATCH_UP_1 (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216) COMMENT 'Digital Customer Identifier, specific to IDP source system. This field is used for IDP sources only.',
	LOYALTY_ID VARCHAR(16777216) COMMENT 'Customer Profile Identifier, specific to Loyalty source systems. This field is used for Loyalty sources only.',
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Source record ID specific to brand customer - MD5(Brand,Email,Mobile). This field is used for internal CIP purposes only to identify unique customer record.',
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL COMMENT 'Process Status specifies the status of the record in the process. For example, `Ready for preprocessing`, `In Progress`, `Completed`, etc.',
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Process Datetime is the date and time in UTC when the record was processed by CIP process.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1CUSTOMER_INTAKE unique (BRAND_ID, SOURCE_SYSTEM_NAME, CIP_SRC_RECORD_ID, LOAD_DTTM)
);
create TABLE IF NOT EXISTS CUSTOMER_INTAKE_HIST_LOAD (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216) COMMENT 'Digital Customer Identifier, specific to IDP source system. This field is used for IDP sources only.',
	LOYALTY_ID VARCHAR(16777216) COMMENT 'Customer Profile Identifier, specific to Loyalty source systems. This field is used for Loyalty sources only.',
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Source record ID specific to brand customer - MD5(Brand,Email,Mobile). This field is used for internal CIP purposes only to identify unique customer record.',
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL COMMENT 'Process Status specifies the status of the record in the process. For example, `Ready for preprocessing`, `In Progress`, `Completed`, etc.',
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Process Datetime is the date and time in UTC when the record was processed by CIP process.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1CUSTOMER_INTAKE unique (BRAND_ID, SOURCE_SYSTEM_NAME, CIP_SRC_RECORD_ID, LOAD_DTTM)
);
create TABLE IF NOT EXISTS CUSTOMER_INTAKE_HIST_LOAD_BACKUP (
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	MIDDLE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	PHONE_NBR VARCHAR(16777216),
	ADDRESS_LINE1_TXT VARCHAR(16777216),
	ADDRESS_LINE2_TXT VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	PROCESS_STATUS VARCHAR(16777216),
	PROCESS_DTTM TIMESTAMP_NTZ(9),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS CUSTOMER_INTAKE_HIST_LOAD_BACKUP_222024 (
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	MIDDLE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	PHONE_NBR VARCHAR(16777216),
	ADDRESS_LINE1_TXT VARCHAR(16777216),
	ADDRESS_LINE2_TXT VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	PROCESS_STATUS VARCHAR(16777216),
	PROCESS_DTTM TIMESTAMP_NTZ(9),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS CUSTOMER_INTAKE_LOYALTY (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216) COMMENT 'Digital Customer Identifier, specific to IDP source system. This field is used for IDP sources only.',
	LOYALTY_ID VARCHAR(16777216) COMMENT 'Customer Profile Identifier, specific to Loyalty source systems. This field is used for Loyalty sources only.',
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Source record ID specific to brand customer - MD5(Brand,Email,Mobile). This field is used for internal CIP purposes only to identify unique customer record.',
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	LOAD_TYPE VARCHAR(16777216) NOT NULL COMMENT 'Type of current load: Incremental or historical',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL COMMENT 'Process Status specifies the status of the record in the process. For example, `Ready for preprocessing`, `In Progress`, `Completed`, etc.',
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Process Datetime is the date and time in UTC when the record was processed by CIP process.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1CUSTOMER_INTAKE unique (BRAND_ID, SOURCE_SYSTEM_NAME, CIP_SRC_RECORD_ID, LOAD_DTTM)
);
create TABLE IF NOT EXISTS CUSTOMER_INTAKE_TEST_AIRFLOW (
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216) COMMENT 'Digital Customer Identifier, specific to IDP source system. This field is used for IDP sources only.',
	LOYALTY_ID VARCHAR(16777216) COMMENT 'Customer Profile Identifier, specific to Loyalty source systems. This field is used for Loyalty sources only.',
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Source record ID specific to brand customer - MD5(Brand,Email,Mobile). This field is used for internal CIP purposes only to identify unique customer record.',
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL COMMENT 'Process Status specifies the status of the record in the process. For example, `Ready for preprocessing`, `In Progress`, `Completed`, etc.',
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Process Datetime is the date and time in UTC when the record was processed by CIP process.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	LOAD_TYPE VARCHAR(16777216) NOT NULL COMMENT 'Type of current load: Incremental or historical',
	constraint XAK1CUSTOMER_INTAKE unique (BRAND_ID, SOURCE_SYSTEM_NAME, IDP_BRAND_CUSTOMER_ID, LOYALTY_ID, CIP_SRC_RECORD_ID, LOAD_DTTM, LOAD_TYPE)
);
create TABLE IF NOT EXISTS CUSTOMER_TEST_TODAY (
	BRAND_ID VARCHAR(16777216),
	MBR_INSPIRE_ID VARCHAR(16777216),
	MBR_ID VARCHAR(16777216),
	CLOSEST_STORE_ID VARCHAR(16777216),
	HOUSEHOLD_ID VARCHAR(16777216),
	EMAIL_ID VARCHAR(16777216),
	MOBILE_DEVICE_ID VARCHAR(16777216),
	EXPERIAN_ID VARCHAR(16777216),
	EXPERIAN_STATUS_TYP VARCHAR(16777216),
	INSPIRE_CUST_TYP VARCHAR(16777216),
	MDM_ID VARCHAR(16777216),
	MDM_ID_DELETED_IND BOOLEAN,
	LOYALTY_CARD_NBR VARCHAR(16777216),
	FIRST_NM VARCHAR(16777216),
	LAST_NM VARCHAR(16777216),
	MIDDLE_INITIAL_TXT VARCHAR(16777216),
	DOB_DT DATE,
	BIRTH_MNTH_ID NUMBER(38,0),
	BIRTH_YEAR_ID NUMBER(38,0),
	BIRTH_DT_IMPLIED_IND BOOLEAN,
	GENDER_TYP VARCHAR(16777216),
	ENROLLMENT_CHANNEL_TYP VARCHAR(16777216),
	MBR_STATUS_CD VARCHAR(16777216),
	POINT_BALANCE_QTY NUMBER(38,0),
	MEMBERSHIP_STATUS_CD VARCHAR(16777216),
	PROFILE_COMPLETED_STATUS_IND BOOLEAN,
	DELIVERABILITY_STATUS_IND VARCHAR(16777216),
	MOBILE_NBR VARCHAR(16777216),
	ADR_LINE_1_TXT VARCHAR(16777216),
	ADR_LINE_2_TXT VARCHAR(16777216),
	CTY_NM VARCHAR(16777216),
	ST_CD VARCHAR(16777216),
	ZIP_CD VARCHAR(16777216),
	CNTRY_CD VARCHAR(16777216),
	EMAIL_OPT_OUT_IND BOOLEAN,
	PUSH_NOTIFICATION_OPT_IN_IND BOOLEAN,
	SMS_OPT_IN_IND BOOLEAN,
	PRIVACY_IND BOOLEAN,
	UNSUBSCRIBE_DTTM TIMESTAMP_NTZ(9),
	POINT_EXPIRE_DTTM TIMESTAMP_NTZ(9),
	ENROLL_START_DTTM TIMESTAMP_NTZ(9),
	LAST_LOGIN_DTTM TIMESTAMP_NTZ(9),
	LAST_STATUS_CHANGE_DTTM TIMESTAMP_NTZ(9),
	PROFILE_COMPLETION_DTTM TIMESTAMP_NTZ(9),
	SUBSCRIBER_KEY VARCHAR(200),
	SUBSCRIBER_SOURCE_NM VARCHAR(100),
	LOYALTY_TIER_CHANGE_DTTM TIMESTAMP_NTZ(9),
	LOYALTY_TIER_NM VARCHAR(16777216),
	LOYALTY_TIER_EXPIRATION_DT DATE,
	LOYALTY_ELITE_VISIT_CNT NUMBER(38,0),
	SOURCE_SYSTEM_NM VARCHAR(16777216),
	CDM_LOAD_DT DATE,
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	PUSH_DEVICE_ID VARCHAR(16777216),
	ADDRESS_OPT_IN_IND BOOLEAN,
	IGNORE_FRAUD_SUSPEND_IND BOOLEAN,
	EMAIL_OPT_OUT_STATUS_IND BOOLEAN,
	CUSTOMER_AUTHENTICATION_STATUS VARCHAR(16777216),
	CUSTOMER_EXTERNAL_CIP_ID VARCHAR(16777216),
	CUSTOMER_CIP_ID VARCHAR(16777216)
);
create TABLE IF NOT EXISTS DEV_LOYALTY_EPSILON_RESPONSE_DEMO_APPEND (
	SRC_JOB_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcRecordId, this provides a unique identifier that links data back to the original data. This is provided on input from the user.',
	SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcJobId, this provides a unique identifier that links data back to the original data. This is provided on input from the user.',
	TSP_MATCH_LEVEL VARCHAR(16777216) COMMENT 'Indicates if the person is identified HOUSEHOLD/INDIVIDUAL',
	PERSON_SEQ_NO VARCHAR(16777216) COMMENT 'Person Seq No',
	DS_AGE_UNKNOWN_OF_ADULTS VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults in the household',
	DS_AGE_75_YEAR_OLD_SPECIFIC VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 75+ in the household',
	DS_AGE_65_74_YEAR_OLD_SPECIFIC VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 65-74 in the household',
	DS_AGE_55_64_YEAR_OLD_SPECIFIC VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 55-64 in the household',
	DS_AGE_45_54_YEAR_OLD_SPECIFIC VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 45-54 in the household',
	DS_AGE_35_44_YEAR_OLD_SPECIFIC VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 35-44 in the household',
	DS_AGE_25_34_YEAR_OLD_SPECIFIC VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 25-34 in the household',
	DS_AGE_18_24_YEAR_OLD_SPECIFIC VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of adults age 18-24 in the household',
	DS_CHILDREN_AGE_0_TO_2_ENH VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 0 and 2',
	DS_CHILDREN_AGE_3_TO_5_ENH VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 3 and 5',
	DS_CHILDREN_AGE_6_TO_10_ENH VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 6 and 10',
	DS_CHILDREN_AGE_11_TO_15_ENH VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 11 and 15',
	DS_CHILDREN_AGE_16_TO_17_ENH VARCHAR(16777216) COMMENT 'Indicates the presence (and gender) of a child in the household between or equal to the ages of 15 and 17',
	DS_KNOWN_NUMBER_OF_CHILDREN_IN_HOUSEHOLD VARCHAR(16777216) COMMENT 'The number of known children in the household; Unknown should not be interpreted as `No` children',
	DS_HOUSEHOLD_TYPE_CODE VARCHAR(16777216) COMMENT 'Represents the makeup of the household, such as married couple with children, married couple with no children or single female with no children',
	DS_NUMBER_OF_GENERATIONS_INSTALL_ENH VARCHAR(16777216) COMMENT 'Number of generations living within a household',
	TXL_ADV_DWELLING_TYPE VARCHAR(16777216) COMMENT 'Type of structure of the building in which the household resides. Based upon Record Type Code from USPS and property tax/deed data.',
	TXL_ADV_DWELLING_TYPE_IND VARCHAR(16777216) COMMENT 'Specific (S = household specific, H = household inferred and A = area inferred); `A` value only offered for DE',
	TXL_ADV_HOME_OWNER VARCHAR(16777216) COMMENT 'Classification of household head as an owner/renter. Under each category there is a definite and probable classification.',
	TXL_ADV_HOME_OWNER_IND VARCHAR(16777216) COMMENT 'Specific (S = household specific, H = household inferred and A = area inferred); `A` value only offered for DE',
	TXL_ADV_HH_MARITAL_STATUS VARCHAR(16777216) COMMENT 'Married or single',
	TXL_ADV_HH_MARITAL_STATUS_IND VARCHAR(16777216) COMMENT 'Advantage Household Marital Status Indicator',
	TXL_ADV_NUMBER_ADULTS VARCHAR(16777216) COMMENT 'Number of adults in the household',
	TXL_ADV_NUMBER_ADULTS_IND VARCHAR(16777216) COMMENT 'Advantage Number of Adults Indicator',
	TXL_ADV_LENGTH_OF_RESIDENCE VARCHAR(16777216) COMMENT 'Dervied from both Assessor data and the origination date of record on the file at the current address. A one-digit code indicating the length of time a household has been identified at this address.',
	TXL_ADV_LENGTH_OF_RESIDENCE_IND VARCHAR(16777216) COMMENT 'Length of time that the surname has been at this particular address; `A` value only offered for DE',
	TXL_ADV_HH_AGE_ENH VARCHAR(16777216) COMMENT 'Age range of current head of household will be specific if an age source was received for household head, or age is inferred if specific age source is not available',
	TXL_ADV_HH_AGE_IND_ENH VARCHAR(16777216) COMMENT 'Age range of current head of household will be specific if an age source was received for household head, or age is inferred if specific age source is not available',
	TXL_ADV_PRESENCE_CHILDREN_ENH VARCHAR(16777216) COMMENT 'Child present in household; Unknown should not be interpreted as `No` children',
	TXL_ADV_PRESENCE_CHILDREN_IND_ENH VARCHAR(16777216) COMMENT 'Advantage Presence of Children Indicator (Enhanced)',
	TXL_ADV_HH_SIZE_ENH VARCHAR(16777216) COMMENT 'Number of persons in the household',
	TXL_ADV_HH_SIZE_IND_ENH VARCHAR(16777216) COMMENT 'Advantage Household Size Indicator (Enhanced)',
	DS_EDUCATION_ENHANCED_INSTALL VARCHAR(16777216) COMMENT 'Median school years completed by adults age 18 or older',
	DS_EDUCATION_ENHANCED_INSTALL_IND VARCHAR(16777216) COMMENT 'Advantage Household Education Indicator (Enhanced)',
	DS_OCCUPATION VARCHAR(16777216) COMMENT 'Most prominent known profession of everybody in the household; self-reported data and occupation titles; an occupation associated (by a source) with someone in the household. In the case of multiple source occupations known within the household, the highest priority (lowest numerical value) occupation will be used.',
	S3_3_GENDER_INSTALL VARCHAR(16777216) COMMENT 'The gender of the individual',
	S3_MEMBER_CODE_OF_PERSON_INSTALL VARCHAR(16777216) COMMENT 'The year and month (YYYYMM) this surname and address were most recently verified by the most current source list.',
	DS_IND_DEMO_VERIFICATION_DATE VARCHAR(16777216) COMMENT 'The year and month (YYYYMM) this surname and address were most recently verified by the most current source list.',
	TXL_ADV_INDIV_MARITAL_STATUS VARCHAR(16777216) COMMENT 'Marital status of individual',
	TXL_ADV_INDIV_MARITAL_STATUS_IND VARCHAR(16777216) COMMENT 'Specific (S = household specific, H = household inferred and A = area inferred);',
	DS_AGE_INDIVIDUAL_EXACT_INSTALL_ENH VARCHAR(16777216) COMMENT 'Individual exact age',
	DS_AGE_INDIVIDUAL_EXACT_IND_INSTALL_ENH VARCHAR(16777216) COMMENT 'Birthday - Month Indicator (Enhanced)',
	DS_LANGUAGE_CODE_INSTALL VARCHAR(16777216) COMMENT 'Indicates whether or not Person 2 speaks a specific language',
	DS_LIFE_HOBBIES_BAKING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_HOBBIES_COOKING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_HOBBIES_CRAFTS_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_HOBBIES_FOOD_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_HOBBIES_WINES_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_MAILORDER_FOOD_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_DIET_NUTRITION_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_DIET_WEIGHT_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_DIET_FOODS_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_DIET_VITAMINS_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_BOOKS_COOKING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_BOOKS_SPORTS_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_BOOKS_WORLDNEWS_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_SPORTS_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_CYCLING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_BOATING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_CAMPING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_FISHING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_FITNESS_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_GOLF_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_HUNTING_SHOOTING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_HUNTING_GAME_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_NASCAR_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_RUNNING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_SKIING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_WALKING_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_LIFE_SPORTS_YOGA_ALL VARCHAR(16777216) COMMENT 'Interest present in HH',
	DS_MT_ALCOHOL_BEV_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Alcohol Beverage Switchers model, which ranks consumers based on their likelihood to switch brands. Model employs TSP data.',
	DS_MT_BEER_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Beer Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_BRAND_LOYALISTS VARCHAR(16777216) COMMENT 'Develop shopping list that includes specific brand preferences (Chobani vs. yogurt) and focus on brands vs. price',
	DS_MT_BREAKFAST_MEAT_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Breakfast Meat Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_BUDGET_MEAL_PLANNERS VARCHAR(16777216) COMMENT 'Plan meals in advance to manage budgets and stretch dollars',
	DS_MT_CANNED_SOUP_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Canned Soup Purchasers model which ranks consumers based upon their likelihood to purchase canned soup. Model employs TSP data.',
	DS_MT_CHOCOLATE_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Chocolate Candy Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_CLICK_TO_CART_HOME_DELIVERY_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s Click to Cart - Home Delivery Customers model which ranks consumers based upon their likelihood to order groceries online and have them delivered. Model employs TSP data. ',
	DS_MT_CLICK_TO_CART_PICKUP_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s Click to Cart - Pick Up Customers model which rankd consumers based upon their likelihood to order groceries online and pick up at the store. Model employs TSP data. ',
	DS_MT_COKE_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Coca Cola Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_CEREAL_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Cold Cereal Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_COFFE_BRAND_SWITCHERS VARCHAR(16777216) COMMENT 'Epsilon`s Coffee Brand Switchers model which ranks consumers based upon their likelihood to switch coffee brands. Model employs TSP data.',
	DS_MT_CONVENIENCE_COOK VARCHAR(16777216) COMMENT 'Cooks focused on deliver easier to prepare and faster meals',
	DS_MT_CRAFT_BEER_ENTH VARCHAR(16777216) COMMENT 'Epsilon`s Craft Beer Enthusiasts model which ranks consumers based upon their likelihood to consume craft beer. Model employs TSP data.',
	DS_MT_DOMESTIC_BEER_ENTH VARCHAR(16777216) COMMENT 'Epsilon`s Domestic Beer Enthusiasts model which ranks consumers based upon their likelihood to consume domestic beer. Model employs TSP data.',
	DS_MT_ENERGY_DRINK_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Energy Drink Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_EVERY_DAY_LOW_PRICE_SHOPPERS VARCHAR(16777216) COMMENT 'Greater likelihood to shop at or favor everyday low price format (example: Walmart)',
	DS_MT_EXPERIMENTAL_COOKS VARCHAR(16777216) COMMENT 'Cooks that like to explore new recipes and meal ideas',
	DS_MT_FLAVORED_DRINK_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Flavored Drink Purchasers model ranks consumers based upon their likelihood to purchase.',
	DS_MT_FREQUENT_TAKEOUT_FOOD_HH VARCHAR(16777216) COMMENT 'Greater likelihood to purchase takeout food',
	DS_MT_FRESH_FOOD_SEEKERS VARCHAR(16777216) COMMENT 'Interested in and purchase fresh food (grocery perimeter items)',
	DS_MT_FROZEN_FOOD_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Frozen Food Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	MT_FRUIT_ALCOHOLIC_BEVERAGE_DRINKERS VARCHAR(16777216) COMMENT 'Epsilon`s Fruit Alcoholic Beverage Drinkers model ranks consumers based upon their likelihood to purchase.',
	DS_MT_GREEN_PRODUCT_PURCHASERS VARCHAR(16777216) COMMENT 'Purchase or seek products that are non toxic, energy and water-efficient, and harmless to the environment',
	DS_MT_GROCERY_LOYALTY_CARD_CUSTOMERS VARCHAR(16777216) COMMENT 'Use 3+ grocery store loyalty/rewards cards regularly',
	MT_HARD_LEMONADE_DRINKERS VARCHAR(16777216) COMMENT 'Epsilon`s Hard Lemonade Enthusiasts model ranks consumers based upon their likelihood to purchase.',
	MT_HARD_TEA_DRINKERS VARCHAR(16777216) COMMENT 'Epsilon`s Hard Tea Enthusiasts model ranks consumers based upon their likelihood to purchase.',
	DS_MT_HARD_SELTZER_ENTH VARCHAR(16777216) COMMENT 'Epsilon`s Hard Seltzer Enthusiasts model which ranks consumers based upon their likelihood to consume hard seltzer. Model employs TSP data.',
	DS_MT_HEALTHY_CEREALS_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Healthy Cereals Purchasers model which ranks consumers based upon their likelihood to purchase healthy cereals. Model employs TSP data.',
	DS_MT_HEAVY_FIBER_FOCUSED_FOOD_BUYERS VARCHAR(16777216) COMMENT 'Greater likelihood to purchase product high in fiber',
	DS_MT_HEAVY_COUPON_USERS VARCHAR(16777216) COMMENT 'Increased likelihood to be active coupon clippers and redeemers',
	DS_MT_HEAVY_GLUTEN_FREE_FOOD_BUYERS VARCHAR(16777216) COMMENT 'Greater likelihood to product without gluten',
	DS_MT_HEAVY_LOW_FAT_FOOD_BUYERS VARCHAR(16777216) COMMENT 'Greater likelihood to purchase low fat foods',
	DS_MT_IMPORT_BEER_ENTH VARCHAR(16777216) COMMENT 'Epsilon`s Import Beer Enthusiasts model which ranks consumers based upon their likelihood to consume import beer. Model employs TSP data.',
	DS_MT_IMPULSE_PURCHASERS VARCHAR(16777216) COMMENT 'Increased likelihood shoppers will purchase off list items during in-store shopping',
	DS_MT_INCENTIVE_SEEKERS VARCHAR(16777216) COMMENT 'Shoppers actively seeking price incentives (e.g. coupons) or researching what is on sale',
	DS_MT_LAUNDRY_NEW_PRODUCT_SEEKERS VARCHAR(16777216) COMMENT 'Actively seeks and interested in new laundry products',
	DS_MT_LIQUOR_ENTH VARCHAR(16777216) COMMENT 'Epsilon`s Liquor Enthusiasts model which ranks consumers based upon their likelihood to consume liquor. Model employs TSP data.',
	DS_MT_MASTER_COOK VARCHAR(16777216) COMMENT 'Skilled cooks that enjoy the experience of cooking',
	DS_MT_MEAL_PLANNERS VARCHAR(16777216) COMMENT 'Shoppers who develop weekly meal plans and shopping list to match their meal plans',
	DS_MT_MEATLESS_PREFERENCE_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s Meatless Preference Consumers model which ranks consumers based upon their likelihood to prefer meatless products. Model employs TSP data.',
	DS_MT_MILLER_LITE_SWITCHERS VARCHAR(16777216) COMMENT 'Epsilon`s Miller Lite Switchers Consumers model which ranks consumers based upon their likelihood to switch to drinking miller lite. Model employs TSP data.',
	DS_MT_MOBIL_SHOPPING_LIST_USERS VARCHAR(16777216) COMMENT 'Greater likelihood to make a shopping list on their mobile phone',
	DS_MT_MULTI_RETAILER VARCHAR(16777216) COMMENT 'Increased likelihood of going to multiple retailers to execute their shopping list and fulfil shopping trip needs',
	DS_MT_NEW_PRODUCT_SEEKERS_FOOD_AND_DRINK VARCHAR(16777216) COMMENT 'Epsilon`s New Product Seekers – Food and Drink model which ranks consumers based upon their likelihood to try new food and drinks. Model employs TSP data.',
	DS_MT_HEALTH_BAR_SWITCH VARCHAR(16777216) COMMENT 'MT - Nutritional Health Bar Brand Switchers',
	DS_MT_ONE_STOP_SHOPPERS VARCHAR(16777216) COMMENT 'Primarily shop at one retailer for each shopping trip',
	DS_MT_ON_THE_GO_FOOD_DRINK_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s On-the-Go Food/Drink Consumers model which ranks consumers based upon their likelihood to eat food and drink on the go. Model employs TSP data.',
	DS_MT_ON_THE_GO_SNACKERS VARCHAR(16777216) COMMENT 'Epsilon`s On-the-Go Snackers (Snack Food) model which ranks consumers based upon their likelihood to eat snack food on the go. Model employs TSP data.',
	DS_MT_ORGANIC_FOOD_PURCHASERS VARCHAR(16777216) COMMENT 'Seek food made from organic raw materials and organic certification',
	DS_MT_ORGANIC_PRODUCT_PURCHASERS VARCHAR(16777216) COMMENT 'Seek products made from organic raw materials and organic certification.',
	DS_MT_PAPER_SHOPPING_LIST_USERS VARCHAR(16777216) COMMENT 'Shoppers who make and use paper list',
	DS_MT_PASTA_SAUCE_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Pasta Sauce Purchasers model which ranks consumers based upon their likelihood to purchase pasta sauce. Model employs TSP data.',
	DS_MT_REAL_INGREDIENT_COOK VARCHAR(16777216) COMMENT 'Cook ingredients that take additional preparation work (buy lettuce vs. bagged salad)',
	DS_MT_RED_WINE_ENTH VARCHAR(16777216) COMMENT 'Epsilon`s Red Wine Enthusiasts model which ranks consumers based upon their likelihood to consume red wine. Model employs TSP data.',
	DS_MT_LUNCH_MEAT_SWITCH VARCHAR(16777216) COMMENT 'MT - Refrigerated Lunch Meat Brand Switchers',
	DS_MT_RETAILER_EMAIL_SUBSCRIBER VARCHAR(16777216) COMMENT 'Greater likelihood to subscribe to retailer emails',
	DS_MT_SALTY_SNACKS_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Salty Snacks Purchasers model which ranks consumers based upon their likelihood to purchase salty snacks. Model employs TSP data.',
	DS_MT_GRANOLA_SWITCH VARCHAR(16777216) COMMENT 'MT - Snack Bar/Granola Bar Brand Switchers',
	DS_MT_SNACK_CHEESE_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Snack Cheese Purchasers model which ranks consumers based upon their likelihood to purchase snack cheese. Model employs TSP data.',
	DS_MT_SOFT_DRINK_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Soft Drinks Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_SPIRITS_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Spirits Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_STOCK_UP_AT_GROCERY_STORES VARCHAR(16777216) COMMENT 'Shopper that complete larger dollar sales trips at traditional grocery stores',
	DS_MT_SUGAR_FREE_FOODS_BEVERAGES_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Sugar Free Foods/Beverages Purchasers model ranks consumers based upon their likelihood to purchase.',
	DS_MT_WHITE_WINE_ENTH VARCHAR(16777216) COMMENT 'Epsilon`s White Wine Enthusiasts model which ranks consumers based upon their likelihood to consume white wine. Model employs TSP data.',
	DS_MT_YOGURT_SWITCH VARCHAR(16777216) COMMENT 'Epsilon`s Yogurt Brand Switchers model which ranks consumers based upon their likelihood to switch brands. Model employs TSP data.',
	DS_MT_APP_USERS VARCHAR(16777216) COMMENT 'Epsilon`s App Users model which ranks consumers based upon their likelihood to use apps. Model employs TSP data.',
	DS_DIRECT_MEDIA_PREF_CUST VARCHAR(16777216) COMMENT 'Epsilon`s Direct Media Preference model ranks consumers based upon their likelihood to review media via postal mail. Model employs TSP data.',
	DS_MT_FANTASY_SPORTS_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Fantasy Sports Enthusiasts model which ranks consumers based upon their likelihood to participate in fantasy sports. Model employs TSP data.',
	DS_SPORTS_READERS VARCHAR(16777216) COMMENT 'Epsilon`s Sports Reader model which ranks consumers based upon their likelihood to read sports books/magazines. Model employs TSP data. ',
	DS_MT_AMERICAN_DINER_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s American Diner Enthusiasts model ranks consumers based upon their likelihood to dine at American diners.',
	DS_MT_BAR_LOUNGE_FOOD_ENTHUSIAST VARCHAR(16777216) COMMENT 'Epsilon`s Bar and Lounge Food Enthusiasts model which ranks consumers based upon their likelihood to go to bars, clubs or lounges 3x a week or more. Model employs TSP data.',
	DS_BARGAIN_SHOPPERS VARCHAR(16777216) COMMENT 'Epsilon`s Bargain Shoppers model ranks consumers based upon their likelihood to shop on sale or use coupons. ',
	DS_MT_BRAND_LOYAL_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s Brand Loyal Consumers model which ranks consumers based upon their likelihood to be brand loyal. Model employs TSP data.',
	DS_MT_BREAKFAST_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Breakfast Dining Enthusiasts model ranks consumers based upon their likelihood to dine out frequently for breakfast. Model employs TSP data.',
	DS_MT_BURGER_DINERS VARCHAR(16777216) COMMENT 'Epsilon`s Burger Diners model which ranks consumers based upon their likelihood to be burger diners. Model employs TSP data.',
	DS_MT_BUY_ONLINE_PICK_UP_IN_STORE_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s Buy Online Pick Up In Store model ranks consumers based upon their likelihood to be buy online-pickup in store consumers.',
	DS_MT_CARRYOUT_ENTHUSIAST VARCHAR(16777216) COMMENT 'Epsilon`s Carry Out Enthusiasts model which ranks consumers based upon their likelihood to go to carry out restaurants 3x a week or more. Model employs TSP data.',
	DS_MT_CASUAL_DINING_ENTHUSIAST VARCHAR(16777216) COMMENT 'Epsilon`s Casual Dining Enthusiasts model which ranks consumers based upon their likelihood to go to casual dining restaurants 3x a week or more. Model employs TSP data.',
	DS_MT_CATERING_DELIVERY_CUSTOMERS VARCHAR(16777216) COMMENT 'Ordered catered food online and had it delivered in the last 12 months',
	DS_MT_CATERING_PICKUP_CUSTOMERS VARCHAR(16777216) COMMENT 'Ordered catered food online 2+ times and picked it up in the last 12 months',
	DS_MT_CHICKEN_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Chicken Dining Enthusiasts model which ranks consumers based upon their likelihood to choose chicken when dining out. Model employs TSP data.',
	DS_MT_CHINESE_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Chinese Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Chinese restuarants.',
	DS_MT_CLEAN_EATING_DINING_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s Clean Eating Dining Customers model ranks consumers based upon the likelihood to be clean eating diners. Model employs TSP data. ',
	DS_MT_COFFEE_ENTHUSIAST VARCHAR(16777216) COMMENT 'Epsilon`s Coffee Enthusiasts model which ranks consumers based upon their likelihood to go to coffee houses/cafes and get coffee to go at least 3x a month. Model employs TSP data.',
	DS_MT_DINNER_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Dinner Dining Enthusiasts model ranks consumers based upon their likelihood to dine out frequently for dinner. Model employs TSP data.',
	DS_MT_DOORDASH_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s DoorDash Dining Enthusiasts model which ranks consumers based upon their likelihood to use DoorDash. Model employs TSP data.',
	DS_MT_EXTREME_FITNESS_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Extreme Fitness Enthusiast model ranks consumers based upon their likelihood to participate in P90X, CrossFit or other fitness programs. Model employs TSP data.',
	DS_MT_FINE_DINING_ENTHUSIAST VARCHAR(16777216) COMMENT 'Epsilon`s Fine dining Enthusiasts model which ranks consumers based upon their likelihood to go to fine dining restaurants 3x a week or more. Model employs TSP data.',
	DS_MT_FITNESS_EQUIPMENT_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Fitness Equipment Purchasers model which ranks consumers based upon their likelihood to purchase fitness equipment. Model employs TSP data.',
	DS_MT_FOOD_TRUCK_CONSU VARCHAR(16777216) COMMENT 'Epsilon`s Food Truck Consumers model which ranks consumers based upon their likelihood to purchase from food trucks. Model employs TSP data.',
	DS_MT_FRESH_FOOD_DELIVERY_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s Fresh Food Delivery Consumers model ranks consumers based upon their likelihood to have fresh food delivered. Model employs TSP data. ',
	DS_MT_FRIED_CHICKEN_SANDWICH_DINERS VARCHAR(16777216) COMMENT 'Epsilon`s Fried Chicken Sandwich Diners model which ranks consumers based upon their likelihood to be fried chicken sandwich diners. Model employs TSP data.',
	DS_MT_FRIED_FISH_SANDWICH_DINERS VARCHAR(16777216) COMMENT 'Epsilon`s Fried Fish Sandwich Diners model which ranks consumers based upon their likelihood to be fried fish diners. Model employs TSP data.',
	DS_MT_GLUTEN_FREE_DINING_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s Gluten Free Dining Customers model ranks consumers based on the likelihood to be gluten free diners. Model employs TSP Data',
	DS_MT_GREEK_MIDDLE_EASTERN_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Greek/Middle Eastern Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Greek/Middle Eastern restuarants.',
	DS_MT_GRILLED_CHICKEN_SANDWICH_DINERS VARCHAR(16777216) COMMENT 'Epsilon`s Fried Chicken Sandwich Diners model which ranks consumers based upon their likelihood to be fried chicken sandwich diners. Model employs TSP data.',
	DS_MT_GROCERY_STORE_APP_USERS VARCHAR(16777216) COMMENT 'Epsilon`s Grocery Store App Users model ranks consumers based upon their likelihood to use grocery apps when they shop. Model employs TSP data.',
	DS_MT_GROCERY_STORE_FREQUENTERS VARCHAR(16777216) COMMENT 'Epsilon`s Grocery Store Frequenters model ranks consumers based upon their likelihood to shop frequently at grocery stores. Model employs TSP data.',
	DS_MT_GRUBHUB_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Grubhub Dining Enthusiasts model which ranks consumers based upon their likelihood to use Grubhub. Model employs TSP data.',
	DS_MT_HOT_DOG_DINERS VARCHAR(16777216) COMMENT 'Epsilon`s Hot Dog Diners model which ranks consumers based upon their likelihood to be hot dog diners. Model employs TSP data.',
	DS_MT_INTERNET_CONNECTED_FITNESS_USERS VARCHAR(16777216) COMMENT 'Epsilon`s Internet Connected Fitness Users model which ranks consumers based upon their likelihood to use internet connected fitness products. Model employs TSP data.',
	DS_MT_ITALIAN_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Italian Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Italian restuarants.',
	DS_MT_JAPANESE_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Japanese Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Japanese restuarants.',
	DS_MT_LOW_CARB_DIET_DINING_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s Low Carb Diet Dining Customers model ranks consumers based on their likelihood to be low carb diners. Model employs TSP Data',
	DS_MT_LOW_SODIUM_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s Low Sodium Consumers model ranks consumers based upon their likelihood to have low sodium diets. Model employs TSP data.',
	DS_MT_LUNCH_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Lunch Dining Enthusiasts model ranks consumers based upon their likelihood to dine out frequently for lunch. Model employs TSP data.',
	DS_MT_MEAL_COMBO_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s Meal Combo Customers model ranks consumers based upon their likelihood to purchase meal combos at restaurants.',
	DS_MT_MEAL_KIT_DELIVERY_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s Meal Kit Delivery Consumers model which ranks consumers based upon their likelihood to have meal kits delivered. Model employs TSP data.',
	DS_MT_MEXICAN_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Mexican Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Mexican restuarants.',
	DS_MT_ONLINE_PICKUP_RESTAURANT_CUSTOMERS VARCHAR(16777216) COMMENT 'Epslon`s Online - Pick Up Restaurant Customers model ranks consumers on their likelihood to order food online and pick up in the restaurant. Model employs TSP data. ',
	DS_MT_OTHER_DIRECT_DELIVERY_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Other Direct Delivery Dining Enthusiasts model which ranks consumers based upon their likelihood to use other direct delivery companies. Model employs TSP data.',
	DS_MT_PAND_IN_RESTAURANT VARCHAR(16777216) COMMENT 'Epsilon`s Pandemic - In Restaurant Diners model which ranks consumers based upon their likelihood to dine in restaurants during the pandemic. Model employs TSP data.',
	DS_MT_PIZZA_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Pizza Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Pizza restuarants.',
	DS_MT_PLANT_BASED_DINING_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s Plant Based Dining Customers model ranks consumers based on their likelihood to be plant based diners. Model employs TSP data',
	DS_MT_POSTMATES_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Postmates Dining Enthusiasts model which ranks consumers based upon their likelihood to use Postmates. Model employs TSP data.',
	DS_MT_QSR_CASH_CONSUMERS VARCHAR(16777216) COMMENT 'Epsilon`s QSR Cash Customers model ranks consumers based upon their likelihood to pay cash at QSRs.',
	DS_MT_QUICK_SERVICE_RESTAURANT_ENTHUSIAST VARCHAR(16777216) COMMENT 'Epsilon`s Quick Service Restaurant Enthusiasts model which ranks consumers based upon their likelihood to go to quick service restaurants 3x a week or more. Model employs TSP data.',
	DS_MT_RESTAURANT_APP_USERS VARCHAR(16777216) COMMENT 'Regularly use 2+ apps for restaurants',
	DS_MT_RESTAURANT_GIFT_CARD_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Restaurant Gift Card Purchasers model which ranks consumers based upon their likelihood to purchase restaurant gift cards. Model employs TSP data.',
	DS_MT_RESTAURANT_LOYALTY_APP_USERS VARCHAR(16777216) COMMENT 'Use restaurant apps that have a connection to a loyalty or rewards program',
	DS_MT_RESTAURANT_LOYALTY_CARD_CUSTOMERS VARCHAR(16777216) COMMENT 'Use 3+ restaurant loyalty cards regularly',
	DS_MT_SANDWICH_DINERS VARCHAR(16777216) COMMENT 'Epsilon`s Sandwich Diners model which ranks consumers based upon their likelihood to be sandwich diners. Model employs TSP data.',
	DS_MT_STORE_GIFT_CARD_PURCHASERS VARCHAR(16777216) COMMENT 'Epsilon`s Store Gift Card Purchasers model which ranks consumers based upon their likelihood to purchase store gift cards. Model employs TSP data.',
	DS_MT_SUB_SANDWICH_DINERS VARCHAR(16777216) COMMENT 'Epsilon`s Sub Sandwich Diners model which ranks consumers based upon their likelihood to be sub sandwich diners. Model employs TSP data.',
	DS_MT_SUB_AUTO_SHIP_FOOD_BEV_CUSTOMERS VARCHAR(16777216) COMMENT 'Epsilon`s Subscription or Auto Shipment-Food or Beverage model ranks consumers based upon their likelihood to be subscribers.',
	DS_MT_THAI_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Thai Dining Enthusiasts model ranks consumers based upon their likelihood to dine at Thai restuarants.',
	DS_MT_UBER_EATS_DINING_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Uber Eats Dining Enthusiasts model which ranks consumers based upon their likelihood to use Uber Eats. Model employs TSP data.',
	DS_MT_VEGETARIANS VARCHAR(16777216) COMMENT 'Epsilon`s Vegetarians model ranks consumers based upon their likelihood to be vegetarians.',
	DS_BASKETBALL_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Basketball Enthusiast model ranks consumers based upon their likelihood to plan to watch basketball for entertainment. ',
	DS_DIET_CONSCIOUS_HOUSEHOLDS VARCHAR(16777216) COMMENT 'Epsilon`s Diet Conscious Households model ranks consumers based upon their likelihood to diet for health, eat low sugar, low fat or low calorie food diets, and have an interest in weight loss.',
	DS_FOOTBALL_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Football Enthusiast model ranks consumers based upon their likelihood to plan to watch football for entertainment. ',
	DS_MT_GYM_MEMBERS VARCHAR(16777216) COMMENT 'Epsilon`s Gym Members model which ranks consumers based upon their likelihood to belong to a gym. Model employs TSP data.',
	DS_MT_HOCKEY_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Hockey Enthusiast model ranks consumers based upon their likelihood to be interested in watching hockey for entertainment.',
	DS_MT_HOME_GYM_OWNERS VARCHAR(16777216) COMMENT 'Epsilon`s Home Gym Owners model which ranks consumers based upon their likelihood to have home gyms. Model employs TSP data.',
	DS_MT_INTERMITTENT_FASTERS VARCHAR(16777216) COMMENT 'Epsilon`s Intermittent Fasters model ranks consumers based upon their likelihood to practice intermittent fasting.',
	DS_NASCAR_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s NASCAR Enthusiast model ranks consumers based upon their likelihood to be interested in NASCAR. ',
	DS_ONLINE_TRANSACTOR VARCHAR(16777216) COMMENT 'Epsilon`s Online Transactor model ranks consumers based upon their likelihood to transact online.',
	DS_MT_PAND_DEC_SPEND VARCHAR(16777216) COMMENT 'Epsilon`s Pandemic - Decreased Spenders model which ranks consumers based upon their likelihood to have decreased spending during the pandemic. Model employs TSP data.',
	DS_FITNESS_MEMBERSHIP VARCHAR(16777216) COMMENT 'Epsilon`s Fitness Membership model ranks consumers based upon their likelihood to plan to get a membership in the next 12 months. ',
	MT_PRO_WRESTLING_WWE_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Pro Wrestling (WWE) Enthusiasts model ranks consumers based upon their likelihood to be a WWE enthusiast.',
	DS_MT_PROACTIVE_HEALTH_MANAGERS VARCHAR(16777216) COMMENT 'Epsilon`s Proactive Health Managers model ranks consumers based upon their likelihood to proactively manage their health. Model employs TSP Data',
	DS_MT_PRO_SPORTS_EVENTS_ATTENDEES VARCHAR(16777216) COMMENT 'Epsilon`s Professional Sports Events attendees model ranks consumers based upon their likelihood to attend these events.',
	DS_MT_SKIING_SNOWBOARD_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Skiing/Snowboarding Enthusiasts model which ranks consumers based upon their likelihood to own skis/snowboards. Model employs TSP data.',
	DS_MT_SOCCER_ENTHUSIASTS VARCHAR(16777216) COMMENT 'Epsilon`s Soccer Enthusiast model which ranks consumers based upon their likelihood to be interested in watching soccer on tv. Model employs TSP data.',
	DS_WELLNESS_HOUSEHOLDS VARCHAR(16777216) COMMENT 'Epsilon`s Wellness Household model ranks consumers based upon their likelihood to exercise on a regular basis, go to a doctor regularly, eat organic/healthy food diets and have an interest in natural foods.',
	DS_MT_YOGA_PILATES_ENTHUSIAST VARCHAR(16777216) COMMENT 'Epsilon`s Yoga/Pilates Enthusiasts model which ranks consumers based upon their likelihood to do yoga/pilates. Model employs TSP data.',
	DS_INCOME_TIERS_INSTALL_20 VARCHAR(16777216) COMMENT 'Modeled household income with removal of protected-class data elements in build algorithm. Model accesses eligible household data and geographic data.',
	DS_INCOME_TIERS_INSTALL_IND_20 VARCHAR(16777216) COMMENT 'Indicates household inferred or area inferred',
	NUM_ACT_SOURCES_VERIFIED VARCHAR(16777216) COMMENT 'The number of active sources to have verified this household',
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'Load  Identifier specifies the Batch that inserted the record into the table. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_ID NUMBER(38,0) NOT NULL COMMENT 'Update Identifier specifies the Batch that updated data. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	LOAD_FILENAME VARCHAR(16777216) NOT NULL COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1DEV_LOYALTY_EPSILON_RESPONSE_DEMO_APPEND unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS DEV_LOYALTY_EPSILON_RESPONSE_IDENTITY (
	SRC_JOB_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcRecordId, this provides a unique identifier that links data back to the original data. This is provided on input from the user.',
	SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcJobId, this provides a unique identifier that links data back to the original data.  This is provided on input from the user.',
	PROCESS_DATE VARCHAR(16777216) COMMENT 'Date the file was processed',
	CLIENT_NAMED_CORE_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreId; Individual ID; In order to accurately match to a NamedCoreId/IndividualID, records must have at a minimum: Name & Address (address only records will not be assigned a named CORE/individual id) OR Email address OR Phone number',
	CLIENT_NAMED_HH_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreHHId',
	IDENTITY_MATCH_CONFIDENCE VARCHAR(16777216) COMMENT 'Indicates match location and confidence of match to known individuals',
	PREFIX_NAME VARCHAR(16777216) COMMENT 'Name prefix',
	GIVEN_NAME VARCHAR(16777216) COMMENT 'First (given) name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Middle name',
	FAMILY_NAME VARCHAR(16777216) COMMENT 'Last (family) name',
	GENERATIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Generational suffix',
	PROFESSIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Professional Suffix',
	GENDER VARCHAR(16777216) COMMENT 'Gender',
	COMPANY_NAME VARCHAR(16777216) COMMENT 'Company or Business name',
	PROFANITY VARCHAR(16777216) COMMENT 'Profanity identified in the name',
	GIVEN_FAMILY_NAME_MATCH VARCHAR(16777216) COMMENT 'First name and last name match',
	POSSIBLE_BUSINESS VARCHAR(16777216) COMMENT 'The name on the record is a possible business name',
	LACS VARCHAR(16777216) COMMENT 'Locatable Address Conversion System indicator',
	LACS_NEW_ADDR_FLAG VARCHAR(16777216) COMMENT 'The type of LACS address',
	LACS_RETURN_CODE VARCHAR(16777216) COMMENT 'LACS footnote to indicates the validation of the address as LACS certified ',
	NCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective NCOA move date',
	NCOA_MOVE_TYPE VARCHAR(16777216) COMMENT 'Type of NCOA move',
	NCOA_LINK_FOOTNOTE VARCHAR(16777216) COMMENT 'NCOA Link return codes',
	PCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective PCOA move date',
	PAC_ACTION_CODE VARCHAR(16777216) COMMENT 'PAC action code',
	PAC_FOOTNOTE VARCHAR(16777216) COMMENT 'PAC footnote',
	ADDRESS_SOURCE VARCHAR(16777216) COMMENT 'The source of the best address',
	CURBSIDE_ID VARCHAR(16777216) COMMENT 'Best value for curbside ID (addressLine1)',
	ADDRESS_LINE1 VARCHAR(16777216) COMMENT 'Standardized address line 1',
	ADDRESS_LINE2 VARCHAR(16777216) COMMENT 'Standardized address line 2',
	ADDRESS_LINE3 VARCHAR(16777216) COMMENT 'Standardized address line 3',
	ADDRESS_LINE4 VARCHAR(16777216) COMMENT 'Standardized address line 4',
	LOCALITY1 VARCHAR(16777216) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb).',
	LOCALITY2 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	LOCALITY3 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	REGION1 VARCHAR(16777216) COMMENT 'State, province, territory, or region.',
	REGION2 VARCHAR(16777216) COMMENT 'Additional state, province, territory, or region.',
	POSTAL_CODE VARCHAR(16777216) COMMENT 'Standarized postal code',
	ZIP4 VARCHAR(16777216) COMMENT 'ZIP+4',
	SEC_RANGE VARCHAR(16777216) COMMENT 'Secondary range used for matching',
	DPBC VARCHAR(16777216) COMMENT 'The two-digit DPBC code.',
	CHECK_DIGIT VARCHAR(16777216) COMMENT 'Check digit for the delivery-point bar code, or for a five-digit bar code if a full postal code (ZIP+4) could not be assigned.',
	ISO_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	CART VARCHAR(16777216) COMMENT 'Carrier-route number',
	LOT VARCHAR(16777216) COMMENT 'Line-of-travel number',
	LOT_ORDER VARCHAR(16777216) COMMENT 'Line-of-travel sortation',
	REC_TYPE VARCHAR(16777216) COMMENT 'The record-type indicator for the assigned address',
	REMAINDER VARCHAR(16777216) COMMENT 'Extraneous data found on the address line, which either can’t be identified by the parser or does not belong in a standardized address',
	DPV_CMRA VARCHAR(16777216) COMMENT 'Commercial Mail Receiving Agency (CMRA) indicator ',
	DPV_FTNOTE VARCHAR(16777216) COMMENT 'DPV footnotes',
	DPV_FALSE_POS_IND VARCHAR(16777216) COMMENT 'DPV false-positive and triggered DPV',
	DPV_NO_STATS VARCHAR(16777216) COMMENT 'No Stat indicator. No Stat means that the address is a vacant property, it receives mail as a part of a drop, or it does not have an established delivery yet.',
	DPV_STATUS VARCHAR(16777216) COMMENT 'The DPV status component that is generated for this record.',
	FOREIGN_CODE VARCHAR(16777216) COMMENT 'Foreign address code',
	MATCH5 VARCHAR(16777216) COMMENT 'Lastline match level indicator',
	MATCH9 VARCHAR(16777216) COMMENT 'Addressline match level indicator',
	MATCH_UN VARCHAR(16777216) COMMENT 'Indicates whether the record is a deliverable address',
	ZIP_MOVE VARCHAR(16777216) COMMENT 'ZIP move indicator',
	ZIP_TYPE VARCHAR(16777216) COMMENT 'Type of ZIP Code assigned',
	CONGRESS VARCHAR(16777216) COMMENT 'District number for the U.S. House of Representatives',
	COUNTY VARCHAR(16777216) COMMENT 'Federal Information Processing Standard (FIPS) county code',
	COUNTY_NAME VARCHAR(16777216) COMMENT 'The fully-spelled county name',
	FAC_TYPE VARCHAR(16777216) COMMENT 'Type of postal facility',
	FIPS_CODE VARCHAR(16777216) COMMENT 'FIPS code',
	ERROR_CODE VARCHAR(16777216) COMMENT 'Address Error Codes',
	STAT_CODE VARCHAR(16777216) COMMENT 'Address Status Code',
	GEO_MATCH VARCHAR(16777216) COMMENT 'Match code indicating the precision of the latitude and longitude assignment',
	GEO_LAT VARCHAR(16777216) COMMENT 'Latitude (degrees north of the equator) in the format 12.123456',
	GEO_LNG VARCHAR(16777216) COMMENT 'Longitude (degrees west of the Greenwich Meridian) in the format -12.123456',
	AGEO_PLA VARCHAR(16777216) COMMENT 'FIPS place code. A number assigned by the U.S. government to each incorporated municipality (city, village, town, etc.)',
	GEO_BLK VARCHAR(16777216) COMMENT 'GEO block',
	AGEO_MCD VARCHAR(16777216) COMMENT 'U.S. Census Bureau minor civil division (MCD) data or, if MCD data is unavailable, census county division (CCD) data',
	CGEO_CBSA VARCHAR(16777216) COMMENT 'A Core-Based Statistical Area (CBSA) consists of:\n- A county with an incorporated place or a census-designated place that has a population of at least 10,000\n- Adjacent counties with at least 25 percent of employed residents of the county who work in the CBSA’s core or central county',
	CGEO_MSA VARCHAR(16777216) COMMENT 'Metropolitan Statistical Area (MSA) number. 0000 indicates the address does not lie in any MSA; usually a rural area',
	AGEO_STA VARCHAR(16777216) COMMENT 'FIPS state code',
	DSF_DROP_FLAG VARCHAR(16777216) COMMENT 'Drop indicator',
	DSF_THROWBACK_FLAG VARCHAR(16777216) COMMENT 'Throwback indicator',
	DSF_SEASONAL_FLAG VARCHAR(16777216) COMMENT 'Seasonal address indicator',
	DSF_VACANT_FLAG VARCHAR(16777216) COMMENT 'Vacant address indicator',
	DSF_DELIVERY_TYPE VARCHAR(16777216) COMMENT 'Delivery type',
	DSF_DROP_COUNT VARCHAR(16777216) COMMENT 'Drop count indicates the number of businesses or families served by this delivery point',
	DSF_LACS_FLAG VARCHAR(16777216) COMMENT 'LACS (Locatable Address Conversion System) indicator',
	DSF_EDUCATIONAL_FLAG VARCHAR(16777216) COMMENT 'Indicates that the address is seasonal and related to an educational institution',
	DSF_REC_TYPE VARCHAR(16777216) COMMENT 'DSF Record type',
	MAILABILITY_SCORE VARCHAR(16777216) COMMENT 'Mailability index',
	RDI_FLAG VARCHAR(16777216) COMMENT 'Best Residential Delivery Indicator',
	LINE1 VARCHAR(16777216) COMMENT 'Mail ready address block line 1',
	LINE2 VARCHAR(16777216) COMMENT 'Mail ready address block line 2',
	LINE3 VARCHAR(16777216) COMMENT 'Mail ready address block line 3',
	LINE4 VARCHAR(16777216) COMMENT 'Mail ready address block line 4',
	LINE5 VARCHAR(16777216) COMMENT 'Mail ready address block line 5',
	LINE6 VARCHAR(16777216) COMMENT 'Mail ready address block line 6',
	LINE7 VARCHAR(16777216) COMMENT 'Mail ready address block line 7',
	LINE8 VARCHAR(16777216) COMMENT 'Mail ready address block line 8',
	LINE9 VARCHAR(16777216) COMMENT 'Mail ready address block line 9',
	LINE10 VARCHAR(16777216) COMMENT 'Mail ready address block line 10',
	OCCUPANCY_SCORE VARCHAR(16777216) COMMENT 'Occupancy score - postal evidence within that timeframe that the person is/was at the input address. The lower the number the more recent the occupancy is verified',
	MULTI_TYPE VARCHAR(16777216) COMMENT 'Dwelling type',
	DECEASED VARCHAR(16777216) COMMENT 'Deceased Suppression Flag',
	DECEASED_DOB VARCHAR(16777216) COMMENT 'Date of Birth on Deceased',
	DECEASED_DOD VARCHAR(16777216) COMMENT 'Date of Death',
	DO_NOT_MAIL VARCHAR(16777216) COMMENT 'Do Not Mail (DNM) Suppression Flag',
	DO_NOT_CALL VARCHAR(16777216) COMMENT 'Do Not Call (DNC) Suppression Flag',
	DO_NOT_FAX VARCHAR(16777216) COMMENT 'Do Not Fax (DNF) Suppression Flag',
	PRISON VARCHAR(16777216) COMMENT 'Prison Address Suppression Flag',
	NURSING_HOME VARCHAR(16777216) COMMENT 'Nursing Home Address Suppression Flag',
	PHONE1 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE1 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE2 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE2 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE3 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE3 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	RETURN_CODE_NAME_HYGIENE VARCHAR(16777216),
	RETURN_CODE_ADDRESS VARCHAR(16777216),
	RETURN_CODE_IDENTITY VARCHAR(16777216),
	CC_MATCH_SOURCE VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'Load  Identifier specifies the Batch that inserted the record into the table. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_ID NUMBER(38,0) NOT NULL COMMENT 'Update Identifier specifies the Batch that updated data. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	LOAD_FILENAME VARCHAR(16777216) NOT NULL COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1DEV_LOYALTY_EPSILON_RESPONSE_IDENTITY unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS DIO_TEST_INPUT (
	ORDER_ID NUMBER(38,0),
	EMPLOYEE_ID NUMBER(38,0),
	EMPLOYEE_NAME VARCHAR(16777216),
	CHECK_NBR NUMBER(38,0),
	LOAD_TYP VARCHAR(16777216),
	ITEM_CNT NUMBER(38,0),
	SOURCE_GROSS_AMT NUMBER(38,0)
);
create TRANSIENT TABLE IF NOT EXISTS DNKN_MENU_ITEM_PRICE (
	STOREID VARCHAR(16777216),
	BUSINESSUNIT VARCHAR(16777216),
	ITEMS VARIANT,
	POSTYPE VARCHAR(16777216),
	BRAND VARCHAR(16777216),
	INSERTDATE VARCHAR(16777216),
	UPSERTDATE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS MISMATCH_RESULTS_DIM_RESTAURANT_DIM_RESTAURANT (
	TABLE1_COLUMN_NAME VARCHAR(16777216),
	IDS_QA_LOCN_DIM_RESTAURANT VARCHAR(16777216),
	TABLE2_COLUMN_NAME VARCHAR(16777216),
	IDS_DEV_LOCN_DIM_RESTAURANT VARCHAR(16777216)
);
create TABLE IF NOT EXISTS OPENWEATHER_FORECAST_5DAY_API_DICTIONARY (
	LOCATION VARCHAR(16777216),
	TEST VARCHAR(16777216)
);
create TABLE IF NOT EXISTS OPENWEATHER_FORECAST_5DAY_RAW (
	URL VARCHAR(16777216),
	RESPONSE VARIANT,
	TIMESTAMP NUMBER(38,0),
	EPOCH NUMBER(38,0),
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM VARCHAR(16777216),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM VARCHAR(16777216),
	LOAD_FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS RESTAURANT_GROUP_EVENT (
	SEQUENCENUMBER NUMBER(38,0),
	OFFSET VARCHAR(16777216),
	ENQUEUEDTIMEUTC VARCHAR(16777216),
	SYSTEMPROPERTIES VARIANT,
	PROPERTIES VARIANT,
	BODY VARIANT,
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS TEST (
	CUSTOMER_UUID VARCHAR(16777216),
	BRAND VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	DOB VARCHAR(16777216),
	FRAUD_STATUS VARCHAR(16777216),
	ACCOUNT_STATUS VARCHAR(16777216),
	PROFILE_COMPLETED VARCHAR(16777216),
	EMAIL_CONFIRMED VARCHAR(16777216),
	CREATED_TS VARCHAR(16777216),
	UPDATED_TS VARCHAR(16777216),
	DEFAULT_TNC_ACCEPTED VARCHAR(16777216),
	MEMBER_SINCE VARCHAR(16777216),
	MIGRATED VARCHAR(16777216),
	MIGRATION_SOURCE VARCHAR(16777216),
	GENDER VARCHAR(16777216),
	ID VARCHAR(16777216),
	PROFILE_AVATAR_URL VARCHAR(16777216),
	MFA_SMS_STATUS VARCHAR(16777216),
	MFA_EMAIL_STATUS VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TEST2 (
	ID VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	IDENTIFIER VARCHAR(16777216),
	OPT_IN VARCHAR(16777216),
	PUBLISHED_TS VARCHAR(16777216),
	CREATED_TS VARCHAR(16777216),
	UPDATED_TS VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TEST3 (
	ID VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	ADDRESS1 VARCHAR(16777216),
	ADDRESS2 VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	POSTAL_CODE VARCHAR(16777216),
	PREFERRED VARCHAR(16777216),
	CREATED_TS VARCHAR(16777216),
	UPDATED_TS VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_ID NUMBER(38,0),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TESTCATCHUP (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS TEST_CONF (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	SOURCE_SYSTEM_NM VARCHAR(16777216) NOT NULL,
	USECASE VARCHAR(16777216),
	SOURCE_DATA_FLOW VARCHAR(16777216),
	ACTIVE_IND BOOLEAN NOT NULL,
	COMMENT VARCHAR(16777216) COMMENT 'Comment is a description of the configuration.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1TEST_CONF unique (BRAND_ID, SOURCE_SYSTEM_NM, USECASE, SOURCE_DATA_FLOW)
);
create TABLE IF NOT EXISTS TEST_CUSTOMER (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL,
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL,
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	DATE_OF_BIRTH VARCHAR(16777216) COMMENT 'Customer date of birth',
	GENDER VARCHAR(16777216) COMMENT 'Customer gender',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	DEVICE_ID VARCHAR(16777216) COMMENT 'Customer device ID',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	STORE_LOCATION_ID VARCHAR(16777216) COMMENT 'Store location number',
	STORE_ZIP_CODE VARCHAR(16777216) COMMENT 'Store location postal code',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL,
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL
);
create TABLE IF NOT EXISTS TEST_CUSTOMER_1 (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL,
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL,
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	DATE_OF_BIRTH VARCHAR(16777216) COMMENT 'Customer date of birth',
	GENDER VARCHAR(16777216) COMMENT 'Customer gender',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	DEVICE_ID VARCHAR(16777216) COMMENT 'Customer device ID',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	STORE_LOCATION_ID VARCHAR(16777216) COMMENT 'Store location number',
	STORE_ZIP_CODE VARCHAR(16777216) COMMENT 'Store location postal code',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL,
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL
);
create TABLE IF NOT EXISTS TEST_CUSTOMER_INTAKE (
	BRAND_ID VARCHAR(16777216) NOT NULL,
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL,
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216) NOT NULL,
	FIRST_NAME VARCHAR(16777216) COMMENT 'Customer first name',
	LAST_NAME VARCHAR(16777216) COMMENT 'Customer last name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Customer middle name or initial',
	EMAIL_ADDRESS VARCHAR(16777216) COMMENT 'Customer email address',
	PHONE_NBR VARCHAR(16777216) COMMENT 'Customer phone number',
	ADDRESS_LINE1_TXT VARCHAR(16777216) COMMENT 'First line of mailing address',
	ADDRESS_LINE2_TXT VARCHAR(16777216) COMMENT 'Supplementary line of mailing address',
	CITY VARCHAR(16777216) COMMENT 'City',
	STATE_CODE VARCHAR(16777216) COMMENT 'State code',
	COUNTRY_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	ZIP_CODE VARCHAR(16777216) COMMENT 'Postal code',
	STORE_LOCATION_ID VARCHAR(16777216) COMMENT 'Store location number',
	STORE_ZIP_CODE VARCHAR(16777216) COMMENT 'Store location postal code',
	PROCESS_STATUS VARCHAR(16777216) NOT NULL,
	PROCESS_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL
);
create TABLE IF NOT EXISTS TEST_DEV_LOYALTY_EPSILON_RESPONSE_DEMO_APPEND (
	SRC_JOB_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcRecordId, this provides a unique identifier that links data back to the original data. This is provided on input from the user.',
	SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcJobId, this provides a unique identifier that links data back to the original data.  This is provided on input from the user.',
	PROCESS_DATE VARCHAR(16777216) COMMENT 'Date the file was processed',
	CLIENT_NAMED_CORE_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreId; Individual ID; In order to accurately match to a NamedCoreId/IndividualID, records must have at a minimum: Name & Address (address only records will not be assigned a named CORE/individual id) OR Email address OR Phone number',
	CLIENT_NAMED_HH_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreHHId',
	IDENTITY_MATCH_CONFIDENCE VARCHAR(16777216) COMMENT 'Indicates match location and confidence of match to known individuals',
	PREFIX_NAME VARCHAR(16777216) COMMENT 'Name prefix',
	GIVEN_NAME VARCHAR(16777216) COMMENT 'First (given) name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Middle name',
	FAMILY_NAME VARCHAR(16777216) COMMENT 'Last (family) name',
	GENERATIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Generational suffix',
	PROFESSIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Professional Suffix',
	GENDER VARCHAR(16777216) COMMENT 'Gender',
	COMPANY_NAME VARCHAR(16777216) COMMENT 'Company or Business name',
	PROFANITY VARCHAR(16777216) COMMENT 'Profanity identified in the name',
	GIVEN_FAMILY_NAME_MATCH VARCHAR(16777216) COMMENT 'First name and last name match',
	POSSIBLE_BUSINESS VARCHAR(16777216) COMMENT 'The name on the record is a possible business name',
	LACS VARCHAR(16777216) COMMENT 'Locatable Address Conversion System indicator',
	LACS_NEW_ADDR_FLAG VARCHAR(16777216) COMMENT 'The type of LACS address',
	LACS_RETURN_CODE VARCHAR(16777216) COMMENT 'LACS footnote to indicates the validation of the address as LACS certified ',
	NCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective NCOA move date',
	NCOA_MOVE_TYPE VARCHAR(16777216) COMMENT 'Type of NCOA move',
	NCOA_LINK_FOOTNOTE VARCHAR(16777216) COMMENT 'NCOA Link return codes',
	PCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective PCOA move date',
	PAC_ACTION_CODE VARCHAR(16777216) COMMENT 'PAC action code',
	PAC_FOOTNOTE VARCHAR(16777216) COMMENT 'PAC footnote',
	ADDRESS_SOURCE VARCHAR(16777216) COMMENT 'The source of the best address',
	CURBSIDE_ID VARCHAR(16777216) COMMENT 'Best value for curbside ID (addressLine1)',
	ADDRESS_LINE1 VARCHAR(16777216) COMMENT 'Standardized address line 1',
	ADDRESS_LINE2 VARCHAR(16777216) COMMENT 'Standardized address line 2',
	ADDRESS_LINE3 VARCHAR(16777216) COMMENT 'Standardized address line 3',
	ADDRESS_LINE4 VARCHAR(16777216) COMMENT 'Standardized address line 4',
	LOCALITY1 VARCHAR(16777216) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb).',
	LOCALITY2 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	LOCALITY3 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	REGION1 VARCHAR(16777216) COMMENT 'State, province, territory, or region.',
	REGION2 VARCHAR(16777216) COMMENT 'Additional state, province, territory, or region.',
	POSTAL_CODE VARCHAR(16777216) COMMENT 'Standarized postal code',
	ZIP4 VARCHAR(16777216) COMMENT 'ZIP+4',
	SEC_RANGE VARCHAR(16777216) COMMENT 'Secondary range used for matching',
	DPBC VARCHAR(16777216) COMMENT 'The two-digit DPBC code.',
	CHECK_DIGIT VARCHAR(16777216) COMMENT 'Check digit for the delivery-point bar code, or for a five-digit bar code if a full postal code (ZIP+4) could not be assigned.',
	ISO_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	CART VARCHAR(16777216) COMMENT 'Carrier-route number',
	LOT VARCHAR(16777216) COMMENT 'Line-of-travel number',
	LOT_ORDER VARCHAR(16777216) COMMENT 'Line-of-travel sortation',
	REC_TYPE VARCHAR(16777216) COMMENT 'The record-type indicator for the assigned address',
	REMAINDER VARCHAR(16777216) COMMENT 'Extraneous data found on the address line, which either can’t be identified by the parser or does not belong in a standardized address',
	DPV_CMRA VARCHAR(16777216) COMMENT 'Commercial Mail Receiving Agency (CMRA) indicator ',
	DPV_FTNOTE VARCHAR(16777216) COMMENT 'DPV footnotes',
	DPV_FALSE_POS_IND VARCHAR(16777216) COMMENT 'DPV false-positive and triggered DPV',
	DPV_NO_STATS VARCHAR(16777216) COMMENT 'No Stat indicator. No Stat means that the address is a vacant property, it receives mail as a part of a drop, or it does not have an established delivery yet.',
	DPV_STATUS VARCHAR(16777216) COMMENT 'The DPV status component that is generated for this record.',
	FOREIGN_CODE VARCHAR(16777216) COMMENT 'Foreign address code',
	MATCH5 VARCHAR(16777216) COMMENT 'Lastline match level indicator',
	MATCH9 VARCHAR(16777216) COMMENT 'Addressline match level indicator',
	MATCH_UN VARCHAR(16777216) COMMENT 'Indicates whether the record is a deliverable address',
	ZIP_MOVE VARCHAR(16777216) COMMENT 'ZIP move indicator',
	ZIP_TYPE VARCHAR(16777216) COMMENT 'Type of ZIP Code assigned',
	CONGRESS VARCHAR(16777216) COMMENT 'District number for the U.S. House of Representatives',
	COUNTY VARCHAR(16777216) COMMENT 'Federal Information Processing Standard (FIPS) county code',
	COUNTY_NAME VARCHAR(16777216) COMMENT 'The fully-spelled county name',
	FAC_TYPE VARCHAR(16777216) COMMENT 'Type of postal facility',
	FIPS_CODE VARCHAR(16777216) COMMENT 'FIPS code',
	ERROR_CODE VARCHAR(16777216) COMMENT 'Address Error Codes',
	STAT_CODE VARCHAR(16777216) COMMENT 'Address Status Code',
	GEO_MATCH VARCHAR(16777216) COMMENT 'Match code indicating the precision of the latitude and longitude assignment',
	GEO_LAT VARCHAR(16777216) COMMENT 'Latitude (degrees north of the equator) in the format 12.123456',
	GEO_LNG VARCHAR(16777216) COMMENT 'Longitude (degrees west of the Greenwich Meridian) in the format -12.123456',
	AGEO_PLA VARCHAR(16777216) COMMENT 'FIPS place code. A number assigned by the U.S. government to each incorporated municipality (city, village, town, etc.)',
	GEO_BLK VARCHAR(16777216) COMMENT 'GEO block',
	AGEO_MCD VARCHAR(16777216) COMMENT 'U.S. Census Bureau minor civil division (MCD) data or, if MCD data is unavailable, census county division (CCD) data',
	CGEO_CBSA VARCHAR(16777216) COMMENT 'A Core-Based Statistical Area (CBSA) consists of:\n- A county with an incorporated place or a census-designated place that has a population of at least 10,000\n- Adjacent counties with at least 25 percent of employed residents of the county who work in the CBSA’s core or central county',
	CGEO_MSA VARCHAR(16777216) COMMENT 'Metropolitan Statistical Area (MSA) number. 0000 indicates the address does not lie in any MSA; usually a rural area',
	AGEO_STA VARCHAR(16777216) COMMENT 'FIPS state code',
	DSF_DROP_FLAG VARCHAR(16777216) COMMENT 'Drop indicator',
	DSF_THROWBACK_FLAG VARCHAR(16777216) COMMENT 'Throwback indicator',
	DSF_SEASONAL_FLAG VARCHAR(16777216) COMMENT 'Seasonal address indicator',
	DSF_VACANT_FLAG VARCHAR(16777216) COMMENT 'Vacant address indicator',
	DSF_DELIVERY_TYPE VARCHAR(16777216) COMMENT 'Delivery type',
	DSF_DROP_COUNT VARCHAR(16777216) COMMENT 'Drop count indicates the number of businesses or families served by this delivery point',
	DSF_LACS_FLAG VARCHAR(16777216) COMMENT 'LACS (Locatable Address Conversion System) indicator',
	DSF_EDUCATIONAL_FLAG VARCHAR(16777216) COMMENT 'Indicates that the address is seasonal and related to an educational institution',
	DSF_REC_TYPE VARCHAR(16777216) COMMENT 'DSF Record type',
	MAILABILITY_SCORE VARCHAR(16777216) COMMENT 'Mailability index',
	RDI_FLAG VARCHAR(16777216) COMMENT 'Best Residential Delivery Indicator',
	LINE1 VARCHAR(16777216) COMMENT 'Mail ready address block line 1',
	LINE2 VARCHAR(16777216) COMMENT 'Mail ready address block line 2',
	LINE3 VARCHAR(16777216) COMMENT 'Mail ready address block line 3',
	LINE4 VARCHAR(16777216) COMMENT 'Mail ready address block line 4',
	LINE5 VARCHAR(16777216) COMMENT 'Mail ready address block line 5',
	LINE6 VARCHAR(16777216) COMMENT 'Mail ready address block line 6',
	LINE7 VARCHAR(16777216) COMMENT 'Mail ready address block line 7',
	LINE8 VARCHAR(16777216) COMMENT 'Mail ready address block line 8',
	LINE9 VARCHAR(16777216) COMMENT 'Mail ready address block line 9',
	LINE10 VARCHAR(16777216) COMMENT 'Mail ready address block line 10',
	OCCUPANCY_SCORE VARCHAR(16777216) COMMENT 'Occupancy score - postal evidence within that timeframe that the person is/was at the input address. The lower the number the more recent the occupancy is verified',
	MULTI_TYPE VARCHAR(16777216) COMMENT 'Dwelling type',
	DECEASED VARCHAR(16777216) COMMENT 'Deceased Suppression Flag',
	DECEASED_DOB VARCHAR(16777216) COMMENT 'Date of Birth on Deceased',
	DECEASED_DOD VARCHAR(16777216) COMMENT 'Date of Death',
	DO_NOT_MAIL VARCHAR(16777216) COMMENT 'Do Not Mail (DNM) Suppression Flag',
	DO_NOT_CALL VARCHAR(16777216) COMMENT 'Do Not Call (DNC) Suppression Flag',
	DO_NOT_FAX VARCHAR(16777216) COMMENT 'Do Not Fax (DNF) Suppression Flag',
	PRISON VARCHAR(16777216) COMMENT 'Prison Address Suppression Flag',
	NURSING_HOME VARCHAR(16777216) COMMENT 'Nursing Home Address Suppression Flag',
	PHONE1 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE1 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE2 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE2 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE3 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE3 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	RETURN_CODE_NAME_HYGIENE VARCHAR(16777216),
	RETURN_CODE_ADDRESS VARCHAR(16777216),
	RETURN_CODE_IDENTITY VARCHAR(16777216),
	CC_MATCH_SOURCE VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'Load  Identifier specifies the Batch that inserted the record into the table. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_ID NUMBER(38,0) NOT NULL COMMENT 'Update Identifier specifies the Batch that updated data. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	LOAD_FILENAME VARCHAR(16777216) NOT NULL COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1DEV_LOYALTY_EPSILON_RESPONSE_IDENTITY unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS TEST_DEV_LOYALTY_EPSILON_RESPONSE_IDENTITY (
	SRC_JOB_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcRecordId, this provides a unique identifier that links data back to the original data. This is provided on input from the user.',
	SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcJobId, this provides a unique identifier that links data back to the original data.  This is provided on input from the user.',
	PROCESS_DATE VARCHAR(16777216) COMMENT 'Date the file was processed',
	CLIENT_NAMED_CORE_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreId; Individual ID; In order to accurately match to a NamedCoreId/IndividualID, records must have at a minimum: Name & Address (address only records will not be assigned a named CORE/individual id) OR Email address OR Phone number',
	CLIENT_NAMED_HH_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreHHId',
	IDENTITY_MATCH_CONFIDENCE VARCHAR(16777216) COMMENT 'Indicates match location and confidence of match to known individuals',
	PREFIX_NAME VARCHAR(16777216) COMMENT 'Name prefix',
	GIVEN_NAME VARCHAR(16777216) COMMENT 'First (given) name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Middle name',
	FAMILY_NAME VARCHAR(16777216) COMMENT 'Last (family) name',
	GENERATIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Generational suffix',
	PROFESSIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Professional Suffix',
	GENDER VARCHAR(16777216) COMMENT 'Gender',
	COMPANY_NAME VARCHAR(16777216) COMMENT 'Company or Business name',
	PROFANITY VARCHAR(16777216) COMMENT 'Profanity identified in the name',
	GIVEN_FAMILY_NAME_MATCH VARCHAR(16777216) COMMENT 'First name and last name match',
	POSSIBLE_BUSINESS VARCHAR(16777216) COMMENT 'The name on the record is a possible business name',
	LACS VARCHAR(16777216) COMMENT 'Locatable Address Conversion System indicator',
	LACS_NEW_ADDR_FLAG VARCHAR(16777216) COMMENT 'The type of LACS address',
	LACS_RETURN_CODE VARCHAR(16777216) COMMENT 'LACS footnote to indicates the validation of the address as LACS certified ',
	NCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective NCOA move date',
	NCOA_MOVE_TYPE VARCHAR(16777216) COMMENT 'Type of NCOA move',
	NCOA_LINK_FOOTNOTE VARCHAR(16777216) COMMENT 'NCOA Link return codes',
	PCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective PCOA move date',
	PAC_ACTION_CODE VARCHAR(16777216) COMMENT 'PAC action code',
	PAC_FOOTNOTE VARCHAR(16777216) COMMENT 'PAC footnote',
	ADDRESS_SOURCE VARCHAR(16777216) COMMENT 'The source of the best address',
	CURBSIDE_ID VARCHAR(16777216) COMMENT 'Best value for curbside ID (addressLine1)',
	ADDRESS_LINE1 VARCHAR(16777216) COMMENT 'Standardized address line 1',
	ADDRESS_LINE2 VARCHAR(16777216) COMMENT 'Standardized address line 2',
	ADDRESS_LINE3 VARCHAR(16777216) COMMENT 'Standardized address line 3',
	ADDRESS_LINE4 VARCHAR(16777216) COMMENT 'Standardized address line 4',
	LOCALITY1 VARCHAR(16777216) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb).',
	LOCALITY2 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	LOCALITY3 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	REGION1 VARCHAR(16777216) COMMENT 'State, province, territory, or region.',
	REGION2 VARCHAR(16777216) COMMENT 'Additional state, province, territory, or region.',
	POSTAL_CODE VARCHAR(16777216) COMMENT 'Standarized postal code',
	ZIP4 VARCHAR(16777216) COMMENT 'ZIP+4',
	SEC_RANGE VARCHAR(16777216) COMMENT 'Secondary range used for matching',
	DPBC VARCHAR(16777216) COMMENT 'The two-digit DPBC code.',
	CHECK_DIGIT VARCHAR(16777216) COMMENT 'Check digit for the delivery-point bar code, or for a five-digit bar code if a full postal code (ZIP+4) could not be assigned.',
	ISO_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	CART VARCHAR(16777216) COMMENT 'Carrier-route number',
	LOT VARCHAR(16777216) COMMENT 'Line-of-travel number',
	LOT_ORDER VARCHAR(16777216) COMMENT 'Line-of-travel sortation',
	REC_TYPE VARCHAR(16777216) COMMENT 'The record-type indicator for the assigned address',
	REMAINDER VARCHAR(16777216) COMMENT 'Extraneous data found on the address line, which either can’t be identified by the parser or does not belong in a standardized address',
	DPV_CMRA VARCHAR(16777216) COMMENT 'Commercial Mail Receiving Agency (CMRA) indicator ',
	DPV_FTNOTE VARCHAR(16777216) COMMENT 'DPV footnotes',
	DPV_FALSE_POS_IND VARCHAR(16777216) COMMENT 'DPV false-positive and triggered DPV',
	DPV_NO_STATS VARCHAR(16777216) COMMENT 'No Stat indicator. No Stat means that the address is a vacant property, it receives mail as a part of a drop, or it does not have an established delivery yet.',
	DPV_STATUS VARCHAR(16777216) COMMENT 'The DPV status component that is generated for this record.',
	FOREIGN_CODE VARCHAR(16777216) COMMENT 'Foreign address code',
	MATCH5 VARCHAR(16777216) COMMENT 'Lastline match level indicator',
	MATCH9 VARCHAR(16777216) COMMENT 'Addressline match level indicator',
	MATCH_UN VARCHAR(16777216) COMMENT 'Indicates whether the record is a deliverable address',
	ZIP_MOVE VARCHAR(16777216) COMMENT 'ZIP move indicator',
	ZIP_TYPE VARCHAR(16777216) COMMENT 'Type of ZIP Code assigned',
	CONGRESS VARCHAR(16777216) COMMENT 'District number for the U.S. House of Representatives',
	COUNTY VARCHAR(16777216) COMMENT 'Federal Information Processing Standard (FIPS) county code',
	COUNTY_NAME VARCHAR(16777216) COMMENT 'The fully-spelled county name',
	FAC_TYPE VARCHAR(16777216) COMMENT 'Type of postal facility',
	FIPS_CODE VARCHAR(16777216) COMMENT 'FIPS code',
	ERROR_CODE VARCHAR(16777216) COMMENT 'Address Error Codes',
	STAT_CODE VARCHAR(16777216) COMMENT 'Address Status Code',
	GEO_MATCH VARCHAR(16777216) COMMENT 'Match code indicating the precision of the latitude and longitude assignment',
	GEO_LAT VARCHAR(16777216) COMMENT 'Latitude (degrees north of the equator) in the format 12.123456',
	GEO_LNG VARCHAR(16777216) COMMENT 'Longitude (degrees west of the Greenwich Meridian) in the format -12.123456',
	AGEO_PLA VARCHAR(16777216) COMMENT 'FIPS place code. A number assigned by the U.S. government to each incorporated municipality (city, village, town, etc.)',
	GEO_BLK VARCHAR(16777216) COMMENT 'GEO block',
	AGEO_MCD VARCHAR(16777216) COMMENT 'U.S. Census Bureau minor civil division (MCD) data or, if MCD data is unavailable, census county division (CCD) data',
	CGEO_CBSA VARCHAR(16777216) COMMENT 'A Core-Based Statistical Area (CBSA) consists of:\n- A county with an incorporated place or a census-designated place that has a population of at least 10,000\n- Adjacent counties with at least 25 percent of employed residents of the county who work in the CBSA’s core or central county',
	CGEO_MSA VARCHAR(16777216) COMMENT 'Metropolitan Statistical Area (MSA) number. 0000 indicates the address does not lie in any MSA; usually a rural area',
	AGEO_STA VARCHAR(16777216) COMMENT 'FIPS state code',
	DSF_DROP_FLAG VARCHAR(16777216) COMMENT 'Drop indicator',
	DSF_THROWBACK_FLAG VARCHAR(16777216) COMMENT 'Throwback indicator',
	DSF_SEASONAL_FLAG VARCHAR(16777216) COMMENT 'Seasonal address indicator',
	DSF_VACANT_FLAG VARCHAR(16777216) COMMENT 'Vacant address indicator',
	DSF_DELIVERY_TYPE VARCHAR(16777216) COMMENT 'Delivery type',
	DSF_DROP_COUNT VARCHAR(16777216) COMMENT 'Drop count indicates the number of businesses or families served by this delivery point',
	DSF_LACS_FLAG VARCHAR(16777216) COMMENT 'LACS (Locatable Address Conversion System) indicator',
	DSF_EDUCATIONAL_FLAG VARCHAR(16777216) COMMENT 'Indicates that the address is seasonal and related to an educational institution',
	DSF_REC_TYPE VARCHAR(16777216) COMMENT 'DSF Record type',
	MAILABILITY_SCORE VARCHAR(16777216) COMMENT 'Mailability index',
	RDI_FLAG VARCHAR(16777216) COMMENT 'Best Residential Delivery Indicator',
	LINE1 VARCHAR(16777216) COMMENT 'Mail ready address block line 1',
	LINE2 VARCHAR(16777216) COMMENT 'Mail ready address block line 2',
	LINE3 VARCHAR(16777216) COMMENT 'Mail ready address block line 3',
	LINE4 VARCHAR(16777216) COMMENT 'Mail ready address block line 4',
	LINE5 VARCHAR(16777216) COMMENT 'Mail ready address block line 5',
	LINE6 VARCHAR(16777216) COMMENT 'Mail ready address block line 6',
	LINE7 VARCHAR(16777216) COMMENT 'Mail ready address block line 7',
	LINE8 VARCHAR(16777216) COMMENT 'Mail ready address block line 8',
	LINE9 VARCHAR(16777216) COMMENT 'Mail ready address block line 9',
	LINE10 VARCHAR(16777216) COMMENT 'Mail ready address block line 10',
	OCCUPANCY_SCORE VARCHAR(16777216) COMMENT 'Occupancy score - postal evidence within that timeframe that the person is/was at the input address. The lower the number the more recent the occupancy is verified',
	MULTI_TYPE VARCHAR(16777216) COMMENT 'Dwelling type',
	DECEASED VARCHAR(16777216) COMMENT 'Deceased Suppression Flag',
	DECEASED_DOB VARCHAR(16777216) COMMENT 'Date of Birth on Deceased',
	DECEASED_DOD VARCHAR(16777216) COMMENT 'Date of Death',
	DO_NOT_MAIL VARCHAR(16777216) COMMENT 'Do Not Mail (DNM) Suppression Flag',
	DO_NOT_CALL VARCHAR(16777216) COMMENT 'Do Not Call (DNC) Suppression Flag',
	DO_NOT_FAX VARCHAR(16777216) COMMENT 'Do Not Fax (DNF) Suppression Flag',
	PRISON VARCHAR(16777216) COMMENT 'Prison Address Suppression Flag',
	NURSING_HOME VARCHAR(16777216) COMMENT 'Nursing Home Address Suppression Flag',
	PHONE1 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE1 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE2 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE2 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE3 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE3 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	RETURN_CODE_NAME_HYGIENE VARCHAR(16777216),
	RETURN_CODE_ADDRESS VARCHAR(16777216),
	RETURN_CODE_IDENTITY VARCHAR(16777216),
	CC_MATCH_SOURCE VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'Load  Identifier specifies the Batch that inserted the record into the table. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_ID NUMBER(38,0) NOT NULL COMMENT 'Update Identifier specifies the Batch that updated data. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	LOAD_FILENAME VARCHAR(16777216) NOT NULL COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1DEV_LOYALTY_EPSILON_RESPONSE_IDENTITY unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS TEST_EPSILON (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS TEST_EXTRACT (
	SRC_JOB_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcRecordId, this provides a unique identifier that links data back to the original data. This is provided on input from the user.',
	SRC_RECORD_ID VARCHAR(16777216) NOT NULL COMMENT 'Combined with the srcJobId, this provides a unique identifier that links data back to the original data.  This is provided on input from the user.',
	PROCESS_DATE VARCHAR(16777216) COMMENT 'Date the file was processed',
	CLIENT_NAMED_CORE_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreId; Individual ID; In order to accurately match to a NamedCoreId/IndividualID, records must have at a minimum: Name & Address (address only records will not be assigned a named CORE/individual id) OR Email address OR Phone number',
	CLIENT_NAMED_HH_ID VARCHAR(16777216) COMMENT 'Client protected epsilonNamedCoreHHId',
	IDENTITY_MATCH_CONFIDENCE VARCHAR(16777216) COMMENT 'Indicates match location and confidence of match to known individuals',
	PREFIX_NAME VARCHAR(16777216) COMMENT 'Name prefix',
	GIVEN_NAME VARCHAR(16777216) COMMENT 'First (given) name',
	MIDDLE_NAME VARCHAR(16777216) COMMENT 'Middle name',
	FAMILY_NAME VARCHAR(16777216) COMMENT 'Last (family) name',
	GENERATIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Generational suffix',
	PROFESSIONAL_SUFFIX VARCHAR(16777216) COMMENT 'Professional Suffix',
	GENDER VARCHAR(16777216) COMMENT 'Gender',
	COMPANY_NAME VARCHAR(16777216) COMMENT 'Company or Business name',
	PROFANITY VARCHAR(16777216) COMMENT 'Profanity identified in the name',
	GIVEN_FAMILY_NAME_MATCH VARCHAR(16777216) COMMENT 'First name and last name match',
	POSSIBLE_BUSINESS VARCHAR(16777216) COMMENT 'The name on the record is a possible business name',
	LACS VARCHAR(16777216) COMMENT 'Locatable Address Conversion System indicator',
	LACS_NEW_ADDR_FLAG VARCHAR(16777216) COMMENT 'The type of LACS address',
	LACS_RETURN_CODE VARCHAR(16777216) COMMENT 'LACS footnote to indicates the validation of the address as LACS certified ',
	NCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective NCOA move date',
	NCOA_MOVE_TYPE VARCHAR(16777216) COMMENT 'Type of NCOA move',
	NCOA_LINK_FOOTNOTE VARCHAR(16777216) COMMENT 'NCOA Link return codes',
	PCOA_MOVE_DATE VARCHAR(16777216) COMMENT 'Effective PCOA move date',
	PAC_ACTION_CODE VARCHAR(16777216) COMMENT 'PAC action code',
	PAC_FOOTNOTE VARCHAR(16777216) COMMENT 'PAC footnote',
	ADDRESS_SOURCE VARCHAR(16777216) COMMENT 'The source of the best address',
	CURBSIDE_ID VARCHAR(16777216) COMMENT 'Best value for curbside ID (addressLine1)',
	ADDRESS_LINE1 VARCHAR(16777216) COMMENT 'Standardized address line 1',
	ADDRESS_LINE2 VARCHAR(16777216) COMMENT 'Standardized address line 2',
	ADDRESS_LINE3 VARCHAR(16777216) COMMENT 'Standardized address line 3',
	ADDRESS_LINE4 VARCHAR(16777216) COMMENT 'Standardized address line 4',
	LOCALITY1 VARCHAR(16777216) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb).',
	LOCALITY2 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	LOCALITY3 VARCHAR(16777216) COMMENT 'Additional city, town, locality, or suburb information.',
	REGION1 VARCHAR(16777216) COMMENT 'State, province, territory, or region.',
	REGION2 VARCHAR(16777216) COMMENT 'Additional state, province, territory, or region.',
	POSTAL_CODE VARCHAR(16777216) COMMENT 'Standarized postal code',
	ZIP4 VARCHAR(16777216) COMMENT 'ZIP+4',
	SEC_RANGE VARCHAR(16777216) COMMENT 'Secondary range used for matching',
	DPBC VARCHAR(16777216) COMMENT 'The two-digit DPBC code.',
	CHECK_DIGIT VARCHAR(16777216) COMMENT 'Check digit for the delivery-point bar code, or for a five-digit bar code if a full postal code (ZIP+4) could not be assigned.',
	ISO_CODE VARCHAR(16777216) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	CART VARCHAR(16777216) COMMENT 'Carrier-route number',
	LOT VARCHAR(16777216) COMMENT 'Line-of-travel number',
	LOT_ORDER VARCHAR(16777216) COMMENT 'Line-of-travel sortation',
	REC_TYPE VARCHAR(16777216) COMMENT 'The record-type indicator for the assigned address',
	REMAINDER VARCHAR(16777216) COMMENT 'Extraneous data found on the address line, which either can’t be identified by the parser or does not belong in a standardized address',
	DPV_CMRA VARCHAR(16777216) COMMENT 'Commercial Mail Receiving Agency (CMRA) indicator ',
	DPV_FTNOTE VARCHAR(16777216) COMMENT 'DPV footnotes',
	DPV_FALSE_POS_IND VARCHAR(16777216) COMMENT 'DPV false-positive and triggered DPV',
	DPV_NO_STATS VARCHAR(16777216) COMMENT 'No Stat indicator. No Stat means that the address is a vacant property, it receives mail as a part of a drop, or it does not have an established delivery yet.',
	DPV_STATUS VARCHAR(16777216) COMMENT 'The DPV status component that is generated for this record.',
	FOREIGN_CODE VARCHAR(16777216) COMMENT 'Foreign address code',
	MATCH5 VARCHAR(16777216) COMMENT 'Lastline match level indicator',
	MATCH9 VARCHAR(16777216) COMMENT 'Addressline match level indicator',
	MATCH_UN VARCHAR(16777216) COMMENT 'Indicates whether the record is a deliverable address',
	ZIP_MOVE VARCHAR(16777216) COMMENT 'ZIP move indicator',
	ZIP_TYPE VARCHAR(16777216) COMMENT 'Type of ZIP Code assigned',
	CONGRESS VARCHAR(16777216) COMMENT 'District number for the U.S. House of Representatives',
	COUNTY VARCHAR(16777216) COMMENT 'Federal Information Processing Standard (FIPS) county code',
	COUNTY_NAME VARCHAR(16777216) COMMENT 'The fully-spelled county name',
	FAC_TYPE VARCHAR(16777216) COMMENT 'Type of postal facility',
	FIPS_CODE VARCHAR(16777216) COMMENT 'FIPS code',
	ERROR_CODE VARCHAR(16777216) COMMENT 'Address Error Codes',
	STAT_CODE VARCHAR(16777216) COMMENT 'Address Status Code',
	GEO_MATCH VARCHAR(16777216) COMMENT 'Match code indicating the precision of the latitude and longitude assignment',
	GEO_LAT VARCHAR(16777216) COMMENT 'Latitude (degrees north of the equator) in the format 12.123456',
	GEO_LNG VARCHAR(16777216) COMMENT 'Longitude (degrees west of the Greenwich Meridian) in the format -12.123456',
	AGEO_PLA VARCHAR(16777216) COMMENT 'FIPS place code. A number assigned by the U.S. government to each incorporated municipality (city, village, town, etc.)',
	GEO_BLK VARCHAR(16777216) COMMENT 'GEO block',
	AGEO_MCD VARCHAR(16777216) COMMENT 'U.S. Census Bureau minor civil division (MCD) data or, if MCD data is unavailable, census county division (CCD) data',
	CGEO_CBSA VARCHAR(16777216) COMMENT 'A Core-Based Statistical Area (CBSA) consists of:\n- A county with an incorporated place or a census-designated place that has a population of at least 10,000\n- Adjacent counties with at least 25 percent of employed residents of the county who work in the CBSA’s core or central county',
	CGEO_MSA VARCHAR(16777216) COMMENT 'Metropolitan Statistical Area (MSA) number. 0000 indicates the address does not lie in any MSA; usually a rural area',
	AGEO_STA VARCHAR(16777216) COMMENT 'FIPS state code',
	DSF_DROP_FLAG VARCHAR(16777216) COMMENT 'Drop indicator',
	DSF_THROWBACK_FLAG VARCHAR(16777216) COMMENT 'Throwback indicator',
	DSF_SEASONAL_FLAG VARCHAR(16777216) COMMENT 'Seasonal address indicator',
	DSF_VACANT_FLAG VARCHAR(16777216) COMMENT 'Vacant address indicator',
	DSF_DELIVERY_TYPE VARCHAR(16777216) COMMENT 'Delivery type',
	DSF_DROP_COUNT VARCHAR(16777216) COMMENT 'Drop count indicates the number of businesses or families served by this delivery point',
	DSF_LACS_FLAG VARCHAR(16777216) COMMENT 'LACS (Locatable Address Conversion System) indicator',
	DSF_EDUCATIONAL_FLAG VARCHAR(16777216) COMMENT 'Indicates that the address is seasonal and related to an educational institution',
	DSF_REC_TYPE VARCHAR(16777216) COMMENT 'DSF Record type',
	MAILABILITY_SCORE VARCHAR(16777216) COMMENT 'Mailability index',
	RDI_FLAG VARCHAR(16777216) COMMENT 'Best Residential Delivery Indicator',
	LINE1 VARCHAR(16777216) COMMENT 'Mail ready address block line 1',
	LINE2 VARCHAR(16777216) COMMENT 'Mail ready address block line 2',
	LINE3 VARCHAR(16777216) COMMENT 'Mail ready address block line 3',
	LINE4 VARCHAR(16777216) COMMENT 'Mail ready address block line 4',
	LINE5 VARCHAR(16777216) COMMENT 'Mail ready address block line 5',
	LINE6 VARCHAR(16777216) COMMENT 'Mail ready address block line 6',
	LINE7 VARCHAR(16777216) COMMENT 'Mail ready address block line 7',
	LINE8 VARCHAR(16777216) COMMENT 'Mail ready address block line 8',
	LINE9 VARCHAR(16777216) COMMENT 'Mail ready address block line 9',
	LINE10 VARCHAR(16777216) COMMENT 'Mail ready address block line 10',
	OCCUPANCY_SCORE VARCHAR(16777216) COMMENT 'Occupancy score - postal evidence within that timeframe that the person is/was at the input address. The lower the number the more recent the occupancy is verified',
	MULTI_TYPE VARCHAR(16777216) COMMENT 'Dwelling type',
	DECEASED VARCHAR(16777216) COMMENT 'Deceased Suppression Flag',
	DECEASED_DOB VARCHAR(16777216) COMMENT 'Date of Birth on Deceased',
	DECEASED_DOD VARCHAR(16777216) COMMENT 'Date of Death',
	DO_NOT_MAIL VARCHAR(16777216) COMMENT 'Do Not Mail (DNM) Suppression Flag',
	DO_NOT_CALL VARCHAR(16777216) COMMENT 'Do Not Call (DNC) Suppression Flag',
	DO_NOT_FAX VARCHAR(16777216) COMMENT 'Do Not Fax (DNF) Suppression Flag',
	PRISON VARCHAR(16777216) COMMENT 'Prison Address Suppression Flag',
	NURSING_HOME VARCHAR(16777216) COMMENT 'Nursing Home Address Suppression Flag',
	PHONE1 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE1 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE2 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE2 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	PHONE3 VARCHAR(16777216) COMMENT 'Cleansed result for phone number',
	RETURN_CODE_PHONE3 VARCHAR(16777216) COMMENT 'Return code from phone cleanse',
	EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS1 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS2 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Cleansed result for the email address',
	RETURN_CODE_EMAIL_ADDRESS3 VARCHAR(16777216) COMMENT 'Return code from email cleanse',
	RETURN_CODE_NAME_HYGIENE VARCHAR(16777216),
	RETURN_CODE_ADDRESS VARCHAR(16777216),
	RETURN_CODE_IDENTITY VARCHAR(16777216),
	CC_MATCH_SOURCE VARCHAR(16777216),
	BRAND_ID VARCHAR(16777216) NOT NULL COMMENT 'Brand Identifier specifies the code which represents an organization. The identifier should be in all capital letters. Sample codes are irb (Inspire Recognized Brands), bww(Buffalo Wild Wing), arbys (Arbys),  dnkn  (Dunkin), etc.',
	SOURCE_SYSTEM_NAME VARCHAR(16777216) NOT NULL COMMENT 'Source System Name specifies the origin of the data. For example, Salesforce Marketing App or Netsuite. Some systems will have a reference table where sources are identified by integers such as 1,2,3, etc.',
	LOAD_ID NUMBER(38,0) NOT NULL COMMENT 'Load  Identifier specifies the Batch that inserted the record into the table. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_ID NUMBER(38,0) NOT NULL COMMENT 'Update Identifier specifies the Batch that updated data. As an interim, until a ETL Process framework is in place, the value of the load_id YYYYMMDD.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	LOAD_FILENAME VARCHAR(16777216) NOT NULL COMMENT 'Load Filename is the name of the file that was used to create the data.',
	constraint XAK1DEV_LOYALTY_EPSILON_RESPONSE_IDENTITY unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS TEST_FINAL (
	SRC_JOB_ID VARCHAR(255),
	SRC_RECORD_ID VARCHAR(255),
	FULL_NAME VARCHAR(255),
	PREFIX_NAME VARCHAR(255),
	GIVEN_NAME VARCHAR(255),
	MIDDLE_NAME VARCHAR(255),
	FAMILY_NAME VARCHAR(255),
	GENERATIONAL_SUFFIX VARCHAR(255),
	GENDER VARCHAR(255),
	COMPANY_NAME VARCHAR(255),
	ADDRESS_LINE1 VARCHAR(255),
	ADDRESS_LINE2 VARCHAR(255),
	ADDRESS_LINE3 VARCHAR(255),
	ADDRESS_LINE4 VARCHAR(255),
	LOCALITY1 VARCHAR(255),
	LOCALITY2 VARCHAR(255),
	LOCALITY3 VARCHAR(255),
	REGION1 VARCHAR(255),
	REGION2 VARCHAR(255),
	POSTAL_CODE VARCHAR(255),
	COUNTRY_CODE VARCHAR(255),
	EMAIL_ADDRESS1 VARCHAR(255),
	EMAIL_ADDRESS2 VARCHAR(255),
	EMAIL_ADDRESS3 VARCHAR(255),
	PHONE1 VARCHAR(255),
	PHONE2 VARCHAR(255),
	PHONE3 VARCHAR(255),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS TEST_SEND (
	VALUE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TEST_SONIC (
	BRAND_ID VARCHAR(16777216),
	SOURCE_SYSTEM_NAME VARCHAR(16777216),
	IDP_BRAND_CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_ID VARCHAR(16777216),
	CIP_SRC_RECORD_ID VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	MIDDLE_NAME VARCHAR(16777216),
	EMAIL_ADDRESS VARCHAR(16777216),
	PHONE_NBR VARCHAR(16777216),
	ADDRESS_LINE1_TXT VARCHAR(16777216),
	ADDRESS_LINE2_TXT VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE_CODE VARCHAR(16777216),
	COUNTRY_CODE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	PROCESS_STATUS VARCHAR(16777216),
	PROCESS_DTTM TIMESTAMP_NTZ(9),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9),
	LOAD_TYPE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TEST_TAGS_TABLE (
	FIRST VARCHAR(16777216),
	SECOND VARCHAR(16777216),
	THIRD VARCHAR(16777216)
);
create TABLE IF NOT EXISTS UC1_SEND_TO_EPSILON (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'User-defined job ID; field is required; part of source system unique key',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; field is required; part of source system unique key',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	BATCH_ID VARCHAR(16777216) NOT NULL,
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL
);
create TABLE IF NOT EXISTS UC2_SEND_TO_EPSILON (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS UC2_SEND_TO_EPSILON_HIST_CATCH_UP_1 (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS UC2_SEND_TO_EPSILON_HIST_LOAD (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS UC2_SEND_TO_EPSILON_HIST_LOAD_250_000 (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS UC2_SEND_TO_EPSILON_HIST_LOAD_BACKUP (
	SRC_JOB_ID VARCHAR(255),
	SRC_RECORD_ID VARCHAR(255),
	FULL_NAME VARCHAR(255),
	PREFIX_NAME VARCHAR(255),
	GIVEN_NAME VARCHAR(255),
	MIDDLE_NAME VARCHAR(255),
	FAMILY_NAME VARCHAR(255),
	GENERATIONAL_SUFFIX VARCHAR(255),
	GENDER VARCHAR(255),
	COMPANY_NAME VARCHAR(255),
	ADDRESS_LINE1 VARCHAR(255),
	ADDRESS_LINE2 VARCHAR(255),
	ADDRESS_LINE3 VARCHAR(255),
	ADDRESS_LINE4 VARCHAR(255),
	LOCALITY1 VARCHAR(255),
	LOCALITY2 VARCHAR(255),
	LOCALITY3 VARCHAR(255),
	REGION1 VARCHAR(255),
	REGION2 VARCHAR(255),
	POSTAL_CODE VARCHAR(255),
	COUNTRY_CODE VARCHAR(255),
	EMAIL_ADDRESS1 VARCHAR(255),
	EMAIL_ADDRESS2 VARCHAR(255),
	EMAIL_ADDRESS3 VARCHAR(255),
	PHONE1 VARCHAR(255),
	PHONE2 VARCHAR(255),
	PHONE3 VARCHAR(255),
	LOAD_DTTM TIMESTAMP_NTZ(9),
	UPDATE_DTTM TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS UC2_SEND_TO_EPSILON_HIST_LOAD_TEST_AIRFLOW (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
create TABLE IF NOT EXISTS UC2_SEND_TO_EPSILON_TEST_AIRFLOW (
	SRC_JOB_ID VARCHAR(255) NOT NULL COMMENT 'Batch Identifier specifies the batch that inserted the record into the table.',
	SRC_RECORD_ID VARCHAR(255) NOT NULL COMMENT 'User-defined record ID; Source record ID specific to brand customer. Example: <Brand>_<MD5(Email || Mobile)>.',
	FULL_NAME VARCHAR(255) COMMENT 'Customer full name - Connect derives from name components',
	PREFIX_NAME VARCHAR(255) COMMENT 'Standard name prefix',
	GIVEN_NAME VARCHAR(255) COMMENT 'Customer first name',
	MIDDLE_NAME VARCHAR(255) COMMENT 'Customer middle name or initial',
	FAMILY_NAME VARCHAR(255) COMMENT 'Customer last name',
	GENERATIONAL_SUFFIX VARCHAR(255) COMMENT 'Standard generational suffix',
	GENDER VARCHAR(255) COMMENT 'Customer gender - Incoming gender not used; Connect derives from name hygiene',
	COMPANY_NAME VARCHAR(255) COMMENT 'Company or business name',
	ADDRESS_LINE1 VARCHAR(255) COMMENT 'First line of mailing address',
	ADDRESS_LINE2 VARCHAR(255) COMMENT 'Supplementary line of mailing address',
	ADDRESS_LINE3 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	ADDRESS_LINE4 VARCHAR(255) COMMENT 'Supplementary line of mailing address, used primarily for non-NA addresses',
	LOCALITY1 VARCHAR(255) COMMENT 'Locality preferred by the postal authority (City, town, locality, or suburb)',
	LOCALITY2 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	LOCALITY3 VARCHAR(255) COMMENT 'Additional city, town, locality, or suburb information',
	REGION1 VARCHAR(255) COMMENT 'State, province, territory, or region',
	REGION2 VARCHAR(255) COMMENT 'Additional state, province, territory, or region',
	POSTAL_CODE VARCHAR(255) COMMENT 'Postal code',
	COUNTRY_CODE VARCHAR(255) COMMENT 'Country code - ISO 3166-1 (alpha-3)',
	EMAIL_ADDRESS1 VARCHAR(255) COMMENT 'Primary email address of customer',
	EMAIL_ADDRESS2 VARCHAR(255) COMMENT 'Additional email address of customer',
	EMAIL_ADDRESS3 VARCHAR(255) COMMENT 'Additional email address of customer',
	PHONE1 VARCHAR(255) COMMENT 'Primary phone number of customer',
	PHONE2 VARCHAR(255) COMMENT 'Additional phone number of customer',
	PHONE3 VARCHAR(255) COMMENT 'Additional phone number of customer',
	LOAD_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Load Datetime is the date and time in UTC when the record was created.',
	UPDATE_DTTM TIMESTAMP_NTZ(9) NOT NULL COMMENT 'Update Datetime is the date and time in UTC when the record was last updated.',
	constraint XAK1UC2_SEND_TO_EPSILON unique (SRC_JOB_ID, SRC_RECORD_ID)
);
CREATE VIEW IF NOT EXISTS AYEFREMOV_IDM_COREDIM_CUSTOMER_V(
	BRAND_ID,
	MBR_INSPIRE_ID,
	MBR_ID,
	CLOSEST_STORE_ID,
	HOUSEHOLD_ID,
	EMAIL_ID,
	MOBILE_DEVICE_ID,
	EXPERIAN_ID,
	EXPERIAN_STATUS_TYP,
	INSPIRE_CUST_TYP,
	CUSTOMER_AUTHENTICATION_STATUS,
	MDM_ID,
	MDM_ID_DELETED_IND,
	LOYALTY_CARD_NBR,
	FIRST_NM,
	LAST_NM,
	MIDDLE_INITIAL_TXT,
	DOB_DT,
	BIRTH_MNTH_ID,
	BIRTH_YEAR_ID,
	BIRTH_DT_IMPLIED_IND,
	GENDER_TYP,
	ENROLLMENT_CHANNEL_TYP,
	MBR_STATUS_CD,
	POINT_BALANCE_QTY,
	MEMBERSHIP_STATUS_CD,
	PROFILE_COMPLETED_STATUS_IND,
	DELIVERABILITY_STATUS_IND,
	MOBILE_NBR,
	ADR_LINE_1_TXT,
	ADR_LINE_2_TXT,
	CTY_NM,
	ST_CD,
	ZIP_CD,
	CNTRY_CD,
	EMAIL_OPT_OUT_IND,
	PUSH_NOTIFICATION_OPT_IN_IND,
	SMS_OPT_IN_IND,
	PRIVACY_IND,
	UNSUBSCRIBE_DTTM,
	POINT_EXPIRE_DTTM,
	ENROLL_START_DTTM,
	LAST_LOGIN_DTTM,
	LAST_STATUS_CHANGE_DTTM,
	PROFILE_COMPLETION_DTTM,
	SUBSCRIBER_KEY,
	SUBSCRIBER_SOURCE_NM,
	LOYALTY_TIER_CHANGE_DTTM,
	LOYALTY_TIER_NM,
	LOYALTY_TIER_EXPIRATION_DT,
	LOYALTY_ELITE_VISIT_CNT,
	SOURCE_SYSTEM_NM,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	PUSH_DEVICE_ID,
	ADDRESS_OPT_IN_IND,
	IGNORE_FRAUD_SUSPEND_IND,
	EMAIL_OPT_OUT_STATUS_IND
) as
SELECT
    BC.BRAND_ID AS BRAND_ID,
    BC.BRAND_CUSTOMER_ID AS MBR_INSPIRE_ID,
    BM.BRAND_MEMBER_ID AS MBR_ID,
    BM.CLOSEST_REST_ID AS CLOSEST_STORE_ID,
    NULL AS HOUSEHOLD_ID,
    BCE.STRIPPED_EMAIL_ADDRESS AS EMAIL_ID,
    BCD.CUSTOMER_DEVICE_ID AS MOBILE_DEVICE_ID,
    NULL AS EXPERIAN_ID,
    NULL AS EXPERIAN_STATUS_TYP,
    NULL AS INSPIRE_CUST_TYP,
    BC.CUSTOMER_AUTHENTICATION_STATUS AS CUSTOMER_AUTHENTICATION_STATUS,
    NULL AS MDM_ID,
    NULL AS MDM_ID_DELETED_IND,
    BM.MEMBER_CARD_NBR AS LOYALTY_CARD_NBR,
    BC.FIRST_NAME AS FIRST_NM,
    BC.LAST_NAME AS LAST_NM,
    BC.MIDDLE_NAME AS MIDDLE_INITIAL_TXT,
    BC.DATE_OF_BIRTH AS DOB_DT,
    BC.BIRTH_MONTH AS BIRTH_MNTH_ID,
    BC.BIRTH_YR AS BIRTH_YEAR_ID,
    BC.BIRTH_IMPLIED_IND AS BIRTH_DT_IMPLIED_IND,
    BC.GENDER AS GENDER_TYP,
    BM.ENROLLMENT_CHANNEL_TYPE AS ENROLLMENT_CHANNEL_TYP,
    BM.MEMBER_STATUS_CODE AS MBR_STATUS_CD,
    BMPB.POINT_BALANCE_QTY AS POINT_BALANCE_QTY,
    NULL AS MEMBERSHIP_STATUS_CD,
    BCPS.SURVEY_COMPLETION_STATUS_IND AS PROFILE_COMPLETED_STATUS_IND,
    BCE.EMAIL_DELIVERABILITY_STATUS AS DELIVERABILITY_STATUS_IND,
    BCP.PHONE_NBR AS MOBILE_NBR,
    BCA.ADDRESS_LINE1_TXT AS ADR_LINE_1_TXT,
    BCA.ADDRESS_LINE2_TXT AS ADR_LINE_2_TXT,
    BCA.CITY AS CTY_NM,
    BCA.STATE_CODE AS ST_CD,
    BCA.ZIP_CODE AS ZIP_CD,
    BCA.COUNTRY_CODE AS CNTRY_CD,
    BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_IND,
    NOT BCD.CUSTOMER_DEVICE_PUSH_OPT_OUT_IND AS PUSH_NOTIFICATION_OPT_IN_IND,
    NOT BCD.CUSTOMER_DEVICE_SMS_OPT_OUT_IND AS SMS_OPT_IN_IND,
    BC.PRIVACY_IND AS PRIVACY_IND,
    BM.UNSUBSCRIBE_DTTM AS UNSUBSCRIBE_DTTM,
    BMPB.POINT_EXPIRE_DATE AS POINT_EXPIRE_DTTM,
    BM.ENROLL_START_DATE AS ENROLL_START_DTTM,
    BM.LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
    BM.LAST_STATUS_CHANGE_DTTM AS LAST_STATUS_CHANGE_DTTM,
    BCPS.SURVEY_COMPLETION_DATE AS PROFILE_COMPLETION_DTTM,
    SPLIT_PART(BM.BRAND_CUSTOMER_ID, '.',  0) as SUBSCRIBER_KEY,
    NULL AS SUBSCRIBER_SOURCE_NM,
    NULL AS LOYALTY_TIER_CHANGE_DTTM,
    NULL AS LOYALTY_TIER_NM,
    NULL AS LOYALTY_TIER_EXPIRATION_DT,
    NULL AS LOYALTY_ELITE_VISIT_CNT,
    CASE
        WHEN BC.SOURCE_SYSTEM_NAME = 'epsilon' AND BC.BRAND_ID = 'arbys' THEN 'loyalty'
        ELSE BC.SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    NULL AS CDM_LOAD_DT,
    BC.LOAD_ID AS LOAD_ID,
    BC.LOAD_DTTM AS LOAD_DTTM,
    BC.UPDATE_ID AS UPDATE_ID,
    BC.UPDATE_DTTM UPDATE_DTTM,
    BCD.DEVICE_ID AS PUSH_DEVICE_ID,
    NOT BCA.CUSTOMER_ADDRESS_OPT_OUT_IND AS ADDRESS_OPT_IN_IND,
    BC.IGNORE_FRAUD_SUSPEND_IND AS IGNORE_FRAUD_SUSPEND_IND,
    CEMSCA.EMAIL_OPT_OUT_STATUS_IND AS EMAIL_OPT_OUT_STATUS_IND
FROM
    IDS_PROD.CUST.BRAND_CUSTOMER BC
    LEFT JOIN IDS_PROD.CUST.BRAND_CUSTOMER_ADDRESS BCA ON BC.BRAND_CUSTOMER_ID = BCA.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_PROD.CUST.BRAND_CUSTOMER_PHONE BCP ON BC.BRAND_CUSTOMER_ID = BCP.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_PROD.CUST.BRAND_CUSTOMER_EMAIL BCE ON BC.BRAND_CUSTOMER_ID = BCE.BRAND_CUSTOMER_ID
    INNER JOIN IDS_PROD.CUST.BRAND_MEMBER BM ON BC.SOURCE_CUSTOMER_ID = BM.SOURCE_MEMBER_ID AND BC.BRAND_ID = BM.BRAND_ID
    LEFT JOIN IDS_PROD.CUST.BRAND_MEMBER_POINT_BALANCE BMPB ON BM.BRAND_MEMBER_ID = BMPB.BRAND_MEMBER_ID
    LEFT JOIN IDS_PROD.CUST.BRAND_CUSTOMER_DEVICE BCD ON BC.BRAND_CUSTOMER_ID = BCD.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_PROD.CUST.BRAND_CUSTOMER_PROFILE_SURVEY BCPS ON BC.BRAND_CUSTOMER_ID = BCPS.BRAND_CUSTOMER_ID
    LEFT JOIN STG_DEV.ARB.AYEFREMOV_CURRENT_EMAIL_STATUS_FROM_CRM_EMAIL_STATUS_CHANGE_ARBYS AS CEMSCA 
    ON lower(BCE.STRIPPED_EMAIL_ADDRESS) = lower(CEMSCA.EMAIL_ADDRESS) and lower(BC.BRAND_CUSTOMER_ID) = lower(CEMSCA.BRAND_CUSTOMER_ID)
WHERE BC.BRAND_ID = 'arbys' and SOURCE_SYSTEM_NM = 'loyalty'
;
CREATE VIEW IF NOT EXISTS COREDIM_CUSTOMER_V_BMTEST(
	BRAND_ID,
	PLATFORM_CUSTOMER_ID,
	PLATFORM_CUSTOMER_SOURCE_NAME,
	CURR_MEMBER_ID,
	CURR_SUBSCRIBER_KEY,
	CLOSEST_REST_ID,
	FIRST_NAME,
	MIDDLE_NAME,
	LAST_NAME,
	DATE_OF_BIRTH,
	BIRTH_MONTH,
	BIRTH_YR,
	BIRTH_IMPLIED_IND,
	GENDER,
	ENROLLMENT_CHANNEL_TYPE,
	MEMBER_STATUS_CODE,
	ENROLL_START_DATE,
	LAST_STATUS_CHANGE_DTTM,
	IGNORE_FRAUD_SUSPEND_IND,
	PRIVACY_IND,
	GLOBAL_OPT_OUT_FLAG,
	INPSIRE_CUSTOMER_TYPE,
	SOURCE_SYSTEM_NAME,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as
select *
from IDS_DEV.CUST.CUSTOMER;
CREATE VIEW IF NOT EXISTS COREDIM_LOYALTY_MBR_OFFER_V(
	BRAND_ID,
	CDM_LOAD_DT,
	CREATE_DTTM,
	EXPIRATION_DTTM,
	LOAD_DTTM,
	LOAD_ID,
	MBR_ID,
	MBR_OFFER_ID,
	OFFER_CD,
	OPT_IN_DTTM,
	PRIVACY_IND,
	SOURCE_LOADED_DTTM,
	SOURCE_MODIFIED_DTTM,
	SOURCE_SYSTEM_NM,
	UPDATE_DTTM,
	UPDATE_ID
) as
    SELECT
    BRAND_ID AS BRAND_ID,
	NULL AS CDM_LOAD_DT,
	CREATE_DTTM AS CREATE_DTTM,
	EXPIRATION_DTTM AS EXPIRATION_DTTM,
	LOAD_DTTM AS LOAD_DTTM,
    LOAD_ID AS LOAD_ID,
	BRAND_MEMBER_ID AS MBR_ID,
    MBR_OFFER_ID AS MBR_OFFER_ID,
	OFFER_CODE AS OFFER_CD,
	OPT_IN_DTTM AS OPT_IN_DTTM,
	'FALSE' AS PRIVACY_IND,
    LOAD_DTTM AS SOURCE_LOADED_DTTM,
	SOURCE_MODIFIED_DTTM AS SOURCE_MODIFIED_DTTM,
	CASE 
		WHEN SOURCE_SYSTEM_NAME = 'epsilon' THEN 'loyalty'
		ELSE SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    UPDATE_DTTM AS UPDATE_DTTM,
    UPDATE_ID AS UPDATE_ID
FROM
    IDS_DEV.CUST.LOYALTY_MEMBER_OFFER;
CREATE VIEW IF NOT EXISTS COREDIM_LOYALTY_OFFER_V(
	BRAND_ID,
	OFFER_CD,
	UNIQUE_OFFER_ID,
	VARIANT_ID,
	OFFER_SEQ_KEY,
	OFFER_NM,
	OFFER_REPORT_DESC,
	OFFER_SHORT_DESC,
	OFFER_DESC,
	OPT_IN_REQUIRED_IND,
	LOC_NM,
	EXCLUSION_TXT,
	OFFER_IMG_NM,
	TERM_AND_CONDITION_TXT,
	MOMENT_ELIGIBILITY_TXT,
	STANDARD_EXCLUSION_TXT,
	OTHER_EXCLUSION_TXT,
	POINT_AWARDED_QTY,
	LIMIT_NBR,
	STORE_LOC_LIST_NBR,
	DAY_PART_LIST_NBR,
	MBR_LIST_NEEDED_IND,
	STORE_TYP,
	ONE_TM_OFFER_IND,
	TRIVIA_NEEDED_IND,
	CHECK_IN_IND,
	OFFER_PRIORITY_NBR,
	OFFER_TYP,
	AWARD_NM,
	PRODUCT_PLUS_DESC,
	TRIVIA_QUESTION_IND,
	TRIVIA_ANSWER_IND,
	TARGET_OFFER_IND,
	PRIVACY_IND,
	VALIDITY_PERIOD_NBR,
	VALIDITY_UNIT_DT_NM,
	DNA_INCENTIVE_TYP,
	DNA_CAMPAIGN_TYP,
	DNA_DISCOUNT_PRODUCT_NM,
	DNA_PRODUCT_CATEGORY_NM,
	DNA_ACTION_REQUIRED_TXT,
	DNA_MINIMUM_SPEND_AMT,
	DNA_OFFER_WEEK_IN_CAMPAIGN_NBR,
	OFFER_VISIBLE_DTTM,
	OFFER_START_DTTM,
	OFFER_END_DTTM,
	DNA_INCENTIVE_AMT,
	SYSTEM_OFFER_ID,
	DNA_DISCOUNT_PRODUCT_LEVEL_CD,
	DNA_OFFER_REDEMPTION_CHANNEL_TYP,
	STATUS_CD,
	OFFER_IMAGE2_NM,
	SOURCE_APPLICATION_TYP,
	PARENT_OFFER_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	OFFER_TAG,
	CDM_LOAD_DT
) as
SELECT
    BRAND_ID AS BRAND_ID,
    OFFER_CODE AS OFFER_CD,
    UNIQ_OFFER_ID AS UNIQUE_OFFER_ID,
    VARIANT_ID AS VARIANT_ID,
    OFFER_SEQ_KEY AS OFFER_SEQ_KEY,
    OFFER_NAME AS OFFER_NM,
    OFFER_REPORT_DESC AS OFFER_REPORT_DESC,
    OFFER_SHORT_DESC AS OFFER_SHORT_DESC,
    OFFER_DESC AS OFFER_DESC,
    OPTIN_REQUIRED_IND AS OPT_IN_REQUIRED_IND,
    LOCATION_NAME AS LOC_NM,
    EXCLUSIONS_TXT AS EXCLUSION_TXT,
    OFFER_IMAGE_NAME AS OFFER_IMG_NM,
    TERMS_AND_CONDITIONS AS TERM_AND_CONDITION_TXT,
    MOMENT_ELIGIBILITY AS MOMENT_ELIGIBILITY_TXT,
    STANDARD_EXCLUSIONS_TXT AS STANDARD_EXCLUSION_TXT,
    OTHER_EXCLUSIONS AS OTHER_EXCLUSION_TXT,
    POINTS_AWARDED_NBR AS POINT_AWARDED_QTY,
    LIMITS_NBR AS LIMIT_NBR,
    REST_LOCATION_NBR AS STORE_LOC_LIST_NBR,
    DAYPART AS DAY_PART_LIST_NBR,
    MEMBER_LIST_NEEDED AS MBR_LIST_NEEDED_IND,
    REST_TYPE AS STORE_TYP,
    ONE_TIME_OFFER_IND AS ONE_TM_OFFER_IND,
    TRIVIA_NEEDED_IND AS TRIVIA_NEEDED_IND,
    CHECK_IN_IND AS CHECK_IN_IND,
    OFFER_PRIORITY_NBR AS OFFER_PRIORITY_NBR,
    OFFER_TYPE AS OFFER_TYP,
    AWARD_NAME AS AWARD_NM,
    PRODUCT_PLU_DESC AS PRODUCT_PLUS_DESC,
    NULL AS TRIVIA_QUESTION_IND,
    NULL AS TRIVIA_ANSWER_IND,
    TARGET_OFFER_IND AS TARGET_OFFER_IND,
    FALSE AS PRIVACY_IND,
    VALIDITY_PERIOD AS VALIDITY_PERIOD_NBR,
    VALIDITY_UNIT_DATE_NAME AS VALIDITY_UNIT_DT_NM,
    DNA_INCENTIVE_TYPE AS DNA_INCENTIVE_TYP,
    DNA_CAMPAIGN_TYPE AS DNA_CAMPAIGN_TYP,
    DNA_DISCOUNT_PRODUCT_NAME AS DNA_DISCOUNT_PRODUCT_NM,
    DNA_PRODUCT_CATEGORY AS DNA_PRODUCT_CATEGORY_NM,
    DNA_ACTION_REQUIRED AS DNA_ACTION_REQUIRED_TXT,
    DNA_MINIMUM_SPEND AS DNA_MINIMUM_SPEND_AMT,
    DNA_OFFER_WEEK_IN_CAMPAIGN AS DNA_OFFER_WEEK_IN_CAMPAIGN_NBR,
    OFFER_VISIBLE_DATE AS OFFER_VISIBLE_DTTM,
    OFFER_START_DATE AS OFFER_START_DTTM,
    OFFER_END_DATE AS OFFER_END_DTTM,
    DNA_INCENTIVE_VAL AS DNA_INCENTIVE_AMT,
    SYSTEM_OFFER_ID AS SYSTEM_OFFER_ID,
    DNA_DISCOUNT_PRODUCT_L_CODE AS DNA_DISCOUNT_PRODUCT_LEVEL_CD,
    DNA_OFFER_REDEMPTION_CHANNEL AS DNA_OFFER_REDEMPTION_CHANNEL_TYP,
    STATUS_CODE AS STATUS_CD,
    OFFER_IMAGE2_NAME AS OFFER_IMAGE2_NM,
    SOURCE_APPLICATION_TYPE AS SOURCE_APPLICATION_TYP,
    PARENT_OFFER_ID AS PARENT_OFFER_ID,
    CASE
        WHEN SOURCE_SYSTEM_NAME = 'epsilon' THEN 'loyalty'
        ELSE SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    LOAD_ID AS LOAD_ID,
    TO_TIMESTAMP(LOAD_DTTM) AS LOAD_DTTM,
    UPDATE_ID AS UPDATE_ID,
    UPDATE_DTTM AS UPDATE_DTTM,
    OFFER_TAG AS OFFER_TAG,
    NULL AS CDM_LOAD_DT
FROM IDS_DEV.CUST.OMS_OFFER;
CREATE VIEW IF NOT EXISTS CUSTOMER_V_RWTEST(
	BRAND_ID,
	MBR_INSPIRE_ID,
	MBR_ID,
	CLOSEST_STORE_ID,
	HOUSEHOLD_ID,
	EMAIL_ID,
	MOBILE_DEVICE_ID,
	EXPERIAN_ID,
	EXPERIAN_STATUS_TYP,
	INSPIRE_CUST_TYP,
	MDM_ID,
	MDM_ID_DELETED_IND,
	LOYALTY_CARD_NBR,
	FIRST_NM,
	LAST_NM,
	MIDDLE_INITIAL_TXT,
	DOB_DT,
	BIRTH_MNTH_ID,
	BIRTH_YEAR_ID,
	BIRTH_DT_IMPLIED_IND,
	GENDER_TYP,
	ENROLLMENT_CHANNEL_TYP,
	MBR_STATUS_CD,
	POINT_BALANCE_QTY,
	MEMBERSHIP_STATUS_CD,
	PROFILE_COMPLETED_STATUS_IND,
	DELIVERABILITY_STATUS_IND,
	MOBILE_NBR,
	ADR_LINE_1_TXT,
	ADR_LINE_2_TXT,
	CTY_NM,
	ST_CD,
	ZIP_CD,
	CNTRY_CD,
	EMAIL_OPT_OUT_IND,
	PUSH_NOTIFICATION_OPT_IN_IND,
	SMS_OPT_IN_IND,
	PRIVACY_IND,
	UNSUBSCRIBE_DTTM,
	POINT_EXPIRE_DTTM,
	ENROLL_START_DTTM,
	LAST_LOGIN_DTTM,
	LAST_STATUS_CHANGE_DTTM,
	PROFILE_COMPLETION_DTTM,
	SUBSCRIBER_KEY,
	SUBSCRIBER_SOURCE_NM,
	LOYALTY_TIER_CHANGE_DTTM,
	LOYALTY_TIER_NM,
	LOYALTY_TIER_EXPIRATION_DT,
	LOYALTY_ELITE_VISIT_CNT,
	SOURCE_SYSTEM_NM,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	PUSH_DEVICE_ID,
	ADDRESS_OPT_IN_IND,
	IGNORE_FRAUD_SUSPEND_IND,
	EMAIL_OPT_OUT_STATUS_IND
) as (
    SELECT
        BC.BRAND_ID AS BRAND_ID,
        'mbr_inspire_id' AS MBR_INSPIRE_ID,
        --is that the same as BC.BRAND_CUSTOMER_ID
        BC.BRAND_CUSTOMER_ID AS MBR_ID,
        -- it was PROFILEID before; BRAND_CUSTOMER_ID will contain PROFILEID + ".epsilon"; should be unique
        BM.CLOSEST_REST_ID AS CLOSEST_STORE_ID,
        ROW_NUMBER() OVER (
            ORDER BY
                BC.BRAND_CUSTOMER_ID
        ) AS HOUSEHOLD_ID,
        -- we don't have PROFILEID, so BC.BRAND_CUSTOMER_ID was used
        BCE.EMAIL_ADDRESS AS EMAIL_ID,
        'mobile_device_id' AS MOBILE_DEVICE_ID,
        -- it might be in BRAND_CUSTOMER_DEVICE
        'experian_id' AS EXPERIAN_ID,
        -- we don't have that
        'experian_status_typ' AS EXPERIAN_STATUS_TYP,
        -- we don't have that
        'inspire_cust_typ' AS INSPIRE_CUST_TYP,
        -- we don't have that
        'mdm_id' AS MDM_ID,
        -- we don't have that
        NULL AS MDM_ID_DELETED_IND,
        -- we don't have that
        BM.MEMBER_CARD_NBR AS LOYALTY_CARD_NBR,
        BC.FIRST_NAME AS FIRST_NM,
        BC.LAST_NAME AS LAST_NM,
        'middle_initial_txt' AS MIDDLE_INITIAL_TXT,
        -- is that BC.MIDDLE_NAME? It seems to suit the pattern of first, middle and last name
        BC.DATE_OF_BIRTH AS DOB_DT,
        -- I know that it's by the design, but DOB is not really meaningfull without context
        BC.BIRTH_MONTH AS BIRTH_MNTH_ID,
        -- is that some kind of ID?
        BC.BIRTH_YR AS BIRTH_YEAR_ID,
        -- is that some kind of ID?
        BC.BIRTH_IMPLIED_IND AS BIRTH_DT_IMPLIED_IND,
        BC.GENDER AS GENDER_TYP,
        BM.ENROLLMENT_CHANNEL_TYPE AS ENROLLMENT_CHANNEL_TYP,
        BM.MEMBER_STATUS_CODE AS MBR_STATUS_CD,
        -- is that correct?
        BMPB.POINT_BALANCE_QTY AS POINT_BALANCE_QTY,
        'membership_status_cd' AS MEMBERSHIP_STATUS_CD,
        -- what is the difference between member and membership status? should it be BM.MEMBER_STATUS_CODE?
        NULL AS PROFILE_COMPLETED_STATUS_IND,
        -- it may be in BRAND_CUSTOMER_PROFILE_SURVEY
        BCE.EMAIL_DELIVERABILITY_STATUS AS DELIVERABILITY_STATUS_IND,
        BCP.PHONE_NBR AS MOBILE_NBR,
        BCA.ADDRESS_LINE1_TXT AS ADR_LINE_1_TXT,
        BCA.ADDRESS_LINE2_TXT AS ADR_LINE_2_TXT,
        BCA.CITY AS CTY_NM,
        BCA.STATE_CODE AS ST_CD,
        BCA.ZIP_CODE AS ZIP_CD,
        BCA.COUNTRY_CODE AS CNTRY_CD,
        BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_IND,
        NULL AS PUSH_NOTIFICATION_OPT_IN_IND,
        --it may be in BRAND_CUSTOMER_DEVICE
        BCP.CUSTOMER_SMS_OPT_OUT_IND AS SMS_OPT_IN_IND,
        BC.PRIVACY_IND AS PRIVACY_IND,
        'unsubscribe_dttm' AS UNSUBSCRIBE_DTTM,
        -- we don't have that ;it was UNSUBSCRIBEDATE; unsubscribe date of what? ;it may be in BRAND_CUSTOMER_DEVICE
        BMPB.POINT_EXPIRE_DATE AS POINT_EXPIRE_DTTM,
        BM.ENROLL_START_DATE AS ENROLL_START_DTTM,
        'last_login_dttm' AS LAST_LOGIN_DTTM,
        -- it was LASTLOGINDATE
        BM.LAST_STATUS_CHANGE_DTTM AS LAST_STATUS_CHANGE_DTTM,
        'profile_completion_dttm' AS PROFILE_COMPLETION_DTTM,
        -- it may be in BRAND_CUSTOMER_PROFILE_SURVEY - it was PROFILECOMPLETIONDATE
        'subscriber_key' AS SUBSCRIBER_KEY,
        -- it was IDPCUSTOMERID;
        BM.BRAND_MEMBER_SOURCE_NAME AS SUBSCRIBER_SOURCE_NM,
        --
        NULL AS LOYALTY_TIER_CHANGE_DTTM,
        NULL AS LOYALTY_TIER_NM,
        NULL AS LOYALTY_TIER_EXPIRATION_DT,
        NULL AS LOYALTY_ELITE_VISIT_CNT,
        BC.SOURCE_SYSTEM_NAME AS SOURCE_SYSTEM_NM,
        NULL AS CDM_LOAD_DT,
        BC.LOAD_ID AS LOAD_ID,
        BC.LOAD_DTTM AS LOAD_DTTM,
        BC.UPDATE_ID AS UPDATE_ID,
        BC.UPDATE_DTTM UPDATE_DTTM,
        NULL AS PUSH_DEVICE_ID,
        -- it may be in BRAND_CUSTOMER_DEVICE
        BCA.CUSTOMER_ADDRESS_OPT_OUT_IND AS ADDRESS_OPT_IN_IND,
        BC.IGNORE_FRAUD_SUSPEND_IND AS IGNORE_FRAUD_SUSPEND_IND,
        BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_STATUS_IND
    FROM
        IDS_DEV.CUST.BRAND_CUSTOMER BC
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_ADDRESS BCA ON BC.BRAND_CUSTOMER_ID = BCA.BRAND_CUSTOMER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_PHONE BCP ON BC.BRAND_CUSTOMER_ID = BCP.BRAND_CUSTOMER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_EMAIL BCE ON BC.BRAND_CUSTOMER_ID = BCE.BRAND_CUSTOMER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_MEMBER BM ON BC.BRAND_CUSTOMER_ID = BM.BRAND_MEMBER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_MEMBER_POINT_BALANCE BMPB ON BC.BRAND_CUSTOMER_ID = BMPB.BRAND_MEMBER_ID
);
CREATE VIEW IF NOT EXISTS CUST_CUSTOMER_V_RWTEST(
	BRAND_ID,
	MBR_INSPIRE_ID,
	MBR_ID,
	CLOSEST_STORE_ID,
	HOUSEHOLD_ID,
	EMAIL_ID,
	MOBILE_DEVICE_ID,
	EXPERIAN_ID,
	EXPERIAN_STATUS_TYP,
	INSPIRE_CUST_TYP,
	MDM_ID,
	MDM_ID_DELETED_IND,
	LOYALTY_CARD_NBR,
	FIRST_NM,
	LAST_NM,
	MIDDLE_INITIAL_TXT,
	DOB_DT,
	BIRTH_MNTH_ID,
	BIRTH_YEAR_ID,
	BIRTH_DT_IMPLIED_IND,
	GENDER_TYP,
	ENROLLMENT_CHANNEL_TYP,
	MBR_STATUS_CD,
	POINT_BALANCE_QTY,
	MEMBERSHIP_STATUS_CD,
	PROFILE_COMPLETED_STATUS_IND,
	DELIVERABILITY_STATUS_IND,
	MOBILE_NBR,
	ADR_LINE_1_TXT,
	ADR_LINE_2_TXT,
	CTY_NM,
	ST_CD,
	ZIP_CD,
	CNTRY_CD,
	EMAIL_OPT_OUT_IND,
	PUSH_NOTIFICATION_OPT_IN_IND,
	SMS_OPT_IN_IND,
	PRIVACY_IND,
	UNSUBSCRIBE_DTTM,
	POINT_EXPIRE_DTTM,
	ENROLL_START_DTTM,
	LAST_LOGIN_DTTM,
	LAST_STATUS_CHANGE_DTTM,
	PROFILE_COMPLETION_DTTM,
	SUBSCRIBER_KEY,
	SUBSCRIBER_SOURCE_NM,
	LOYALTY_TIER_CHANGE_DTTM,
	LOYALTY_TIER_NM,
	LOYALTY_TIER_EXPIRATION_DT,
	LOYALTY_ELITE_VISIT_CNT,
	SOURCE_SYSTEM_NAME,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	PUSH_DEVICE_ID,
	ADDRESS_OPT_IN_IND,
	IGNORE_FRAUD_SUSPEND_IND,
	EMAIL_OPT_OUT_STATUS_IND
) as (
    SELECT
        BC.BRAND_ID AS BRAND_ID,
        NULL AS MBR_INSPIRE_ID,
        -- is that the same as BC.BRAND_CUSTOMER_ID; it is deprecated
        BC.BRAND_CUSTOMER_ID AS MBR_ID,
        -- it was PROFILEID before; BRAND_CUSTOMER_ID will contain PROFILEID + ".epsilon"; should be unique
        BM.CLOSEST_REST_ID AS CLOSEST_STORE_ID,
        ROW_NUMBER() OVER (
            ORDER BY
                BC.BRAND_CUSTOMER_ID
        ) AS HOUSEHOLD_ID,
        -- we don't have PROFILEID, so BC.BRAND_CUSTOMER_ID was used
        BCE.EMAIL_ADDRESS AS EMAIL_ID,
        BCD.CUSTOMER_DEVICE_ID AS MOBILE_DEVICE_ID,
        'experian_id' AS EXPERIAN_ID,
        -- we don't have that
        'experian_status_typ' AS EXPERIAN_STATUS_TYP,
        -- we don't have that
        'inspire_cust_typ' AS INSPIRE_CUST_TYP,
        -- we don't have that
        'mdm_id' AS MDM_ID,
        -- we don't have that
        NULL AS MDM_ID_DELETED_IND,
        -- we don't have that
        BM.MEMBER_CARD_NBR AS LOYALTY_CARD_NBR,
        BC.FIRST_NAME AS FIRST_NM,
        BC.LAST_NAME AS LAST_NM,
        'middle_initial_txt' AS MIDDLE_INITIAL_TXT,
        -- is that BC.MIDDLE_NAME? It seems to suit the pattern of first, middle and last name
        BC.DATE_OF_BIRTH AS DOB_DT,
        -- I know that it's by the design, but DOB is not really meaningfull without context
        BC.BIRTH_MONTH AS BIRTH_MNTH_ID,
        -- is that some kind of ID?
        BC.BIRTH_YR AS BIRTH_YEAR_ID,
        -- is that some kind of ID?
        BC.BIRTH_IMPLIED_IND AS BIRTH_DT_IMPLIED_IND,
        BC.GENDER AS GENDER_TYP,
        BM.ENROLLMENT_CHANNEL_TYPE AS ENROLLMENT_CHANNEL_TYP,
        BM.MEMBER_STATUS_CODE AS MBR_STATUS_CD,
        BMPB.POINT_BALANCE_QTY AS POINT_BALANCE_QTY,
        'membership_status_cd' AS MEMBERSHIP_STATUS_CD,
        -- what is the difference between member and membership status? should it be BM.MEMBER_STATUS_CODE?
        BCPS.SURVEY_COMPLETION_STATUS_IND AS PROFILE_COMPLETED_STATUS_IND,
        BCE.EMAIL_DELIVERABILITY_STATUS AS DELIVERABILITY_STATUS_IND,
        BCP.PHONE_NBR AS MOBILE_NBR,
        BCA.ADDRESS_LINE1_TXT AS ADR_LINE_1_TXT,
        BCA.ADDRESS_LINE2_TXT AS ADR_LINE_2_TXT,
        BCA.CITY AS CTY_NM,
        BCA.STATE_CODE AS ST_CD,
        BCA.ZIP_CODE AS ZIP_CD,
        BCA.COUNTRY_CODE AS CNTRY_CD,
        BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_IND,
        NULL AS PUSH_NOTIFICATION_OPT_IN_IND,
        -- it may be in BRAND_CUSTOMER_DEVICE; we don't have that
        BCP.CUSTOMER_SMS_OPT_OUT_IND AS SMS_OPT_IN_IND,
        BC.PRIVACY_IND AS PRIVACY_IND,
        'unsubscribe_dttm' AS UNSUBSCRIBE_DTTM,
        -- we don't have that ;it was UNSUBSCRIBEDATE; unsubscribe date of what? ;it may be in BRAND_CUSTOMER_DEVICE
        BMPB.POINT_EXPIRE_DATE AS POINT_EXPIRE_DTTM,
        BM.ENROLL_START_DATE AS ENROLL_START_DTTM,
        'last_login_dttm' AS LAST_LOGIN_DTTM,
        -- it was LASTLOGINDATE
        BM.LAST_STATUS_CHANGE_DTTM AS LAST_STATUS_CHANGE_DTTM,
        'profile_completion_dttm' AS PROFILE_COMPLETION_DTTM,
        -- it may be in BRAND_CUSTOMER_PROFILE_SURVEY - it was PROFILECOMPLETIONDATE; we are missing that
        BM.BRAND_MEMBER_ID AS SUBSCRIBER_KEY,
        -- it was IDPCUSTOMERID; should it be modified somehow (like IDP added or so?)
        BM.BRAND_MEMBER_SOURCE_NAME AS SUBSCRIBER_SOURCE_NM,
        NULL AS LOYALTY_TIER_CHANGE_DTTM,
        NULL AS LOYALTY_TIER_NM,
        NULL AS LOYALTY_TIER_EXPIRATION_DT,
        NULL AS LOYALTY_ELITE_VISIT_CNT,
        CASE 
    		WHEN BC.SOURCE_SYSTEM_NAME = 'epsilon' THEN 'loyalty'
    		ELSE BC.SOURCE_SYSTEM_NAME
        END AS SOURCE_SYSTEM_NAME,
        NULL AS CDM_LOAD_DT,
        BC.LOAD_ID AS LOAD_ID,
        BC.LOAD_DTTM AS LOAD_DTTM,
        BC.UPDATE_ID AS UPDATE_ID,
        BC.UPDATE_DTTM UPDATE_DTTM,
        BCD.DEVICE_ID AS PUSH_DEVICE_ID,
        -- is that just device_id or customer_device_id?
        BCA.CUSTOMER_ADDRESS_OPT_OUT_IND AS ADDRESS_OPT_IN_IND,
        BC.IGNORE_FRAUD_SUSPEND_IND AS IGNORE_FRAUD_SUSPEND_IND,
        BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_STATUS_IND
    FROM
        IDS_DEV.CUST.BRAND_CUSTOMER BC
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_ADDRESS BCA ON BC.BRAND_CUSTOMER_ID = BCA.BRAND_CUSTOMER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_PHONE BCP ON BC.BRAND_CUSTOMER_ID = BCP.BRAND_CUSTOMER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_EMAIL BCE ON BC.BRAND_CUSTOMER_ID = BCE.BRAND_CUSTOMER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_MEMBER BM ON BC.BRAND_CUSTOMER_ID = BM.BRAND_MEMBER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_MEMBER_POINT_BALANCE BMPB ON BC.BRAND_CUSTOMER_ID = BMPB.BRAND_MEMBER_ID
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_DEVICE BCD ON BC.BRAND_CUSTOMER_ID = BCD.BRAND_CUSTOMER_ID -- no data for Arby's
        LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_PROFILE_SURVEY BCPS ON BC.BRAND_CUSTOMER_ID = BCPS.BRAND_CUSTOMER_ID -- no data for Arby's
);
CREATE VIEW IF NOT EXISTS ECOSURE_AUDIT_V(
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	CONCEPTNUM,
	UNITNUM,
	REFERENCENUM,
	FORMNAME,
	AUDITID,
	AUDITSTARTED,
	AUDITENDED,
	CYCLE,
	VISIT,
	UNITMGRFIRST,
	UNITMGRLAST,
	UNITMGRTITLE,
	AUDITORID,
	ADDRESS1,
	ADDRESS2,
	CITY,
	STATE,
	ZIP,
	COUNTRY,
	NOTE,
	ECOSURESTOREID,
	LOAD_DTTM,
	LOAD_FILENAME
) as
WITH
    ecosure_audit AS (
        SELECT
            CASE
                WHEN trim(CONCEPTNUM) = 'ARB' THEN 'arbys'
                WHEN trim(CONCEPTNUM) = 'SON' THEN 'sonic'
                WHEN trim(CONCEPTNUM) in ('BWW', 'BWG') THEN 'bww'
                WHEN trim(CONCEPTNUM) in ('DBI', 'DB', 'DBF') THEN splt.BRAND_ID
                END                                  as BRAND_ID,
            trim(CONCEPTNUM)                         as CONCEPTNUM,
            CASE
                WHEN CONCEPTNUM in ('DBI', 'DB', 'DBF') THEN
                    IFF(LENGTH(UNITNUM) < 6,
                        LPAD(UNITNUM, 6, 0),
                        UNITNUM)
                ELSE IFF(LENGTH(UNITNUM) < 5,
                        LPAD(UNITNUM, 5, 0),
                        UNITNUM)
                END                                  as UNITNUM,
            UPPER(TRIM(REFERENCENUM))                as REFERENCENUM,
            UPPER(TRIM(FORMNAME))                    as FORMNAME,
            AUDITID::VARCHAR                         as AUDITID,
            AUDITSTARTED                             as AUDITSTARTED,
            AUDITENDED                               as AUDITENDED,
            CYCLE                                    as CYCLE,
            VISIT                                    as VISIT,
            NULLIF(UPPER(TRIM(UNITMGRFIRST)), '')    as UNITMGRFIRST,
            NULLIF(UPPER(TRIM(UNITMGRLAST)), '')     as UNITMGRLAST,
            NULLIF(UPPER(TRIM(UNITMGRTITLE)), '')    as UNITMGRTITLE,
            AUDITORID                                as AUDITORID,
            NULLIF(TRIM(ADDRESS1), '')               as ADDRESS1,
            NULLIF(TRIM(ADDRESS2), '')               as ADDRESS2,
            NULLIF(TRIM(CITY), '')                   as CITY,
            NULLIF(TRIM(STATE), '')                  as STATE,
            NULLIF(TRIM(ZIP), '')                    as ZIP,
            NULLIF(TRIM(COUNTRY), '')                as COUNTRY,
            NULLIF(TRIM(NOTE), '')                   as NOTE,
            ECOSURESTOREID                           as ECOSURESTOREID,
            LOAD_DTTM,
            LOAD_FILENAME
        from RDS_DEV.IRB.ECOSURE_AUDIT
        LEFT JOIN table(IDS_DEV.LOCN.DNKN_BSKN_SPLIT()) splt
            on IFF(LENGTH(UNITNUM) < 6, LPAD(UNITNUM, 6, 0), UNITNUM) = splt.STORE_ID
                and trim(CONCEPTNUM) in ('DBI', 'DB', 'DBF')
        WHERE trim(CONCEPTNUM) in ('ARB', 'BWW', 'BWG', 'SON', 'DBI', 'DB', 'DBF')
    )
SELECT
    audit.BRAND_ID,
    'ecosure' as SOURCE_SYSTEM_NAME,
    audit.CONCEPTNUM,
    audit.UNITNUM,
    audit.REFERENCENUM,
    audit.FORMNAME,
    audit.AUDITID,
    audit.AUDITSTARTED,
    audit.AUDITENDED,
    audit.CYCLE,
    audit.VISIT,
    audit.UNITMGRFIRST,
    audit.UNITMGRLAST,
    audit.UNITMGRTITLE,
    audit.AUDITORID,
    audit.ADDRESS1,
    audit.ADDRESS2,
    audit.CITY,
    audit.STATE,
    audit.ZIP,
    audit.COUNTRY,
    audit.NOTE,
    audit.ECOSURESTOREID,
    audit.LOAD_DTTM,
    audit.LOAD_FILENAME
FROM ecosure_audit audit
INNER JOIN IDM_DEV.COREDIM_BV.RESTAURANT_SCD_DIM_BV rest
    ON audit.BRAND_ID = rest.BRAND_ID
    AND audit.UNITNUM = rest.STORE_ID
    AND rest.current_ind = TRUE
WHERE audit.BRAND_ID is not null
;
CREATE VIEW IF NOT EXISTS ECOSURE_SECTIONS_V(
	SECTION_LEVEL,
	AUDITID,
	SECTIONNUM,
	SUBSECTIONFLAG,
	SECTIONNAME,
	PARENTSECTIONNUM,
	LOAD_DTTM,
	FILE_DATE
) as
WITH RECURSIVE
    latest_ecosure_sections as ( /* NON-RECURSIVE CTE */
        SELECT AUDITID::varchar                             as AUDITID,
               nullif(trim(upper(SECTIONNUM)), '')          as SECTIONNUM,
               nullif(trim(upper(SECTIONNAME)), '')         as SECTIONNAME,
               nullif(trim(NOTE), '')                       as NOTE,
               nullif(trim(upper(SUBSECTIONFLAG)), '')      as SUBSECTIONFLAG,
               nullif(trim(upper(PARENTSECTIONNUM)), '')    as PARENTSECTIONNUM,
               nullif(trim(upper(PARENTSECTIONNAME)), '')   as PARENTSECTIONNAME,
               LOAD_DTTM                                    as LOAD_DTTM,
               TO_DATE(
                    REGEXP_SUBSTR(LOAD_FILENAME, '_([0-9]{8,})(/part|\\.csv)', 1, 1, 'e', 1),
                    'yyyyMMdd'
                 )                                          as FILE_DATE
        FROM RDS_DEV.IRB.ECOSURE_SECTIONS
        QUALIFY row_number() over (
            partition by AUDITID, trim(upper(SECTIONNUM))
            order by LOAD_DTTM desc,
                     FILE_DATE desc NULLS LAST
            ) = 1
    ),
    sections AS ( /* RECURSIVE CTE */
        SELECT
            1 AS SECTION_LEVEL,
            AUDITID,
            SECTIONNUM,
            SUBSECTIONFLAG,
            SECTIONNAME,
            PARENTSECTIONNUM,
            LOAD_DTTM,
            FILE_DATE
        FROM latest_ecosure_sections
        WHERE SUBSECTIONFLAG = 'N'

        UNION ALL

        SELECT
            sections.SECTION_LEVEL + 1 AS SECTION_LEVEL,
            src_sections.AUDITID,
            src_sections.SECTIONNUM,
            src_sections.SUBSECTIONFLAG,
            src_sections.SECTIONNAME,
            src_sections.PARENTSECTIONNUM,
            src_sections.LOAD_DTTM,
            src_sections.FILE_DATE
        FROM latest_ecosure_sections src_sections
        INNER JOIN sections
            ON src_sections.AUDITID = sections.AUDITID
            AND src_sections.PARENTSECTIONNUM = sections.SECTIONNUM
    )
SELECT
    SECTION_LEVEL,
    AUDITID,
    SECTIONNUM,
    SUBSECTIONFLAG,
    SECTIONNAME,
    PARENTSECTIONNUM,
    LOAD_DTTM,
    FILE_DATE
from sections;
CREATE VIEW IF NOT EXISTS ECOSURE_UNION_ISSUES_V(
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	AUDITID,
	AUDITRESPID,
	ISSUENUM,
	ISSUE,
	NOTE,
	LOAD_DTTM,
	BATCH_DT
) as
(
    SELECT 'arbys' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           AUDITRESPID,
           ISSUENUM,
           ISSUE,
           NOTE,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.ARB.ECOSUREAUDITEXTRACT_ISSUES

    UNION ALL

    SELECT 'dnkn' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           AUDITRESPID,
           ISSUENUM,
           ISSUE,
           NOTE,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.DUN.ECOSUREAUDITEXTRACT_ISSUES

    UNION ALL

    SELECT 'bww' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           AUDITRESPID,
           ISSUENUM,
           ISSUE,
           NOTE,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.BWW.ECOSUREAUDITEXTRACT_ISSUES

    UNION ALL

    SELECT 'sonic' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           AUDITRESPID,
           ISSUENUM,
           ISSUE,
           NOTE,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.SDI.ECOSUREAUDITEXTRACT_ISSUES

    UNION ALL

    SELECT BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           AUDITRESPID,
           ISSUENUM,
           ISSUE,
           NOTE,
           LOAD_DTTM,
           TO_DATE(
                   REGEXP_SUBSTR(LOAD_FILENAME, '_([0-9]{8,})/', 1, 1, 'e', 1),
                   'yyyyMMdd'
           ) AS BATCH_DT
    FROM RDS_DEV.IRB.ECOSURE_ISSUES

);
CREATE VIEW IF NOT EXISTS ECOSURE_UNION_RESPONSE_V(
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	AUDITID,
	SECTIONNUM,
	AUDITRESPID,
	QUESTIONID,
	QUESTIONTEXT,
	RESPONSE,
	RESPONSENOTES,
	SCOREGROUPNUM,
	BOXGROUPNUM,
	VIOLATIONPOINTS,
	POINTS,
	POINTSTOTOTAL,
	LOAD_DTTM,
	BATCH_DT
) as
(
    SELECT 'arbys' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SECTIONNUM,
           AUDITRESPID,
           QUESTIONID,
           QUESTIONTEXT,
           RESPONSE,
           RESPONSENOTES,
           SCOREGROUPNUM,
           BOXGROUPNUM,
           VIOLATIONPOINTS,
           POINTS,
           POINTSTOTOTAL,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.ARB.ECOSUREAUDITEXTRACT_RESPONSE

    UNION ALL

    SELECT 'dnkn' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SECTIONNUM,
           AUDITRESPID,
           QUESTIONID,
           QUESTIONTEXT,
           RESPONSE,
           RESPONSENOTES,
           SCOREGROUPNUM,
           BOXGROUPNUM,
           VIOLATIONPOINTS,
           POINTS,
           POINTSTOTOTAL,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.DUN.ECOSUREAUDITEXTRACT_RESPONSE

    UNION ALL

    SELECT 'bww' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SECTIONNUM,
           AUDITRESPID,
           QUESTIONID,
           QUESTIONTEXT,
           RESPONSE,
           RESPONSENOTES,
           SCOREGROUPNUM,
           BOXGROUPNUM,
           VIOLATIONPOINTS,
           POINTS,
           POINTSTOTOTAL,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.BWW.ECOSUREAUDITEXTRACT_RESPONSE

    UNION ALL

    SELECT 'sonic' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SECTIONNUM,
           AUDITRESPID,
           QUESTIONID,
           QUESTIONTEXT,
           RESPONSE,
           RESPONSENOTES,
           SCOREGROUPNUM,
           BOXGROUPNUM,
           VIOLATIONPOINTS,
           POINTS,
           POINTSTOTOTAL,
           LOAD_DTTM,
           AUDIT_DATE AS BATCH_DT
    FROM RDS_DEV.SDI.ECOSUREAUDITEXTRACT_RESPONSE

    UNION ALL

    SELECT BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SECTIONNUM,
           AUDITRESPID,
           QUESTIONID,
           QUESTIONTEXT,
           RESPONSE,
           RESPONSENOTES,
           SCOREGROUPNUM,
           BOXGROUPNUM,
           VIOLATIONPOINTS,
           POINTS,
           POINTSTOTOTAL,
           LOAD_DTTM,
           TO_DATE(
                   REGEXP_SUBSTR(LOAD_FILENAME, '_([0-9]{8,})/', 1, 1, 'e', 1),
                   'yyyyMMdd'
           ) AS BATCH_DT
    FROM RDS_DEV.IRB.ECOSURE_RESPONSE
);
CREATE VIEW IF NOT EXISTS ECOSURE_UNION_SCORE_V(
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	AUDITID,
	SCORENAME,
	SCORE,
	RATING,
	INCOMPLIANCE,
	OUTOFCOMPLIANCE,
	LOAD_DTTM,
	BATCH_DT
) as (

    SELECT 'arbys' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SCORENAME,
           SCORE,
           RATING,
           INCOMPLIANCE,
           OUTOFCOMPLIANCE,
           LOAD_DTTM,
           AUDIT_DATE as BATCH_DT
    FROM RDS_DEV.ARB.ECOSUREAUDITEXTRACT_SCORE

    UNION ALL

    SELECT 'dunkin' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SCORENAME,
           SCORE,
           RATING,
           INCOMPLIANCE,
           OUTOFCOMPLIANCE,
           LOAD_DTTM,
           AUDIT_DATE as BATCH_DT
    FROM RDS_DEV.DUN.ECOSUREAUDITEXTRACT_SCORE

    UNION ALL

    SELECT 'bww' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SCORENAME,
           SCORE,
           RATING,
           INCOMPLIANCE,
           OUTOFCOMPLIANCE,
           LOAD_DTTM,
           AUDIT_DATE as BATCH_DT
    FROM RDS_DEV.BWW.ECOSUREAUDITEXTRACT_SCORE

    UNION ALL

    SELECT 'sonic' as BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SCORENAME,
           SCORE,
           RATING,
           INCOMPLIANCE,
           OUTOFCOMPLIANCE,
           LOAD_DTTM,
           AUDIT_DATE as BATCH_DT
    FROM RDS_DEV.SDI.ECOSUREAUDITEXTRACT_SCORE

    UNION ALL

    SELECT BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID,
           SCORENAME,
           SCORE,
           RATING,
           INCOMPLIANCE,
           OUTOFCOMPLIANCE,
           LOAD_DTTM,
           TO_DATE(
               REGEXP_SUBSTR(LOAD_FILENAME, '_([0-9]{8,})/', 1, 1, 'e', 1),
               'yyyyMMdd'
           )                                              as BATCH_DT
    FROM RDS_DEV.IRB.ECOSURE_SCORE
);
CREATE VIEW IF NOT EXISTS ECOSURE_UNION_SECTIONS_V(
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	AUDITID,
	SECTIONNUM,
	SECTIONNAME,
	NOTE,
	SUBSECTIONFLAG,
	PARENTSECTIONNUM,
	PARENTSECTIONNAME,
	LOAD_DTTM,
	BATCH_DT
) as (

    SELECT 'arbys'                                      AS BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID                                      AS AUDITID,
           NULLIF(TRIM(SECTIONNUM), '')                 AS SECTIONNUM,
           NULLIF(TRIM(SECTIONNAME), '')                AS SECTIONNAME,
           NULLIF(TRIM(NOTE), '')                       AS NOTE,
           NULLIF(TRIM(SUBSECTIONFLAG), '')             AS SUBSECTIONFLAG,
           NULLIF(TRIM(PARENTSECTIONNUM), '')           AS PARENTSECTIONNUM,
           NULLIF(TRIM(PARENTSECTIONNAME), '')          AS PARENTSECTIONNAME,
           LOAD_DTTM                                    AS LOAD_DTTM,
           AUDIT_DATE                                   AS BATCH_DT
    FROM RDS_DEV.ARB.ECOSUREAUDITEXTRACT_SECTIONS

    UNION ALL

    SELECT 'dunkin'                                     AS BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID                                      AS AUDITID,
           NULLIF(TRIM(SECTIONNUM), '')                 AS SECTIONNUM,
           NULLIF(TRIM(SECTIONNAME), '')                AS SECTIONNAME,
           NULLIF(TRIM(NOTE), '')                       AS NOTE,
           NULLIF(TRIM(SUBSECTIONFLAG), '')             AS SUBSECTIONFLAG,
           NULLIF(TRIM(PARENTSECTIONNUM), '')           AS PARENTSECTIONNUM,
           NULLIF(TRIM(PARENTSECTIONNAME), '')          AS PARENTSECTIONNAME,
           LOAD_DTTM                                    AS LOAD_DTTM,
           AUDIT_DATE                                   AS BATCH_DT
    FROM RDS_DEV.DUN.ECOSUREAUDITEXTRACT_SECTIONS

    UNION ALL

    SELECT 'bww'                                        AS BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID                                      AS AUDITID,
           NULLIF(TRIM(SECTIONNUM), '')                 AS SECTIONNUM,
           NULLIF(TRIM(SECTIONNAME), '')                AS SECTIONNAME,
           NULLIF(TRIM(NOTE), '')                       AS NOTE,
           NULLIF(TRIM(SUBSECTIONFLAG), '')             AS SUBSECTIONFLAG,
           NULLIF(TRIM(PARENTSECTIONNUM), '')           AS PARENTSECTIONNUM,
           NULLIF(TRIM(PARENTSECTIONNAME), '')          AS PARENTSECTIONNAME,
           LOAD_DTTM                                    AS LOAD_DTTM,
           AUDIT_DATE                                   AS BATCH_DT
    FROM RDS_DEV.BWW.ECOSUREAUDITEXTRACT_SECTIONS

    UNION ALL

    SELECT 'sonic'                                      AS BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID                                      AS AUDITID,
           NULLIF(TRIM(SECTIONNUM), '')                 AS SECTIONNUM,
           NULLIF(TRIM(SECTIONNAME), '')                AS SECTIONNAME,
           NULLIF(TRIM(NOTE), '')                       AS NOTE,
           NULLIF(TRIM(SUBSECTIONFLAG), '')             AS SUBSECTIONFLAG,
           NULLIF(TRIM(PARENTSECTIONNUM), '')           AS PARENTSECTIONNUM,
           NULLIF(TRIM(PARENTSECTIONNAME), '')          AS PARENTSECTIONNAME,
           LOAD_DTTM                                    AS LOAD_DTTM,
           AUDIT_DATE                                   AS BATCH_DT
    FROM RDS_DEV.SDI.ECOSUREAUDITEXTRACT_SECTIONS

    UNION ALL

    SELECT BRAND_ID                                     AS BRAND_ID,
           'ecosure' as SOURCE_SYSTEM_NAME,
           AUDITID                                      AS AUDITID,
           NULLIF(TRIM(SECTIONNUM), '')                 AS SECTIONNUM,
           NULLIF(TRIM(SECTIONNAME), '')                AS SECTIONNAME,
           NULLIF(TRIM(NOTE), '')                       AS NOTE,
           NULLIF(TRIM(SUBSECTIONFLAG), '')             AS SUBSECTIONFLAG,
           NULLIF(TRIM(PARENTSECTIONNUM), '')           AS PARENTSECTIONNUM,
           NULLIF(TRIM(PARENTSECTIONNAME), '')          AS PARENTSECTIONNAME,
           LOAD_DTTM                                    AS LOAD_DTTM,
           TO_DATE(
               REGEXP_SUBSTR(LOAD_FILENAME, '_([0-9]{8,})/', 1, 1, 'e', 1),
               'yyyyMMdd'
           )                                            AS BATCH_DT
    FROM RDS_DEV.IRB.ECOSURE_SECTIONS
);
CREATE VIEW IF NOT EXISTS IDH_D_CRM_LOYALTY_TRANSACTION_V(
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
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	LOAD_TYPE,
	FILE_NAME
) as
WITH
    cdm AS (
        SELECT
            BRAND_ID AS BRAND_ID,
            CONCAT(TRANSACTION_ID, SOURCE_SYSTEM_NM) as LOYALTY_TRANS_ID,
            CONCAT(MEMBER_ID, '.epsilon') as BRAND_MEMBER_ID,
            ORDER_ID AS ORDER_ID,
            null as ORDER_SOURCE_SYSTEM_NAME,
            TRANSACTION_ID AS SOURCE_LOYALTY_TRANS_ID,
            null as SOURCE_ORDER_ID,
            EMPLOYEE_ID AS EMPLOYEE_ID,
            STORE_ID AS REST_ID,
            EMPLOYEE_NM AS EMPLOYEE_NAME,
            CHECK_NBR AS CHECK_NBR,
            MEMBER_CARD_NBR AS MEMBER_CARD_NBR,
            CAST(DOLLAR_NET_VALUE_AMT AS FLOAT) AS DOLLAR_NET_VALUE,
            CAST(ELIGIBLE_REVENUE_AMT AS FLOAT) AS ELIGIBLE_REV,
            ATTACHMENT_METHOD_TYP AS METHOD_OF_ATTACHMENT,
            TRANSACTION_STATUS_TYP AS TRANS_STATUS,
            MEMBER_PHONE_NBR AS MEMBER_PHONE_NBR,
            REASON_CD AS REASON_CODE,
            MIN_DAY_PART_ID AS MIN_DAY_PART_ID,
            MAX_DAY_PART_ID AS MAX_DAY_PART_ID,
            SUSPENDED_TRANSACTION_ID AS SUSPEND_TRANS_ID,
            SUSPENDED_TRANSACTION_STATUS_IND AS SUSPEND_TRANS_STATUS,
            BUSINESS_DT AS BUSINESS_DATE,
            SOURCE_SYSTEM_NM AS SOURCE_SYSTEM_NAME,
            LOAD_ID AS LOAD_ID,
            LOAD_DTTM AS LOAD_DTTM,
            UPDATE_ID AS UPDATE_ID,
            UPDATE_DTTM AS UPDATE_DTTM
        FROM IDS_DEV.TXN_BV.LOYALTY_ORDER_BV
    ),

    cdm_and_ids AS (
        SELECT * FROM cdm

        UNION ALL

        SELECT * FROM IDS_DEV.CUST.LOYALTY_TRANS
    ),

    final AS (
        SELECT
            SOURCE_LOYALTY_TRANS_ID as TRANS_ID,
            ORDER_ID as ORDER_ID,
            EMPLOYEE_ID as EMPLOYEE_ID,
            BRAND_MEMBER_ID as PROFILE_ID,
            REST_ID as REST_ID,
            BRAND_ID as BRAND_ID,
            BUSINESS_DATE as BUSINESS_DATE,
            EMPLOYEE_NAME as EMPLOYEE_NAME,
            CHECK_NBR as CHECK_NBR,
            MEMBER_CARD_NBR as MEMBER_CARD_NBR,
            DOLLAR_NET_VALUE as DOLLAR_NET_VALUE,
            ELIGIBLE_REV as ELIGIBLE_REVENUE,
            METHOD_OF_ATTACHMENT as METHOD_OF_ATTACHMENT,
            TRANS_STATUS as TRANS_STATUS,
            MEMBER_PHONE_NBR as MEMBER_PHONE_NBR,
            REASON_CODE as REASON_CODE,
            MIN_DAY_PART_ID as MIN_DAY_PART_ID,
            MAX_DAY_PART_ID as MAX_DAY_PART_ID,
            SUSPEND_TRANS_ID as SUSPEND_TRANS_ID,
            SUSPEND_TRANS_STATUS as SUSPEND_TRANS_STATUS,
            CASE
                WHEN SOURCE_SYSTEM_NAME = 'epsilon' THEN 'loyalty'
                ELSE SOURCE_SYSTEM_NAME
            END AS SOURCE_SYSTEM_NM,
            LOAD_ID as LOAD_ID,
            LOAD_DTTM as LOAD_DTTM,
            UPDATE_ID as UPDATE_ID,
            UPDATE_DTTM as UPDATE_DTTM,
            NULL as LOAD_TYPE,
            NULL as FILE_NAME
        FROM cdm_and_ids
    )
SELECT * FROM final;
CREATE VIEW IF NOT EXISTS IDM_COREDIM_CUSTOMER_V(
	BRAND_ID,
	MBR_INSPIRE_ID,
	MBR_ID,
	CLOSEST_STORE_ID,
	HOUSEHOLD_ID,
	EMAIL_ID,
	MOBILE_DEVICE_ID,
	EXPERIAN_ID,
	EXPERIAN_STATUS_TYP,
	INSPIRE_CUST_TYP,
	CUSTOMER_AUTHENTICATION_STATUS,
	MDM_ID,
	MDM_ID_DELETED_IND,
	LOYALTY_CARD_NBR,
	FIRST_NM,
	LAST_NM,
	MIDDLE_INITIAL_TXT,
	DOB_DT,
	BIRTH_MNTH_ID,
	BIRTH_YEAR_ID,
	BIRTH_DT_IMPLIED_IND,
	GENDER_TYP,
	ENROLLMENT_CHANNEL_TYP,
	MBR_STATUS_CD,
	POINT_BALANCE_QTY,
	MEMBERSHIP_STATUS_CD,
	PROFILE_COMPLETED_STATUS_IND,
	DELIVERABILITY_STATUS_IND,
	MOBILE_NBR,
	ADR_LINE_1_TXT,
	ADR_LINE_2_TXT,
	CTY_NM,
	ST_CD,
	ZIP_CD,
	CNTRY_CD,
	EMAIL_OPT_OUT_IND,
	PUSH_NOTIFICATION_OPT_IN_IND,
	SMS_OPT_IN_IND,
	PRIVACY_IND,
	UNSUBSCRIBE_DTTM,
	POINT_EXPIRE_DTTM,
	ENROLL_START_DTTM,
	LAST_LOGIN_DTTM,
	LAST_STATUS_CHANGE_DTTM,
	PROFILE_COMPLETION_DTTM,
	SUBSCRIBER_KEY,
	SUBSCRIBER_SOURCE_NM,
	LOYALTY_TIER_CHANGE_DTTM,
	LOYALTY_TIER_NM,
	LOYALTY_TIER_EXPIRATION_DT,
	LOYALTY_ELITE_VISIT_CNT,
	SOURCE_SYSTEM_NM,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	PUSH_DEVICE_ID,
	ADDRESS_OPT_IN_IND,
	IGNORE_FRAUD_SUSPEND_IND,
	EMAIL_OPT_OUT_STATUS_IND
) as
SELECT
    BC.BRAND_ID AS BRAND_ID,
    BC.BRAND_CUSTOMER_ID AS MBR_INSPIRE_ID,
    BM.BRAND_MEMBER_ID AS MBR_ID,
    BM.CLOSEST_REST_ID AS CLOSEST_STORE_ID,
    NULL AS HOUSEHOLD_ID,
    BCE.STRIPPED_EMAIL_ADDRESS AS EMAIL_ID,
    BCD.CUSTOMER_DEVICE_ID AS MOBILE_DEVICE_ID,
    NULL AS EXPERIAN_ID,
    NULL AS EXPERIAN_STATUS_TYP,
    NULL AS INSPIRE_CUST_TYP,
    BC.CUSTOMER_AUTHENTICATION_STATUS AS CUSTOMER_AUTHENTICATION_STATUS,
NULL AS MDM_ID,
NULL AS MDM_ID_DELETED_IND,
    BM.MEMBER_CARD_NBR AS LOYALTY_CARD_NBR,
    BC.FIRST_NAME AS FIRST_NM,
    BC.LAST_NAME AS LAST_NM,
    BC.MIDDLE_NAME AS MIDDLE_INITIAL_TXT,
    BC.DATE_OF_BIRTH AS DOB_DT,
    BC.BIRTH_MONTH AS BIRTH_MNTH_ID,
    BC.BIRTH_YR AS BIRTH_YEAR_ID,
    BC.BIRTH_IMPLIED_IND AS BIRTH_DT_IMPLIED_IND,
    BC.GENDER AS GENDER_TYP,
    BM.ENROLLMENT_CHANNEL_TYPE AS ENROLLMENT_CHANNEL_TYP,
    BM.MEMBER_STATUS_CODE AS MBR_STATUS_CD,
    BMPB.POINT_BALANCE_QTY AS POINT_BALANCE_QTY,
NULL AS MEMBERSHIP_STATUS_CD,
    BCPS.SURVEY_COMPLETION_STATUS_IND AS PROFILE_COMPLETED_STATUS_IND,
    BCE.EMAIL_DELIVERABILITY_STATUS AS DELIVERABILITY_STATUS_IND,
    BCP.PHONE_NBR AS MOBILE_NBR,
    BCA.ADDRESS_LINE1_TXT AS ADR_LINE_1_TXT,
    BCA.ADDRESS_LINE2_TXT AS ADR_LINE_2_TXT,
    BCA.CITY AS CTY_NM,
    BCA.STATE_CODE AS ST_CD,
    BCA.ZIP_CODE AS ZIP_CD,
    BCA.COUNTRY_CODE AS CNTRY_CD,
    BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_IND,
    NOT BCD.CUSTOMER_DEVICE_PUSH_OPT_OUT_IND AS PUSH_NOTIFICATION_OPT_IN_IND,
    NOT BCD.CUSTOMER_DEVICE_SMS_OPT_OUT_IND AS SMS_OPT_IN_IND,
    BC.PRIVACY_IND AS PRIVACY_IND,
    BM.UNSUBSCRIBE_DTTM AS UNSUBSCRIBE_DTTM,
    BMPB.POINT_EXPIRE_DATE AS POINT_EXPIRE_DTTM,
    BM.ENROLL_START_DATE AS ENROLL_START_DTTM,
    BM.LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
    BM.LAST_STATUS_CHANGE_DTTM AS LAST_STATUS_CHANGE_DTTM,
    BCPS.SURVEY_COMPLETION_DATE AS PROFILE_COMPLETION_DTTM,
    SPLIT_PART(BM.BRAND_CUSTOMER_ID, '.',  0) as SUBSCRIBER_KEY,
NULL AS SUBSCRIBER_SOURCE_NM,
    NULL AS LOYALTY_TIER_CHANGE_DTTM,
    NULL AS LOYALTY_TIER_NM,
    NULL AS LOYALTY_TIER_EXPIRATION_DT,
    NULL AS LOYALTY_ELITE_VISIT_CNT,
    CASE
        WHEN BC.SOURCE_SYSTEM_NAME = 'epsilon' AND BC.BRAND_ID = 'arbys' THEN 'loyalty'
        ELSE BC.SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    NULL AS CDM_LOAD_DT,
    BC.LOAD_ID AS LOAD_ID,
    BC.LOAD_DTTM AS LOAD_DTTM,
    BC.UPDATE_ID AS UPDATE_ID,
    BC.UPDATE_DTTM UPDATE_DTTM,
BCD.DEVICE_ID AS PUSH_DEVICE_ID,
    NOT BCA.CUSTOMER_ADDRESS_OPT_OUT_IND AS ADDRESS_OPT_IN_IND,
    BC.IGNORE_FRAUD_SUSPEND_IND AS IGNORE_FRAUD_SUSPEND_IND,
    CEMSCA.EMAIL_OPT_OUT_STATUS_IND AS EMAIL_OPT_OUT_STATUS_IND
FROM
    IDS_DEV.CUST.BRAND_CUSTOMER BC
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_ADDRESS BCA ON BC.BRAND_CUSTOMER_ID = BCA.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_PHONE BCP ON BC.BRAND_CUSTOMER_ID = BCP.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_EMAIL BCE ON BC.BRAND_CUSTOMER_ID = BCE.BRAND_CUSTOMER_ID
    INNER JOIN IDS_DEV.CUST.BRAND_MEMBER BM ON BC.SOURCE_CUSTOMER_ID = BM.SOURCE_MEMBER_ID AND BC.BRAND_ID = BM.BRAND_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_MEMBER_POINT_BALANCE BMPB ON BM.BRAND_MEMBER_ID = BMPB.BRAND_MEMBER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_DEVICE BCD ON BC.BRAND_CUSTOMER_ID = BCD.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_PROFILE_SURVEY BCPS ON BC.BRAND_CUSTOMER_ID = BCPS.BRAND_CUSTOMER_ID
    LEFT JOIN STG_DEV.ARB.CURRENT_EMAIL_STATUS_FROM_CRM_EMAIL_STATUS_CHANGE_ARBYS AS CEMSCA ON BC.BRAND_CUSTOMER_ID = CEMSCA.BRAND_CUSTOMER_ID
WHERE BC.BRAND_ID = 'arbys'
;
CREATE VIEW IF NOT EXISTS IDM_COREDIM_CUSTOMER_V_TEST(
	BRAND_ID,
	MBR_INSPIRE_ID,
	MBR_ID,
	CLOSEST_STORE_ID,
	HOUSEHOLD_ID,
	EMAIL_ID,
	MOBILE_DEVICE_ID,
	EXPERIAN_ID,
	EXPERIAN_STATUS_TYP,
	INSPIRE_CUST_TYP,
	MDM_ID,
	MDM_ID_DELETED_IND,
	LOYALTY_CARD_NBR,
	FIRST_NM,
	LAST_NM,
	MIDDLE_INITIAL_TXT,
	DOB_DT,
	BIRTH_MNTH_ID,
	BIRTH_YEAR_ID,
	BIRTH_DT_IMPLIED_IND,
	GENDER_TYP,
	ENROLLMENT_CHANNEL_TYP,
	MBR_STATUS_CD,
	POINT_BALANCE_QTY,
	MEMBERSHIP_STATUS_CD,
	PROFILE_COMPLETED_STATUS_IND,
	DELIVERABILITY_STATUS_IND,
	MOBILE_NBR,
	ADR_LINE_1_TXT,
	ADR_LINE_2_TXT,
	CTY_NM,
	ST_CD,
	ZIP_CD,
	CNTRY_CD,
	EMAIL_OPT_OUT_IND,
	PUSH_NOTIFICATION_OPT_IN_IND,
	SMS_OPT_IN_IND,
	PRIVACY_IND,
	UNSUBSCRIBE_DTTM,
	POINT_EXPIRE_DTTM,
	ENROLL_START_DTTM,
	LAST_LOGIN_DTTM,
	LAST_STATUS_CHANGE_DTTM,
	PROFILE_COMPLETION_DTTM,
	SUBSCRIBER_KEY,
	SUBSCRIBER_SOURCE_NM,
	LOYALTY_TIER_CHANGE_DTTM,
	LOYALTY_TIER_NM,
	LOYALTY_TIER_EXPIRATION_DT,
	LOYALTY_ELITE_VISIT_CNT,
	SOURCE_SYSTEM_NM,
	CDM_LOAD_DT,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	PUSH_DEVICE_ID,
	ADDRESS_OPT_IN_IND,
	IGNORE_FRAUD_SUSPEND_IND,
	EMAIL_OPT_OUT_STATUS_IND
) as
SELECT
    BC.BRAND_ID AS BRAND_ID,
    BC.BRAND_CUSTOMER_ID AS MBR_INSPIRE_ID,
    BM.BRAND_MEMBER_ID AS MBR_ID,
    BM.CLOSEST_REST_ID AS CLOSEST_STORE_ID,
    NULL AS HOUSEHOLD_ID,
    BCE.STRIPPED_EMAIL_ADDRESS AS EMAIL_ID,
    BCD.CUSTOMER_DEVICE_ID AS MOBILE_DEVICE_ID,
    NULL AS EXPERIAN_ID,
    NULL AS EXPERIAN_STATUS_TYP,
    BC.CUSTOMER_AUTHENTICATION_STATUS AS INSPIRE_CUST_TYP,
    NULL AS MDM_ID,
    NULL AS MDM_ID_DELETED_IND,
    BM.MEMBER_CARD_NBR AS LOYALTY_CARD_NBR,
    BC.FIRST_NAME AS FIRST_NM,
    BC.LAST_NAME AS LAST_NM,
    BC.MIDDLE_NAME AS MIDDLE_INITIAL_TXT,
    BC.DATE_OF_BIRTH AS DOB_DT,
    BC.BIRTH_MONTH AS BIRTH_MNTH_ID,
    BC.BIRTH_YR AS BIRTH_YEAR_ID,
    BC.BIRTH_IMPLIED_IND AS BIRTH_DT_IMPLIED_IND,
    BC.GENDER AS GENDER_TYP,
    BM.ENROLLMENT_CHANNEL_TYPE AS ENROLLMENT_CHANNEL_TYP,
    BM.MEMBER_STATUS_CODE AS MBR_STATUS_CD,
    BMPB.POINT_BALANCE_QTY AS POINT_BALANCE_QTY,
NULL AS MEMBERSHIP_STATUS_CD,
    BCPS.SURVEY_COMPLETION_STATUS_IND AS PROFILE_COMPLETED_STATUS_IND,
    BCE.EMAIL_DELIVERABILITY_STATUS AS DELIVERABILITY_STATUS_IND,
    BCP.PHONE_NBR AS MOBILE_NBR,
    BCA.ADDRESS_LINE1_TXT AS ADR_LINE_1_TXT,
    BCA.ADDRESS_LINE2_TXT AS ADR_LINE_2_TXT,
    BCA.CITY AS CTY_NM,
    BCA.STATE_CODE AS ST_CD,
    BCA.ZIP_CODE AS ZIP_CD,
    BCA.COUNTRY_CODE AS CNTRY_CD,
    BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_IND,
    NOT BCD.CUSTOMER_DEVICE_PUSH_OPT_OUT_IND AS PUSH_NOTIFICATION_OPT_IN_IND,
    NOT BCD.CUSTOMER_DEVICE_SMS_OPT_OUT_IND AS SMS_OPT_IN_IND,
    BC.PRIVACY_IND AS PRIVACY_IND,
    BM.UNSUBSCRIBE_DTTM AS UNSUBSCRIBE_DTTM,
    BMPB.POINT_EXPIRE_DATE AS POINT_EXPIRE_DTTM,
    BM.ENROLL_START_DATE AS ENROLL_START_DTTM,
    BM.LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
    BM.LAST_STATUS_CHANGE_DTTM AS LAST_STATUS_CHANGE_DTTM,
    BCPS.SURVEY_COMPLETION_DATE AS PROFILE_COMPLETION_DTTM,
    SPLIT_PART(BM.BRAND_CUSTOMER_ID, '.',  0) as SUBSCRIBER_KEY,
NULL AS SUBSCRIBER_SOURCE_NM,
    NULL AS LOYALTY_TIER_CHANGE_DTTM,
    NULL AS LOYALTY_TIER_NM,
    NULL AS LOYALTY_TIER_EXPIRATION_DT,
    NULL AS LOYALTY_ELITE_VISIT_CNT,
    CASE
        WHEN BC.SOURCE_SYSTEM_NAME = 'epsilon' AND BC.BRAND_ID = 'arbys' THEN 'loyalty'
        ELSE BC.SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    NULL AS CDM_LOAD_DT,
    BC.LOAD_ID AS LOAD_ID,
    BC.LOAD_DTTM AS LOAD_DTTM,
    BC.UPDATE_ID AS UPDATE_ID,
    BC.UPDATE_DTTM UPDATE_DTTM,
BCD.DEVICE_ID AS PUSH_DEVICE_ID,
    NOT BCA.CUSTOMER_ADDRESS_OPT_OUT_IND AS ADDRESS_OPT_IN_IND,
    BC.IGNORE_FRAUD_SUSPEND_IND AS IGNORE_FRAUD_SUSPEND_IND,
    BCE.CUSTOMER_EMAIL_OPT_OUT_IND AS EMAIL_OPT_OUT_STATUS_IND
FROM
    IDS_DEV.CUST.BRAND_CUSTOMER BC
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_ADDRESS BCA ON BC.BRAND_CUSTOMER_ID = BCA.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_PHONE BCP ON BC.BRAND_CUSTOMER_ID = BCP.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_EMAIL BCE ON BC.BRAND_CUSTOMER_ID = BCE.BRAND_CUSTOMER_ID
    INNER JOIN IDS_DEV.CUST.BRAND_MEMBER BM 
    ON 
        -- BC.BRAND_CUSTOMER_ID = BM.BRAND_CUSTOMER_ID AND 
        -- BC.SOURCE_CUSTOMER_ID = BM.SOURCE_MEMBER_ID
        BM.BRAND_MEMBER_ID = CONCAT_WS('.', BC.SOURCE_CUSTOMER_ID, BC.SOURCE_SYSTEM_NAME)
    LEFT JOIN IDS_DEV.CUST.BRAND_MEMBER_POINT_BALANCE BMPB ON BM.BRAND_MEMBER_ID = BMPB.BRAND_MEMBER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_DEVICE BCD ON BC.BRAND_CUSTOMER_ID = BCD.BRAND_CUSTOMER_ID
    LEFT JOIN IDS_DEV.CUST.BRAND_CUSTOMER_PROFILE_SURVEY BCPS ON BC.BRAND_CUSTOMER_ID = BCPS.BRAND_CUSTOMER_ID
WHERE BC.BRAND_ID = 'arbys';
CREATE VIEW IF NOT EXISTS IDM_COREDIM_LOYALTY_DISCOUNT_FACT_V(
	BRAND_ID,
	TRX_ID,
	TRX_DISCOUNT_ID,
	SOURCE_ITEM_ID,
	OFFER_CD,
	DISCOUNT_AMT,
	DISCOUNT_DESC,
	CDM_LOAD_DT,
	SOURCE_SYSTEM_NM,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM
) as
SELECT
    BRAND_ID AS BRAND_ID,
    LOYALTY_TRANS_ID AS TRX_ID,
    TRANS_DISC_ID AS TRX_DISCOUNT_ID,
    SOURCE_ITEM_ID AS SOURCE_ITEM_ID,
    OFFER_CODE AS OFFER_CD,
    DISC_AMT AS DISCOUNT_AMT,
    DISC_DESC AS DISCOUNT_DESC,
    NULL AS CDM_LOAD_DT,
    CASE
        WHEN SOURCE_SYSTEM_NAME = 'epsilon' THEN 'loyalty'
        ELSE SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    LOAD_ID AS LOAD_ID,
    LOAD_DTTM AS LOAD_DTTM,
    UPDATE_ID AS UPDATE_ID,
    UPDATE_DTTM AS UPDATE_DTTM
FROM
    IDS_DEV.CUST.LOYALTY_DISC;
CREATE VIEW IF NOT EXISTS IDM_COREDIM_LOYALTY_MBR_OFFER_V(
	BRAND_ID,
	CDM_LOAD_DT,
	CREATE_DTTM,
	EXPIRATION_DTTM,
	LOAD_DTTM,
	LOAD_ID,
	MBR_ID,
	MBR_OFFER_ID,
	OFFER_CD,
	OPT_IN_DTTM,
	PRIVACY_IND,
	SOURCE_LOADED_DTTM,
	SOURCE_MODIFIED_DTTM,
	SOURCE_SYSTEM_NM,
	UPDATE_DTTM,
	UPDATE_ID
) as
    SELECT
    BRAND_ID AS BRAND_ID,
	NULL AS CDM_LOAD_DT,
	CREATE_DTTM AS CREATE_DTTM,
	EXPIRATION_DTTM AS EXPIRATION_DTTM,
	LOAD_DTTM AS LOAD_DTTM,
    LOAD_ID AS LOAD_ID,
	BRAND_MEMBER_ID AS MBR_ID,
    MBR_OFFER_ID AS MBR_OFFER_ID,
	OFFER_CODE AS OFFER_CD,
	OPT_IN_DTTM AS OPT_IN_DTTM,
	'FALSE' AS PRIVACY_IND,
    SOURCE_LOADED_DTTM AS SOURCE_LOADED_DTTM,
	SOURCE_MODIFIED_DTTM AS SOURCE_MODIFIED_DTTM,
	CASE 
		WHEN SOURCE_SYSTEM_NAME = 'epsilon' THEN 'loyalty'
		ELSE SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    UPDATE_DTTM AS UPDATE_DTTM,
    UPDATE_ID AS UPDATE_ID
FROM
    IDS_DEV.CUST.OMS_MEMBER_OFFER;
CREATE VIEW IF NOT EXISTS IDM_COREDIM_LOYALTY_OFFER_V(
	BRAND_ID,
	OFFER_CD,
	UNIQUE_OFFER_ID,
	VARIANT_ID,
	OFFER_SEQ_KEY,
	OFFER_NM,
	OFFER_REPORT_DESC,
	OFFER_SHORT_DESC,
	OFFER_DESC,
	OPT_IN_REQUIRED_IND,
	LOC_NM,
	EXCLUSION_TXT,
	OFFER_IMG_NM,
	TERM_AND_CONDITION_TXT,
	MOMENT_ELIGIBILITY_TXT,
	STANDARD_EXCLUSION_TXT,
	OTHER_EXCLUSION_TXT,
	POINT_AWARDED_QTY,
	LIMIT_NBR,
	STORE_LOC_LIST_NBR,
	DAY_PART_LIST_NBR,
	MBR_LIST_NEEDED_IND,
	STORE_TYP,
	ONE_TM_OFFER_IND,
	TRIVIA_NEEDED_IND,
	CHECK_IN_IND,
	OFFER_PRIORITY_NBR,
	OFFER_TYP,
	AWARD_NM,
	PRODUCT_PLUS_DESC,
	TRIVIA_QUESTION_IND,
	TRIVIA_ANSWER_IND,
	TARGET_OFFER_IND,
	PRIVACY_IND,
	VALIDITY_PERIOD_NBR,
	VALIDITY_UNIT_DT_NM,
	DNA_INCENTIVE_TYP,
	DNA_CAMPAIGN_TYP,
	DNA_DISCOUNT_PRODUCT_NM,
	DNA_PRODUCT_CATEGORY_NM,
	DNA_ACTION_REQUIRED_TXT,
	DNA_MINIMUM_SPEND_AMT,
	DNA_OFFER_WEEK_IN_CAMPAIGN_NBR,
	OFFER_VISIBLE_DTTM,
	OFFER_START_DTTM,
	OFFER_END_DTTM,
	DNA_INCENTIVE_AMT,
	SYSTEM_OFFER_ID,
	DNA_DISCOUNT_PRODUCT_LEVEL_CD,
	DNA_OFFER_REDEMPTION_CHANNEL_TYP,
	STATUS_CD,
	OFFER_IMAGE2_NM,
	SOURCE_APPLICATION_TYP,
	PARENT_OFFER_ID,
	SOURCE_SYSTEM_NM,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	OFFER_TAG,
	CDM_LOAD_DT
) as
SELECT
    BRAND_ID AS BRAND_ID,
    OFFER_CODE AS OFFER_CD,
    UNIQ_OFFER_ID AS UNIQUE_OFFER_ID,
    VARIANT_ID AS VARIANT_ID,
    OFFER_SEQ_KEY AS OFFER_SEQ_KEY,
    OFFER_NAME AS OFFER_NM,
    OFFER_REPORT_DESC AS OFFER_REPORT_DESC,
    OFFER_SHORT_DESC AS OFFER_SHORT_DESC,
    OFFER_DESC AS OFFER_DESC,
    OPTIN_REQUIRED_IND AS OPT_IN_REQUIRED_IND,
    LOCATION_NAME AS LOC_NM,
    EXCLUSIONS_TXT AS EXCLUSION_TXT,
    OFFER_IMAGE_NAME AS OFFER_IMG_NM,
    TERMS_AND_CONDITIONS AS TERM_AND_CONDITION_TXT,
    MOMENT_ELIGIBILITY AS MOMENT_ELIGIBILITY_TXT,
    STANDARD_EXCLUSIONS_TXT AS STANDARD_EXCLUSION_TXT,
    OTHER_EXCLUSIONS AS OTHER_EXCLUSION_TXT,
    POINTS_AWARDED_NBR AS POINT_AWARDED_QTY,
    LIMITS_NBR AS LIMIT_NBR,
    REST_LOCATION_NBR AS STORE_LOC_LIST_NBR,
    DAYPART AS DAY_PART_LIST_NBR,
    MEMBER_LIST_NEEDED AS MBR_LIST_NEEDED_IND,
    REST_TYPE AS STORE_TYP,
    ONE_TIME_OFFER_IND AS ONE_TM_OFFER_IND,
    TRIVIA_NEEDED_IND AS TRIVIA_NEEDED_IND,
    CHECK_IN_IND AS CHECK_IN_IND,
    OFFER_PRIORITY_NBR AS OFFER_PRIORITY_NBR,
    OFFER_TYPE AS OFFER_TYP,
    AWARD_NAME AS AWARD_NM,
    PRODUCT_PLU_DESC AS PRODUCT_PLUS_DESC,
    NULL AS TRIVIA_QUESTION_IND,
    NULL AS TRIVIA_ANSWER_IND,
    TARGET_OFFER_IND AS TARGET_OFFER_IND,
    FALSE AS PRIVACY_IND,
    VALIDITY_PERIOD AS VALIDITY_PERIOD_NBR,
    VALIDITY_UNIT_DATE_NAME AS VALIDITY_UNIT_DT_NM,
    DNA_INCENTIVE_TYPE AS DNA_INCENTIVE_TYP,
    DNA_CAMPAIGN_TYPE AS DNA_CAMPAIGN_TYP,
    DNA_DISCOUNT_PRODUCT_NAME AS DNA_DISCOUNT_PRODUCT_NM,
    DNA_PRODUCT_CATEGORY AS DNA_PRODUCT_CATEGORY_NM,
    DNA_ACTION_REQUIRED AS DNA_ACTION_REQUIRED_TXT,
    DNA_MINIMUM_SPEND_AMT AS DNA_MINIMUM_SPEND_AMT,
    DNA_OFFER_WEEK_IN_CAMPAIGN AS DNA_OFFER_WEEK_IN_CAMPAIGN_NBR,
    OFFER_VISIBLE_DTTM AS OFFER_VISIBLE_DTTM,
    OFFER_START_DTTM AS OFFER_START_DTTM,
    OFFER_END_DTTM AS OFFER_END_DTTM,
    DNA_INCENTIVE_VAL AS DNA_INCENTIVE_AMT,
    SYSTEM_OFFER_ID AS SYSTEM_OFFER_ID,
    DNA_DISCOUNT_PRODUCT_L_CODE AS DNA_DISCOUNT_PRODUCT_LEVEL_CD,
    DNA_OFFER_REDEMPTION_CHANNEL AS DNA_OFFER_REDEMPTION_CHANNEL_TYP,
    STATUS_CODE AS STATUS_CD,
    OFFER_IMAGE2_NAME AS OFFER_IMAGE2_NM,
    SOURCE_APPLICATION_TYPE AS SOURCE_APPLICATION_TYP,
    PARENT_OFFER_ID AS PARENT_OFFER_ID,
    CASE
        WHEN SOURCE_SYSTEM_NAME = 'epsilon' THEN 'loyalty'
        ELSE SOURCE_SYSTEM_NAME
    END AS SOURCE_SYSTEM_NM,
    LOAD_ID AS LOAD_ID,
    TO_TIMESTAMP(LOAD_DTTM) AS LOAD_DTTM,
    UPDATE_ID AS UPDATE_ID,
    UPDATE_DTTM AS UPDATE_DTTM,
    OFFER_TAG AS OFFER_TAG,
    NULL AS CDM_LOAD_DT
FROM IDS_DEV.CUST.OMS_OFFER;
CREATE VIEW IF NOT EXISTS OPENWEATHER_FORECAST_5DAY_FLAT_V(
	URL,
	EXPLODED_RECORD,
	DT_TXT,
	CITY_ID,
	TIMESTAMP,
	EPOCH,
	BRAND_ID,
	SOURCE_SYSTEM_NAME,
	LOAD_ID,
	LOAD_DTTM,
	UPDATE_ID,
	UPDATE_DTTM,
	LOAD_FILENAME
) as SELECT
    URL AS URL,
    STG_DEV.IRB.MERGE_JSON({'list_item': response_list.value}::variant, {'city': response:city}::variant) as EXPLODED_RECORD,
    response_list.value:dt_txt as DT_TXT, --Business Key (from exploded record)
    response:city:id::number AS CITY_ID, --Business Key
    TIMESTAMP AS TIMESTAMP,
    EPOCH AS EPOCH,

    BRAND_ID AS BRAND_ID,
    SOURCE_SYSTEM_NAME AS SOURCE_SYSTEM_NAME,
    LOAD_ID AS LOAD_ID,
    LOAD_DTTM AS LOAD_DTTM,
    UPDATE_ID AS UPDATE_ID,
    UPDATE_DTTM AS UPDATE_DTTM,
    LOAD_FILENAME AS LOAD_FILENAME
FROM STG_DEV.IRB.OPENWEATHER_FORECAST_5DAY_RAW,
LATERAL FLATTEN(input => response:list) AS response_list
WHERE SPLIT_PART(LOAD_FILENAME, '/', -2) = 
    (SELECT MAX(SPLIT_PART(LOAD_FILENAME, '/', -2)) FROM STG_DEV.IRB.OPENWEATHER_FORECAST_5DAY_RAW);
CREATE FILE FORMAT IF NOT EXISTS CIP_TEST
	FIELD_DELIMITER = '|'
	FILE_EXTENSION = 'dat'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS CIP_UC1_LOYALTY_TO_EPSILON
	FIELD_DELIMITER = '|'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS CIP_UC2_IDENTITY_EPSILON_RESPONSE
	FIELD_DELIMITER = '|'
	FILE_EXTENSION = 'dat'
	ESCAPE = '\\'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS CIP_UC2_LOYALTY_TO_EPSILON
	FIELD_DELIMITER = '|'
	FILE_EXTENSION = 'dat'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS DEV_CIP_UC2_LOYALTY_FROM_EPSILON_FILE_FORMAT
	FIELD_DELIMITER = '|'
	ESCAPE_UNENCLOSED_FIELD = 'NONE'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS DEV_CIP_UC2_LOYALTY_TO_EPSILON_FILE_FORMAT
	FIELD_DELIMITER = '|'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS LOYALTY_SEND_TO_EPSILON_FILE_FORMAT
	FIELD_DELIMITER = '|'
	FILE_EXTENSION = 'dat'
	ESCAPE = '\\'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS LOYALTY_SEND_TO_TEST
	FIELD_DELIMITER = '|'
	FILE_EXTENSION = 'dat'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS RM_AVRO_FILE_FORMAT
	TYPE = AVRO
	NULL_IF = ()
;
CREATE FILE FORMAT IF NOT EXISTS RM_BIG_JSON_FORMAT
	TYPE = json
	NULL_IF = ()
	COMPRESSION = none
	ALLOW_DUPLICATE = TRUE
	STRIP_OUTER_ARRAY = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_COMMA_DELIMITED_QUOTED_ESCAPED_CSV_FILE_FORMAT
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_COMMA_DELIM_QUOTED_CSV_FILE_FORMAT
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_CSV_FILE_FORMAT
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_JSON_FORMAT
	TYPE = json
	NULL_IF = ()
;
CREATE FILE FORMAT IF NOT EXISTS RM_PIPE_DELIMITED_QUOTED_CSV_ESCAPED_FILE_FORMAT
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_PIPE_DELIMITED_QUOTED_CSV_FILE_FORMAT
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_PIPE_DELIMITED_QUOTED_CSV_FILE_FORMAT_TEST_VP
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_PIPE_DELIMITED_QUOTED_ESCAPED_CSV_FILE_FORMAT
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_PIPE_DELIMITED_QUOTED_ESCAPED_CSV_FILE_FORMAT_TEMP
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_PIPE_DELIMITED_QUOTED_ESCAPED_CSV_FILE_FORMAT_TEST_VP
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	ESCAPE = '\\'
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS RM_TAB_DELIMITED_CSV_FILE_FORMAT
	FIELD_DELIMITER = '\t'
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS TEST
	FIELD_DELIMITER = '|'
	ESCAPE = '\\'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS TEST_20230821_FISCAR_COMPETITOR_TEST_FORMAT
	FIELD_DELIMITER = '\t'
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	SKIP_BLANK_LINES = TRUE
	REPLACE_INVALID_CHARACTERS = TRUE
;
CREATE FILE FORMAT IF NOT EXISTS TEST_CATCH_UP
	FIELD_DELIMITER = '|'
	ESCAPE_UNENCLOSED_FIELD = 'NONE'
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = NONE
;
CREATE FILE FORMAT IF NOT EXISTS US_ECONOMIC_DATA
	TYPE = csv
	SKIP_HEADER = 1
	TRIM_SPACE = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
;
CREATE PROCEDURE IF NOT EXISTS "EXECUTE_DYNAMIC_QUERY"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var sql_statement = `
    SELECT ''SELECT * FROM TEST WHERE '' || 
           LISTAGG(
               CASE 
                   WHEN DATA_TYPE = ''TEXT'' AND COLUMN_NAME NOT IN (''A'', ''B'', ''C'', ''C'')  THEN COLUMN_NAME || '' = '''''''' ''
                   ELSE NULL 
               END, 
               '' OR '') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS dynamic_query
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ''TEST'';
  `;
  
  var stmt = snowflake.createStatement({
    sqlText: sql_statement
  });
  
  var rs = stmt.execute();
  
  var dynamic_sql = ''''; // Initialize a variable to store the dynamically generated SQL
  
  // Loop through the result set and extract the dynamically generated SQL
  while (rs.next()) {
    dynamic_sql = rs.getColumnValue(1); // Assuming the dynamic query is in the first column
  }
  
  // Execute the dynamically generated SQL
  var dynamic_stmt = snowflake.createStatement({
    sqlText: dynamic_sql
  });
  
  var result_set = dynamic_stmt.execute(); // Execute the dynamically generated SQL
  
  var result = ''''; // Initialize an empty string to store the result set
  
  // Loop through the result set and concatenate rows into the result string
  while (result_set.next()) {
    result += JSON.stringify(result_set) + ''\\n''; // Adjust as per your requirements
  }
  
  return result; // Return the concatenated result string
';
CREATE PROCEDURE IF NOT EXISTS "LOC_DMA_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES_DELTA"("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var insert_stream_data = `INSERT INTO `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`."loc_dma_plr"(
                                DMACODE, 
DMASOURCENAME, 
DMANAME, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
FILENAME
                              )
                          SELECT
                                delta_stream.DMACODE, 
delta_stream.DMASOURCENAME, 
delta_stream.DMANAME, 
nullif(STG_DEV.CDMSYNC.decode_partition(delta_stream.BRANDID), ''__HIVE_DEFAULT_PARTITION__''), 
delta_stream.SOURCE, 
nullif(STG_DEV.CDMSYNC.decode_partition(delta_stream.CDMLOADDATE), ''__HIVE_DEFAULT_PARTITION__''), 
delta_stream.FILENAME
                          FROM `
                              + SRC_DB_PARAM+`.`+SRC_SCHEMA_PARAM+`.loc_dma_plr_delta delta_stream
                          WHERE EXISTS(
                                        select
                                           stream_add.add_file
                                        from
                                           (
                                              select
                                                 log_stream.add_file    
                                              from `
                                                 + SRC_DB_PARAM+`.`+SRC_SCHEMA_PARAM+`.loc_dma_plr_delta_log log_stream 
                                              where log_stream.add_file is not null 
                                                  and not exists (
                                                          select 
                                                             distinct filename 
                                                          from `
                                                            + DST_DB_PARAM+`.`+DST_SCHEMA_PARAM+`.loc_dma_plr target 
                                                          where 
                                                            target.filename = log_stream.add_file
                                                  ) 
                                           ) stream_add 
                                             where        
                                           stream_add.add_file = delta_stream.filename )`;

    var delete_json_data = `DELETE FROM ` + DST_DB_PARAM+`.`+DST_SCHEMA_PARAM+`."loc_dma_plr" target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `
                                            + SRC_DB_PARAM+`.`+SRC_SCHEMA_PARAM+`.loc_dma_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL
                            )`;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_stream_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_stream_data
                }
        );
        insert_stream_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';
CREATE FUNCTION IF NOT EXISTS "MERGE_JSON"("OBJ1" VARIANT, "OBJ2" VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS '
                          return x = Object.assign(OBJ1, OBJ2);
                          ';
CREATE FUNCTION IF NOT EXISTS "REGEX_PY"("WORD" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'regex_py'
AS '
import re
def regex_py(word):
  result = re.fullmatch(''[\\w\\'']+'', word)
  return result
';
CREATE STREAM IF NOT EXISTS RESTAURANT_GROUP_EVENT_STREAM on table RESTAURANT_GROUP_EVENT;
CREATE STREAM IF NOT EXISTS TEST_TAGS_STREAM on table TEST_TAGS_TABLE append_only = true;
CREATE PIPE IF NOT EXISTS RESTAURANT_GROUP_SNOWPIPE auto_ingest=true integration='UDP_QUEUE_INT_DEV' as COPY INTO STG_DEV.IRB.RESTAURANT_GROUP_EVENT
(
  SequenceNumber,
  Offset,
  EnqueuedTimeUtc,
  SystemProperties,
  Properties,
  Body,
  SOURCE_SYSTEM_NAME,
  LOAD_ID,
  LOAD_DTTM
)
FROM (SELECT
    $1:SequenceNumber as SequenceNumber,
    $1:Offset as Offset,
    $1:EnqueuedTimeUtc as EnqueuedTimeUtc,
    $1:SystemProperties as SystemProperties,
    $1:Properties as Properties,
    to_variant(parse_json(to_char($1:Body::binary,'utf-8'))) as Body,
    'price_portal' as SOURCE_SYSTEM_NAME,
    cast(to_varchar(SYSDATE(),'yyyymmddhh24missFF3') as NUMBER(38,0)),
    cast(SYSDATE() as TIMESTAMP_NTZ(9))
  FROM @STG_DEV.IRB.RESTAURANT_GROUP_EVENT_STAGE
)
FILE_FORMAT = STG_DEV.IRB.RM_AVRO_FILE_FORMAT
pattern = '.*\.avro';
CREATE TASK IF NOT EXISTS TEST_TAGS_TASK
	warehouse=RA_UA_WH
	schedule='1 minute'
	when system$Stream_has_data('stg_dev.irb.test_tags_stream')
	as INSERT INTO stg_dev.irb.test_tags_table_dest(first,second,third) SELECT * from stg_dev.irb.test_tags_stream;