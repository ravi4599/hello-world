-- create stage for Arbys files

CREATE STAGE IF NOT EXISTS "RDS_PROD"."ARB".SALES_RECON_READ_ADLS2
	STORAGE_INTEGRATION = UDP_STORAGE_INT_PROD
	URL = 'azure://ibue2prod01udpadls2.blob.core.windows.net/arbys/landing/ftp/SalesRecon/';

CREATE STAGE IF NOT EXISTS "RDS_QA"."ARB".ITEM_XML_READ_ADLS2
	STORAGE_INTEGRATION = UDP_STORAGE_INT_QA
	URL = 'azure://ibue2qa01udpadls2.blob.core.windows.net/arbys/staging/domain/Agilence/transaction/settings/GetItems/';

CREATE STAGE IF NOT EXISTS "RDS_QA"."ARB"."REVENUE_CENTER_XML_READ_ADLS2"
	STORAGE_INTEGRATION = UDP_STORAGE_INT_QA
	URL = 'azure://ibue2qa01udpadls2.blob.core.windows.net/arbys/staging/domain/Agilence/transaction/settings/GetRevenueCenters/';
