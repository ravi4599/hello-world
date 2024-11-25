--- Arbys-----
CREATE STAGE IF NOT EXISTS "RDS_DEV"."ARB".SALES_RECON_READ_ADLS2
	STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
	URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/arbys/landing/ftp/SalesRecon/';

CREATE STAGE IF NOT EXISTS "RDS_DEV"."ARB"."ODI_READ_CSV_FROM_ADLS_TWO" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV 
    URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/polaris-app-integration/Altametrics/DEV/Arbys/inputs/Sales/';

CREATE STAGE IF NOT EXISTS "RDS_DEV"."ARB"."REVENUE_CENTER_XML_READ_ADLS2"
	STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
	URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/arbys/staging/domain/Agilence/transaction/settings/GetRevenueCenters/';

CREATE STAGE IF NOT EXISTS "RDS_DEV"."ARB".ITEM_XML_READ_ADLS2
	STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
	URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/arbys/staging/domain/Agilence/transaction/settings/GetItems/';

---BWW OLD---
CREATE STAGE IF NOT EXISTS "RDS_DEV"."BWW"."SALES_RECON_READ_BWW_ADLS2" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV 
    URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/bww/landing/ftp/SalesRecon/';

---BWW--- 
CREATE STAGE IF NOT EXISTS "RDS_DEV"."BWW"."SALES_RECON_READ_BWW_ADLS2_NEW" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV 
    URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/bww/landing/db/Insight/Corp/dpvHstGndItem/';

CREATE STAGE IF NOT EXISTS "RDS_DEV"."BWW"."SALES_RECON_BWW_NBO_CSV_ADLS2" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV 
    URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/bww/landing/db/Sales/';

---BWW Polled_Sales---
CREATE STAGE IF NOT EXISTS "IDH_DEV"."D_TRANS"."stage_for_bww_polled_sales"
  STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV 
  URL='azure://ibue2dev01udpadls2.blob.core.windows.net/bww/outbound/polled_sales'
  
-----Sonic-----
CREATE STAGE IF NOT EXISTS "RDS_DEV"."SDI"."SALES_RECON_SDI_SFTP_DATA_TO_ADLS2" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV 
    URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/sonic/staging/remote_dir/';
  

CREATE STAGE IF NOT EXISTS "RDS_DEV"."SDI"."SALES_RECON_READ_SDI_ADLS2" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV 
    URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/sonic/landing/ftp/SalesRecon/';

CREATE STAGE IF NOT EXISTS "IDH_DEV"."D_TRANS"."stage_for_sonic_polled_sales"
    STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
    URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/sonic/outbound/polled_sales';   
