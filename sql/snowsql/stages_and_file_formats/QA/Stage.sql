---BWW--- 
CREATE STAGE IF NOT EXISTS "RDS_QA"."BWW"."SALES_RECON_READ_BWW_ADLS2_NEW" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_QA
    URL = 'azure://ibue2qa01udpadls2.blob.core.windows.net/bww/landing/db/Insight/Corp/dpvHstGndItem/';


CREATE STAGE IF NOT EXISTS "RDS_QA"."BWW"."SALES_RECON_BWW_NBO_CSV_ADLS2" 
    STORAGE_INTEGRATION = UDP_STORAGE_INT_QA
    URL = 'azure://ibue2qa01udpadls2.blob.core.windows.net/bww/landing/db/Sales/';

---BWW Polled_Sales---
CREATE STAGE IF NOT EXISTS "IDH_QA"."D_TRANS"."stage_for_bww_polled_sales"
  STORAGE_INTEGRATION = UDP_STORAGE_INT_QA
  URL='azure://ibue2qa01udpadls2.blob.core.windows.net/bww/outbound/polled_sales'

---SONIC---
CREATE STAGE IF NOT EXISTS "IDH_QA"."D_TRANS"."stage_for_sonic_polled_sales"
    STORAGE_INTEGRATION = UDP_STORAGE_INT_QA
    URL = 'azure://ibue2qa01udpadls2.blob.core.windows.net/sonic/outbound/polled_sales';
