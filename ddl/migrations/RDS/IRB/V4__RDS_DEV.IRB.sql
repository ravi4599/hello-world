
--------------------------------------------

-- create stage for publicis dunkin csv files for incremental
CREATE STAGE IF NOT EXISTS INC_IRB_PUBLICIS_DUNKIN_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/publicis/incremental/dunkin/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = MO_PUBLICIS_CSV_FORMAT;

-- create stage for publicis dunkin csv files for historical
CREATE STAGE IF NOT EXISTS HIST_IRB_PUBLICIS_DUNKIN_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/publicis/historical/dunkin/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = MO_PUBLICIS_CSV_FORMAT;

-- create stage for publicis sonic csv files for incremental
CREATE STAGE IF NOT EXISTS INC_IRB_PUBLICIS_SONIC_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/publicis/incremental/sonic/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = MO_PUBLICIS_CSV_FORMAT;

-- create stage for publicis sonic csv files for historical
CREATE STAGE IF NOT EXISTS HIST_IRB_PUBLICIS_SONIC_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/publicis/historical/sonic/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = MO_PUBLICIS_CSV_FORMAT;