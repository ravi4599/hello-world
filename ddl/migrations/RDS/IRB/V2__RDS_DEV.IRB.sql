
-- create table for nielsen all brands
CREATE TABLE IF NOT EXISTS MO_NIELSEN_TRADITIONAL
(
AirDetectedEvent       VARCHAR(16777216),
Product                VARCHAR(16777216),
"Week Of"              VARCHAR(16777216),
"Air Date"             VARCHAR(16777216),
"Market Code"          VARCHAR(16777216),
"Market Rank"          VARCHAR(16777216),
Market                 VARCHAR(16777216),
Network                VARCHAR(16777216),
Station                VARCHAR(16777216),
"Media Type"           VARCHAR(16777216),
"Type of Demographic"  VARCHAR(16777216),
Demographic            VARCHAR(16777216),
"Data Stream"          VARCHAR(16777216),
"Air ISCI"             VARCHAR(16777216),
"Cmml Title"           VARCHAR(16777216),
"Allocated Impression" VARCHAR(16777216),
"Allocated Rating"     VARCHAR(16777216),
"DMA UE"               VARCHAR(16777216),
"Rtg Source"           VARCHAR(16777216),
Clearance              VARCHAR(16777216),
BRAND                  VARCHAR(16777216),
LOADDATETIME           VARCHAR(16777216),
LOADID                 VARCHAR(16777216),
LOADTYPE               VARCHAR(16777216),
FILENAME               VARCHAR(16777216)
);

--------------------------------------------

-- Create csv file format in rds_dev for nielsen all brands
CREATE FILE FORMAT IF NOT EXISTS MO_NIELSEN_CSV_FORMAT
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
TRIM_SPACE = FALSE
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
ESCAPE = 'NONE'
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'AUTO'
NULL_IF = ('\\N');

-- create stage for nielsen dunkin csv files for incremental
CREATE STAGE IF NOT EXISTS INC_IRB_NIELSEN_DUNKIN_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/nielsen/incremental/dunkin/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = RDS_DEV.IRB.MO_NIELSEN_CSV_FORMAT;

-- create stage for nielsen dunkin csv files for historical
CREATE STAGE IF NOT EXISTS HIST_IRB_NIELSEN_DUNKIN_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/nielsen/historical/dunkin/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = RDS_DEV.IRB.MO_NIELSEN_CSV_FORMAT;

-- create stage for nielsen sonic csv files for incremental
CREATE STAGE IF NOT EXISTS INC_IRB_NIELSEN_SONIC_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/nielsen/incremental/sonic/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = RDS_DEV.IRB.MO_NIELSEN_CSV_FORMAT;

-- create stage for nielsen sonic csv files for historical
CREATE STAGE IF NOT EXISTS HIST_IRB_NIELSEN_SONIC_SFTP_CSV_TO_ADLS2
URL = 'azure://ibue2dev01udpadls2.blob.core.windows.net/irb/staging/ftp/nielsen/historical/sonic/'
STORAGE_INTEGRATION = UDP_STORAGE_INT_DEV
FILE_FORMAT = RDS_DEV.IRB.MO_NIELSEN_CSV_FORMAT;