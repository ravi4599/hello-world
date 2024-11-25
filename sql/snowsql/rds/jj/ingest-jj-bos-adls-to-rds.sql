USE WAREHOUSE {{ params.warehouse }};

COPY INTO RDS_{{params.env}}.{{params.schema}}.BOS_ALTAMETRICS_SALES
FROM 	(
SELECT
$1:: STRING AS   "Store ID",
$2:: STRING AS   "Category ID",
$3:: STRING AS   Amount,
$4:: STRING AS   "Sales Date",
to_timestamp_ntz(current_timestamp)::STRING AS LOADDATETIME,
to_number(to_char(current_date, 'YYYYMMDD'))::STRING AS LOADID,
LOWER('incremental') AS LOADTYPE,
LOWER('jj') || '/' || METADATA$FILENAME::STRING AS FILENAME,
TO_DATE(LEFT(RIGHT(METADATA$FILENAME,19),8),'YYYYMMDD') AS FILEDATE,
TO_DATE(RIGHT(LEFT(METADATA$FILENAME,50),8),'YYYYMMDD') AS FOLDERDATE
FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_JJ_CSV_ADLS2/BOS/
(file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
);