USE WAREHOUSE {{ params.warehouse }};

COPY INTO RDS_{{params.env}}.{{params.schema}}.BOS_MICROS_SALES
FROM 	(
SELECT
         $1:: STRING AS RAW_DATA,
         to_timestamp_ntz(current_timestamp)::STRING AS LOADDATETIME,
         to_number(to_char(current_date, 'YYYYMMDD'))::STRING AS LOADID,
         LOWER('incremental') AS LOADTYPE,
         LOWER('sdi') || '/' || METADATA$FILENAME::STRING AS FILENAME,
         TO_DATE(LEFT(RIGHT(METADATA$FILENAME,12),8),'YYYYMMDD') AS FILEDATE,
         TO_DATE(RIGHT(LEFT(METADATA$FILENAME,27),8),'YYYYMMDD') AS FOLDERDATE
FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_SDI_SFTP_DATA_TO_ADLS2/{{params.from_date}}/
(file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
);