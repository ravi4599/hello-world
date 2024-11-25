USE WAREHOUSE {{ params.warehouse }};

COPY INTO RDS_{{params.env}}.{{params.schema}}.CASH_VARIANCE
FROM 	(
  SELECT
	$1:: STRING AS  RestaurantNumber,
	$2:: STRING AS  BusinessDate,
	$3:: STRING AS  Description,
	$4:: STRING AS  CashAmount,
	to_timestamp_ntz(current_timestamp)::STRING AS LOADDATETIME,
    to_number(to_char(current_date, 'YYYYMMDD'))::STRING AS LOADID,
    LOWER('incremental') AS LOADTYPE,
    LOWER('arbys') || '/' || METADATA$FILENAME::STRING AS FILENAME,
    LEFT(RIGHT(METADATA$FILENAME,18),8) AS FILEDATE,
    LEFT(RIGHT(METADATA$FILENAME,45),8) AS FOLDERDATE
    FROM @RDS_{{params.env}}.{{params.schema}}.ODI_READ_CSV_FROM_ADLS_TWO/{{params.run_date}}/ 
    (file_format => '{{params.file_format}}', pattern => '{{params.cvpattern}}')
);
