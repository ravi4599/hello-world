USE WAREHOUSE {{ params.warehouse }};

COPY INTO RDS_{{params.env}}.{{params.schema}}.PAID_IN_OUT
FROM 	(
  SELECT
	$1:: STRING AS  InvoiceNumber	              ,
	$2:: STRING AS  RestaurantNumber	          ,
	$3:: STRING AS  BusinessDate	              ,
	$4:: STRING AS  PaidInOutTypeId	              ,
	$5:: STRING AS  PaidInOutTypeDescription	  ,
	$6:: STRING AS  Description	                  ,
	$7:: STRING AS  GLAccountId	                  ,
	$8:: STRING AS  Amount                        ,   
    to_timestamp_ntz(current_timestamp)::STRING AS LOADDATETIME,
    to_number(to_char(current_date, 'YYYYMMDD'))::STRING AS LOADID,
    LOWER('incremental') AS LOADTYPE,
    LOWER('arbys') || '/' || METADATA$FILENAME::STRING AS FILENAME,
    LEFT(RIGHT(METADATA$FILENAME,18),8) AS FILEDATE,
    LEFT(RIGHT(METADATA$FILENAME,48),8) AS FOLDERDATE
    FROM @RDS_{{params.env}}.{{params.schema}}.ODI_READ_CSV_FROM_ADLS_TWO/{{params.run_date}}/ 
    (file_format => '{{params.file_format}}', pattern => '{{params.piopattern}}')
);
