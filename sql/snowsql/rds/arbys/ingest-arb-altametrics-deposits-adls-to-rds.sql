USE WAREHOUSE {{ params.warehouse }};

COPY INTO RDS_{{params.env}}.{{params.schema}}.DEPOSITS
FROM 	(
  SELECT
	$1:: STRING AS RestaurantNumber,
    $2:: STRING AS BusinessDate,	    
    $3:: STRING AS BagNumber,	       
    $4:: STRING AS EnteredAmount,	    
    $5:: STRING AS EnteredTime,	        
    $6:: STRING AS EnteredBy,	        
    $7:: STRING AS VerifiedAmount,	    
    $8:: STRING AS VerifiedTime,	    
    $9:: STRING AS VerifiedBy,	        
    $10:: STRING AS ValidatedAmount,	    
    $11:: STRING AS ValidatedTime,	    
    $12:: STRING AS ValidatedBy,         
    to_timestamp_ntz(current_timestamp)::STRING AS LOADDATETIME,
    to_number(to_char(current_date, 'YYYYMMDD'))::STRING AS LOADID,
    LOWER('incremental') AS LOADTYPE,
    LOWER('arbys') || '/' || METADATA$FILENAME::STRING AS FILENAME,
    LEFT(RIGHT(METADATA$FILENAME,18),8) AS FILEDATE,
    LEFT(RIGHT(METADATA$FILENAME,41),8) AS FOLDERDATE
    FROM @RDS_{{params.env}}.{{params.schema}}.ODI_READ_CSV_FROM_ADLS_TWO/{{params.run_date}}/ 
    (file_format => '{{params.file_format}}', pattern => '{{params.depositspattern}}')
);
