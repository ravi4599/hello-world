USE WAREHOUSE {{ params.warehouse }};

COPY INTO RDS_{{params.env}}.{{params.schema}}.DPVHSTSALESSUMMARY
FROM
(
  SELECT
  $1::STRING AS uniqueid,
  $2::STRING AS type,
  $3::STRING AS typeid,
  $4::STRING AS typeid2,
  $5::STRING AS amount,
  $6::STRING AS lcount,
  $7::STRING AS fkstoreid,
  $8::STRING AS dateofbusiness,
  $9::STRING AS fkoccasionid,
  $10::STRING AS importid,
  $11::STRING AS importchecksum,
  $12::STRING AS datetimestamp,
  $13::STRING AS eimporterfileguid,
  to_timestamp_ntz(current_timestamp)::STRING AS loaddatetime,
  to_number(to_char(current_date, 'YYYYMMDD'))::STRING AS loadid,
  LOWER('bww') || '/' || METADATA$FILENAME::STRING AS filename,
  LEFT(RIGHT(METADATA$FILENAME,10),10) AS filedate,
  LEFT(RIGHT(METADATA$FILENAME,10),10)AS folderdate
  FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_READ_BWW_ADLS2/bww_insight/dpvHstSalesSummary/{{params.run_date}}/
  (file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
 );     