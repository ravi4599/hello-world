USE WAREHOUSE {{ params.warehouse }};

START TRANSACTION NAME DPVHSTSALESBYINTERVAL;

DELETE FROM RDS_{{params.env}}.{{params.schema}}.DPVHSTSALESBYINTERVAL
WHERE CAST(DATEOFBUSINESS AS DATE) BETWEEN '{{params.fromdate}}' AND '{{params.todate}}';

COPY INTO RDS_{{params.env}}.{{params.schema}}.DPVHSTSALESBYINTERVAL
FROM
(
  SELECT
  $1::STRING AS uniqueid,
  $2::STRING AS dateofbusiness,
  $3::STRING AS fkstoreid,
  $4::STRING AS fkrevenueid,
  $5::STRING AS period,
  $6::STRING AS type,
  $7::STRING AS typeid,
  $8::STRING AS typeid2,
  $9::STRING AS amount,
  $10::STRING AS openhour,
  $11::STRING AS lcount,
  $12::STRING AS fkoccasionid,
  $13::STRING AS importchecksum,
  $14::STRING AS datetimestamp,
  $15::STRING AS eimporterfileguid,
  to_timestamp_ntz(CURRENT_TIMESTAMP)::STRING AS loaddatetime,
  to_number(to_char(CURRENT_DATE, 'YYYYMMDD'))::STRING AS loadid,
  LOWER('incremental') AS loadtype,
  LOWER('bww') || '/' || METADATA$FILENAME::STRING AS filename,
  LEFT(RIGHT(METADATA$FILENAME,14), 10) AS filedate,
  LEFT(RIGHT(METADATA$FILENAME,14), 10)AS folderdate
  FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_READ_BWW_ADLS2/bww_insight/dpvHstSalesByInterval/{{params.run_date}}/
  (file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
);

COMMIT;