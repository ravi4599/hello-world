USE WAREHOUSE {{ params.warehouse }};

START TRANSACTION NAME GNDTENDER ;

DELETE FROM RDS_{{params.env}}.{{params.schema}}.DPVHSTGNDTENDER
WHERE CAST(DATEOFBUSINESS AS DATE) >= '{{params.fromdate}}' AND CAST(DATEOFBUSINESS AS DATE) <= '{{params.todate}}';

COPY INTO RDS_{{params.env}}.{{params.schema}}.DPVHSTGNDTENDER
FROM
(
  SELECT
  $1::STRING AS uniqueid,
  $2::STRING AS fkemployeenumber,
  $3::STRING AS checknumber,
  $4::STRING AS dateofbusiness,
  $5::STRING AS type,
  $6::STRING AS typeid,
  $7::STRING AS ident,
  $8::STRING AS auth,
  $9::STRING AS exp,
  $10::STRING AS name,
  $11::STRING AS unit,
  $12::STRING AS amount,
  $13::STRING AS tip,
  $14::STRING AS nr,
  $15::STRING AS track,
  $16::STRING AS fkhouseaccountid,
  $17::STRING AS fktippableemployee,
  $18::STRING AS fkmanagernumber,
  $19::STRING AS hour,
  $20::STRING AS minute,
  $21::STRING AS id,
  $22::STRING AS autogratuity,
  $23::STRING AS fkstoreid,
  $24::STRING AS fkrevenueid,
  $25::STRING AS fkoccasionid,
  $26::STRING AS importchecksum,
  $27::STRING AS datetimestamp,
  $28::STRING AS systemdate,
  $29::STRING AS unitid,
  $30::STRING AS source,
  $31::STRING AS pmspoststatus,
  $32::STRING AS drawerid,
  $33::STRING AS eimporterfileguid,
  to_timestamp_ntz(CURRENT_TIMESTAMP)::STRING AS loaddatetime,
  to_number(to_char(CURRENT_DATE, 'YYYYMMDD'))::STRING AS loadid,
  LOWER('bww') || '/' || METADATA$FILENAME::STRING AS filename,
  LEFT(RIGHT(METADATA$FILENAME,14),10) AS filedate,
  LEFT(RIGHT(METADATA$FILENAME,14),10) AS folderdate
  FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_READ_BWW_ADLS2/bww_insight/dpvHstGndTender/{{params.run_date}}/
  (file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
);

COMMIT;
 
