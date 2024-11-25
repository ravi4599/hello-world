USE WAREHOUSE {{ params.warehouse }};

START TRANSACTION NAME DPVHSTCHECKSUMMARY ;

DELETE FROM RDS_{{params.env}}.{{params.schema}}.DPVHSTCHECKSUMMARY
WHERE CAST(DATEOFBUSINESS AS DATE) >= '{{params.fromdate}}' AND CAST(DATEOFBUSINESS AS DATE) <= '{{params.todate}}';

COPY INTO RDS_{{params.env}}.{{params.schema}}.DPVHSTCHECKSUMMARY
FROM
(
  SELECT
  $1::STRING AS uniqueid,
  $2::STRING AS dateofbusiness,
  $3::STRING AS fkstoreid,
  $4::STRING AS fkemployeenumber,
  $5::STRING AS checkid,
  $6::STRING AS tabledescription,
  $7::STRING AS comps,
  $8::STRING AS guestcount,
  $9::STRING AS itemcount,
  $10::STRING AS itemsales,
  $11::STRING AS modecharge,
  $12::STRING AS promos,
  $13::STRING AS payment,
  $14::STRING AS surcharge,
  $15::STRING AS tax,
  $16::STRING AS opentime,
  $17::STRING AS firstordertime,
  $18::STRING AS lastordertime,
  $19::STRING AS closetime,
  $20::STRING AS actualclosetime,
  $21::STRING AS checkclosetime,
  $22::STRING AS checktime,
  $23::STRING AS fkdaypartid,
  $24::STRING AS fkrevenueid,
  $25::STRING AS netsales,
  $26::STRING AS taxexemptamt,
  $27::STRING AS lastpaymenttime,
  $28::STRING AS seattime,
  $29::STRING AS lastbumptime,
  $30::STRING AS additionalcharges,
  $31::STRING AS gsttax,
  $32::STRING AS fkoccasionid,
  $33::STRING AS importid,
  $34::STRING AS importchecksum,
  $35::STRING AS datetimestamp,
  $36::STRING AS tippablesales,
  $37::STRING AS tippablechargesales,
  $38::STRING AS checkcounter,
  $39::STRING AS eimporterfileguid,
  to_timestamp_ntz(CURRENT_TIMESTAMP)::STRING AS loaddatetime,
  to_number(to_char(CURRENT_DATE, 'YYYYMMDD'))::STRING AS loadid,
  LOWER('bww') || '/' || METADATA$FILENAME::STRING AS filename,
  LEFT(RIGHT(METADATA$FILENAME,14),10) AS filedate,
  LEFT(RIGHT(METADATA$FILENAME,14),10)AS folderdate
  FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_READ_BWW_ADLS2/bww_insight/dpvHstCheckSummary/{{params.run_date}}/
  (file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
);

COMMIT;