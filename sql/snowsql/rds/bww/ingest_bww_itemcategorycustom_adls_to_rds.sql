USE WAREHOUSE {{ params.warehouse }};

START TRANSACTION NAME ITEMCATEGORYCUSTOM;

TRUNCATE TABLE RDS_{{params.env}}.{{params.schema}}.ITEMCATEGORYCUSTOM;

COPY INTO RDS_{{params.env}}.{{params.schema}}.ITEMCATEGORYCUSTOM
FROM
(
  SELECT
  $1::STRING AS categoryid,
  $2::STRING AS name,
  $3::STRING AS fkitemid,
  to_timestamp_ntz(CURRENT_TIMESTAMP)::STRING AS loaddatetime,
  to_number(to_char(CURRENT_DATE, 'YYYYMMDD'))::STRING AS loadid,
  LOWER('full') AS loadtype,
  LOWER('bww') || '/' || METADATA$FILENAME::STRING AS filename,
  LEFT(RIGHT(METADATA$FILENAME,14),10) AS filedate,
  LEFT(RIGHT(METADATA$FILENAME,14),10) AS folderdate
  FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_READ_BWW_ADLS2/bww_insight/ItemCategoryCustom/{{params.run_date}}/
  (file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
);

COMMIT;
