USE WAREHOUSE {{ params.warehouse }};

INSERT INTO RDS_{{params.env}}.{{params.schema}}.ORACLE_GL
(
  ledger_name, posted_date, period, gl_date, batch_name, jeh_creaton_date, jeh_created_by,
  jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt, co, lob, cc, acct, interco,
  f1, f2, loaddatetime, loadid, loadtype, filename, filedate, folderdate
)
SELECT
  ledger_name, posted_date, period, gl_date, batch_name, jeh_creaton_date, jeh_created_by,
  jeh_name, je_source, jel_description, cur_code, dr_amt, cr_amt, co, lob, cc, acct, interco,
  f1, f2, loaddatetime, loadid, loadtype, filename, filedate, folderdate
FROM
(
SELECT
$1::STRING AS ledger_name,
$2::STRING AS posted_date,
$3::STRING AS period,
$4::STRING AS gl_date,
$5::STRING AS batch_name,
$6::STRING AS jeh_creaton_date,
$7::STRING AS jeh_created_by,
$8::STRING AS jeh_name,
$9::STRING AS je_source,
$10::STRING AS jel_description,
$11::STRING AS cur_code,
$12::STRING AS dr_amt,
$13::STRING AS cr_amt,
$14::STRING AS co,
$15::STRING AS lob,
$16::STRING AS cc,
$17::STRING AS acct,
$18::STRING AS interco,
$19::STRING AS f1,
$20::STRING AS f2,
to_timestamp_ntz(CURRENT_TIMESTAMP)::STRING AS loaddatetime,
to_number(to_char(CURRENT_DATE, 'YYYYMMDD'))::STRING AS loadid,
LOWER('incremental') AS loadtype,
LOWER('arbys') || '/' || METADATA$FILENAME::STRING AS filename,
to_Date( substring(filename, POSITION('UDP_', filename, 1)+4,8),'yyyymmdd') AS filedate , filedate as FolderDate
FROM @RDS_{{params.env}}.{{params.schema}}.SALES_RECON_READ_ADLS2/OracleGL/
(file_format => '{{params.file_format}}', pattern => '{{params.pattern}}')
WHERE CAST(METADATA$FILE_LAST_MODIFIED AS DATE) BETWEEN DATE('{{params.load_start_dt}}', 'YYYYMMDD') AND 
DATE('{{params.load_end_dt}}', 'YYYYMMDD')
);