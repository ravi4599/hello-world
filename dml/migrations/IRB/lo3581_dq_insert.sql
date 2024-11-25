INSERT INTO IDS_UAT.INT_REF.DQ_VALIDATION_RULE	
(
	RULE_TYPE
	,RULE_DESC
	,RULE_SEVERITY_TYPE
	,TARGET_DATA_ZONE_CODE
	,TABLE_NAME
	,COLUMN_NAME
	,RULE_SQL_TEXT
	,RULE_LEVEL_TYPE
	,RULE_OWNER_TYPE
	,RULE_ACTIVE_IND
	,RULE_STATUS_TYPE
	,RULE_STATUS_DESC
	,BRAND_ID
	,SOURCE_SYSTEM_NAME
	,LOAD_ID
	,LOAD_DTTM
	,UPDATE_ID
	,UPDATE_DTTM
)
VALUES
(
'File check',
'File availability for current day run',
'Critical',
'STG',
'ACRELAC_SOS_BY_CAR_LAST_WEEK',
'N/A',
'SELECT  
CASE 
    WHEN DAYOFWEEK(CURRENT_DATE()) != 3 THEN \'PASS\'
    WHEN DAYOFWEEK(CURRENT_DATE()) = 3 and COUNT(*) > 0 THEN \'PASS\'     
    ELSE \'FAIL\' 
  END AS match_found
FROM RDS_UAT.ARB.ACRELAC_SOS_BY_CAR
WHERE LOWER(LOAD_FILENAME) LIKE CONCAT(\'%arbys_weekly_\', TO_CHAR(CURRENT_DATE() - 3, \'yyyyMMdd\'), \'/part%\')',
'TABLE',
'IT',
'TRUE',
'Active',
'New Rule',
'arbys',
'acrelac',
to_char(sysdate(), 'yyyyMMddhhmiss'),
sysdate(),
to_char(sysdate(), 'yyyyMMddhhmiss'),
sysdate()
)
;
