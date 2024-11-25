
DELETE FROM IDS_{{params.env}}.TXN.FN_SYSTEM;

INSERT INTO IDS_{{params.env}}.TXN.FN_SYSTEM
(FN_SYSTEM_ID,FN_SYSTEM_CATEGORY_CODE,SYSTEM_NAME,SYSTEM_DESC,BRAND_ID,SOURCE_SYSTEM_NAME,LOAD_ID,LOAD_DTTM,UPDATE_ID,UPDATE_DTTM)
SELECT *, 'irb' SOURCE_SYSTEM_NAME,  TO_CHAR(CURRENT_DATE, 'YYYYMMDD') LOAD_ID,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)) LOAD_DTTM
, TO_CHAR(CURRENT_DATE, 'YYYYMMDD') UPDATE_ID,TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)) UPDATE_DTTM
FROM ( VALUES 
(1 , 'POS' , 'PAR Brink Data' , 'PAR Brink Data' , 'arbys' ), 
(2,  'BOS', 'Altametrics' ,  'Altametrics' , 'arbys' ), 
(3,  'SS', 'Sales System' ,  'Sales System' , 'arbys' ), 
(4,  'GL', 'Oracle General Ledger' , 'Oracle General Ledger' , 'arbys' ), 
(5,  'POS', 'Infor Back Office System' , 'Infor Back Office System' , 'sonic' ),
(6,  'POS', 'Micros Back Office System' , 'Micros Back Office System' , 'sonic' ),
(7,  'BOS', 'Infor Back Office System' , 'Infor Back Office System' , 'sonic' ),
(8,  'BOS', 'Micros Back Office System' , 'Micros Back Office System' , 'sonic' ),
(9,  'GL',  'Oracle General Ledger' ,' Oracle General Ledger' , 'sonic' ),
(10, 'POS', 'Aloha System' , 'Aloha System Data' , 'bww' ),
(11, 'BOS', 'Altametrics' , 'Altametrics' , 'bww' ),
(12, 'BOS', 'NCR Back Office System' , 'NCR Back Office System' , 'bww' ),
(13, 'GL',  'Oracle General Ledger' , 'Oracle General Ledger' , 'bww' ),
(14, 'POS', 'pdq' , 'pdq' , 'jj' ),
(15, 'BOS', 'JJ Services' , 'JJ Services' , 'jj' ),
(16, 'GL',  'Oracle General Ledger' , 'Oracle General Ledger' , 'jj' ),
(17, 'POS', 'Symphony' , 'Symphony' , 'dunkin' ),
(18, 'BOS', 'CrunchTime' , 'CrunchTime' , 'dunkin' ),
(19, 'GL',  'Oracle General Ledger' , 'Oracle General Ledger' , 'dunkin' )
) as New_Data (FN_SYSTEM_ID,FN_SYSTEM_CATEGORY_CODE,SYSTEM_NAME,SYSTEM_DESC,BRAND_ID);