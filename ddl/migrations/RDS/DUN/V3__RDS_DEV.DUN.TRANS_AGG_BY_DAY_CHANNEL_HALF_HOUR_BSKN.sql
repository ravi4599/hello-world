DROP TABLE DNU_TRANS_AGG_BY_DAY_CHANNEL_HALF_HOUR_DNKN;
create or replace dynamic table DNU_TRANS_AGG_BY_DAY_CHANNEL_HALF_HOUR_DNKN
(
REST_ID	VARCHAR(16777216),
BRAND_ID	VARCHAR(16777216),
BUSINESS_DATE	DATE,
DAYPART_HALF_HOUR_ID	NUMBER(2,0),
CHANNEL_ID	VARCHAR(16777216),
TRANS_CNT	NUMBER(38,0),
DERIVED_GROSS_AMT	FLOAT,
DERIVED_DISCOUNT_AMT	FLOAT,
DERIVED_NET_AMT	FLOAT,
ITEM_CNT	NUMBER(38,0),
MAX_LOAD_DTTM	TIMESTAMP_NTZ(9),
MAX_UPDATE_DTTM	TIMESTAMP_NTZ(9)
)
TARGET_LAG = '5 minutes'
REFRESH_MODE = INCREMENTAL
WAREHOUSE = RA_UA_WH
AS
select
    trans.rest_id,
    trans.brand_id,
    trans.business_date,
    nvl(dayprt.HALF_HOUR_ID, -1) as DAYPART_HALF_HOUR_ID,
    nvl(trans.channel_id, '-1') as channel_id,
    sum(trans.trans_cnt) as trans_cnt,
    sum(trans.DERIVED_GROSS_AMT) as DERIVED_GROSS_AMT,
    sum(trans.derived_discount_amt) derived_discount_amt,
    sum(trans.derived_net_amt) derived_net_amt,
    sum(trans.item_cnt) item_cnt,
    Max(LOAD_DTTM) as MAX_LOAD_DTTM,
    Max(UPDATE_DTTM) as MAX_UPDATE_DTTM
from idh_dev.d_trans.trans_dnkn trans
join idh_dev.D_REF.DAYPART_HALF_HOUR dayprt 
    on (
        nvl(trans.time_key,-1) between dayprt.half_hour_start_time_key and dayprt.half_hour_end_time_key 
        and lower(dayprt.brand_id) = 'irb'
       )
group by
        trans.rest_id,
        trans.brand_id,
        trans.business_date,
        nvl(dayprt.HALF_HOUR_ID, -1),
        nvl(trans.channel_id, '-1');