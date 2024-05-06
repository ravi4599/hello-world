drop table if exists hive_schema_stg.seaware_revenue_fact;

CREATE EXTERNAL TABLE hive_schema_stg.seaware_revenue_fact
(
   snapshot_time         timestamp    ,
   res_id                int          ,
   guest_id              int          ,
   package_id            int          ,
   invoice_item_type_id  int          ,
   price_area_id         int          ,
   ship_id               int          ,
   agent_id              int          ,
   agency_id             int          ,
   promotion_id          int          ,
   addon_id              int          ,
   cardeck_id            int          ,
   shorex_id             int          ,
   hotel_res_req_id      int          ,
   ship_room_req_id	int,
   sail_id               int          ,
   amount                float       ,
   commission_percent    float       ,
   net_due               float       ,
   addon_qty             float       ,
   currency              varchar(5)   ,
   currency_rate         float       ,
   etl_ld_dt             timestamp    ,
   etl_upd_dt            timestamp            
)
PARTITIONED BY ( 
  snapshot_date date) 
STORED AS PARQUET  
LOCATION
  's3://vv-qa-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_revenue_fact'  
;

ALTER TABLE hive_schema_stg.seaware_revenue_fact ADD IF NOT EXISTS  PARTITION (snapshot_date = '2020-05-20') location 's3://vv-qa-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_revenue_fact/snapshot_date=2020-05-20';

ALTER TABLE hive_schema_stg.seaware_revenue_fact ADD IF NOT EXISTS  PARTITION (snapshot_date = '2020-05-21') location 's3://vv-qa-emr-cluster/data/core/seaware/hvtb_nbx_core_sw_revenue_fact/snapshot_date=2020-05-21';
