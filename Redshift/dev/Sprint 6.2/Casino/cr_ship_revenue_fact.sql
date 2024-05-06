drop table if exists hive_schema_stg.ship_revenue_fact;
CREATE EXTERNAL TABLE hive_schema_stg.ship_revenue_fact
(
   voyage_id                     varchar(100),
   sale_detail_skey              bigint      ,
   pos_item_skey                 bigint      ,
   folio_item_skey               bigint      ,
   person_skey                   bigint      ,
   itinerary_skey                bigint      ,
   outlet_skey                   bigint      ,
   activity_skey                 bigint      ,
   sale_date_skey                bigint      ,
   sale_time_skey                bigint      ,
   device_skey bigint, 
  time_on_device_seconds int, 
  avg_wager_amount float, 
  casino_game_dph int, 
  theoritical_winnings float, 
   sale_quantity                 int         ,
   item_list_price_amount        float,
   debit_amount                  float,
   credit_amount                 float,
   tax_amount                    float,
   discount_amount               float,
   discount_value                float,
   cost_of_goods                 float,
   manual_adjustment_amount      float,
   total_collected_sales_amount  float,
   total_collected_sales_ap      float,
   total_shared_revenue_amount   float,
   total_shared_revenue_ap       float,
   load_dt                       timestamp   ,
   upd_dt                        timestamp) 
PARTITIONED BY ( 
  part_dt date)   
STORED AS PARQUET 
LOCATION 
  's3://vv-dev-emr-cluster/data/mart/hvtb_mart_fact_revenue'  
;

ALTER TABLE hive_schema_stg.ship_revenue_fact ADD IF NOT EXISTS  PARTITION (part_dt = '2020-01-21') location 's3://vv-dev-emr-cluster/data/mart/hvtb_mart_fact_revenue/part_dt=2020-01-21';



