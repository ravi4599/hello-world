CREATE EXTERNAL TABLE hive_schema_stg.person_dim
(
   person_skey             bigint      ,
   person_id               varchar(100),
   person_guid             varchar(100),
   person_charge_id        varchar(100),
   guest_wearable_id       varchar(100),
   account_number          varchar(100),
   person_type             varchar(100),
   person_first_name       varchar(100),
   person_middle_name      varchar(100),
   person_last_name        varchar(100),
   person_dob              date        ,
   person_age              int         ,
   person_nationality      varchar(100),
   address_country         varchar(100),
   address_type            varchar(100),
   address_mailing         varchar(500),
   address_line1           varchar(500),
   address_line2           varchar(500),
   address_line3           varchar(500),
   address_postal_code     varchar(50) ,
   address_city            varchar(50) ,
   address_state           varchar(50) ,
   booking_arrival_date    timestamp   ,
   booking_departure_date  timestamp   ,
   booking_reference       varchar(100),
   cabin_number            varchar(100),
   cabin_category          varchar(100),
   cabin_type              varchar(100),
   mega_rockstar_flag      boolean     ,
   vip_flag                boolean     ,
   sailor_tribe            varchar(100),
   sailor_subtribe         varchar(100),
   voyage_id               varchar(100),
   load_dt                 timestamp   ,
   upd_dt                  timestamp   ,
   primaryhash             varchar(500),
   md5_hash                varchar(500),
   seaware_id varchar(100), 
  person_card_status varchar(100), 
  person_card_type varchar(100), 
  person_card_level decimal(2,0), 
  person_card_color decimal(1,0), 
  person_card_color_value varchar(100)
)
STORED AS PARQUET 
LOCATION
  's3://vv-dev-emr-cluster/data/mart/hvtb_mart_dim_person'
;


