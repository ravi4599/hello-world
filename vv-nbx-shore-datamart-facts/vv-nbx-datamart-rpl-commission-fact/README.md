HIVE TABLE : 

CREATE EXTERNAL TABLE vv_db.hvtb_nbx_core_sw_commission_fact(res_id INT, guest_id INT, package_id INT, addon_id INT, invoice_item_type_id INT, ship_id INT, agency_id INT, agent_id INT, sail_id INT, currency_code STRING, currency_rate DOUBLE, commission_type_skey INT, charge_id INT, is_cancelled STRING, commission_percent DOUBLE, commission_amount DOUBLE, commission_fare DOUBLE, commission_payout_date TIMESTAMP, base_commission_voyagefare_regular DOUBLE, base_commission_voyagefare_protected DOUBLE, base_commission_voyagefare_manual DOUBLE, base_commission_voyagefare DOUBLE, base_commission_taxesandfees_regular DOUBLE, base_commission_taxesandfees_protected DOUBLE, base_commission_taxesandfees_manual DOUBLE, base_commission_taxesandfees DOUBLE, base_commission_shorex_regular DOUBLE, base_commission_shorex_protected DOUBLE, base_commission_shorex_manual DOUBLE, base_commission_shorex DOUBLE, base_commission_sailorloot_regular DOUBLE, base_commission_sailorloot_protected DOUBLE, base_commission_sailorloot_manual DOUBLE, base_commission_sailorloot DOUBLE, base_commission_hotelfare_regular DOUBLE, base_commission_hotelfare_protected DOUBLE, base_commission_hotelfare_manual DOUBLE, base_commission_hotelfare DOUBLE, base_commission_insurance_regular DOUBLE, base_commission_insurance_protected DOUBLE, base_commission_insurance DOUBLE, bonus_commission_voyagefare_regular DOUBLE, bonus_commission_voyagefare_protected DOUBLE, bonus_commission_voyagefare_manual DOUBLE, bonus_commission DOUBLE, voyagefare_commission DOUBLE, gsa_commission_voyagefare DOUBLE, gsa_commission_taxesandfees DOUBLE, gsa_commission_insurance DOUBLE, gsa_commission_shorex DOUBLE, gsa_commission_sailorloot DOUBLE, protected_commission DOUBLE, base_protected_commission DOUBLE, base_commission_other_bookable DOUBLE, base_commission_voyagefare_regular_usd DOUBLE, base_commission_voyagefare_protected_usd DOUBLE, base_commission_voyagefare_manual_usd DOUBLE, base_commission_voyagefare_usd DOUBLE, base_commission_taxesandfees_regular_usd DOUBLE, base_commission_taxesandfees_protected_usd DOUBLE, base_commission_taxesandfees_manual_usd DOUBLE, base_commission_taxesandfees_usd DOUBLE, base_commission_shorex_regular_usd DOUBLE, base_commission_shorex_protected_usd DOUBLE, base_commission_shorex_manual_usd DOUBLE, base_commission_shorex_usd DOUBLE, base_commission_sailorloot_regular_usd DOUBLE, base_commission_sailorloot_protected_usd DOUBLE, base_commission_sailorloot_manual_usd DOUBLE, base_commission_sailorloot_usd DOUBLE, base_commission_hotelfare_regular_usd DOUBLE, base_commission_hotelfare_protected_usd DOUBLE, base_commission_hotelfare_manual_usd DOUBLE, base_commission_hotelfare_usd DOUBLE, base_commission_insurance_regular_usd DOUBLE, base_commission_insurance_protected_usd DOUBLE, base_commission_insurance_usd DOUBLE, bonus_commission_voyagefare_regular_usd DOUBLE, bonus_commission_voyagefare_protected_usd DOUBLE, bonus_commission_voyagefare_manual_usd DOUBLE, bonus_commission_usd DOUBLE, voyagefare_commission_usd DOUBLE, gsa_commission_voyagefare_usd DOUBLE, gsa_commission_taxesandfees_usd DOUBLE, gsa_commission_insurance_usd DOUBLE, gsa_commission_shorex_usd DOUBLE, gsa_commission_sailorloot_usd DOUBLE, protected_commission_usd DOUBLE, base_protected_commission_usd DOUBLE, base_commission_other_bookable_usd DOUBLE, upd_dt TIMESTAMP,load_dt TIMESTAMP)
PARTITIONED BY (snapshot_date DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'timestamp.formats' = 'yyyy-MM-dd HH:mm:ss',
  'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'gs://vv-dev-nbx-cluster/data/mart/seaware/hvtb_nbx_core_sw_commission_fact'
TBLPROPERTIES (
  'transient_lastDdlTime' = '1661853993')
  
  
  
  
GCP TABLE NAME : 

CREATE OR REPLACE EXTERNAL TABLE hive_schema_stg.seaware_commission_fact (
    res_id int,
    guest_id int,
    package_id int,
    addon_id int,
    invoice_item_type_id int,
    ship_id int,
    agency_id int,
    agent_id int,
    sail_id int,
    currency_code string,
    currency_rate float64,
    commission_type_skey int,
    charge_id int,
    is_cancelled string,
    commission_percent float64,
    commission_amount float64,
    commission_fare float64,
    commission_payout_date timestamp,
    base_commission_voyagefare_regular float64,
    base_commission_voyagefare_protected float64,
    base_commission_voyagefare_manual float64,
    base_commission_voyagefare float64,
    base_commission_taxesandfees_regular float64,
    base_commission_taxesandfees_protected float64,
    base_commission_taxesandfees_manual float64,
    base_commission_taxesandfees float64,
    base_commission_shorex_regular float64,
    base_commission_shorex_protected float64,
    base_commission_shorex_manual float64,
    base_commission_shorex float64,
    base_commission_sailorloot_regular float64,
    base_commission_sailorloot_protected float64,
    base_commission_sailorloot_manual float64,
    base_commission_sailorloot float64,
    base_commission_hotelfare_regular float64,
    base_commission_hotelfare_protected float64,
    base_commission_hotelfare_manual float64,
    base_commission_hotelfare float64,
    base_commission_insurance_regular float64,
    base_commission_insurance_protected float64,
    base_commission_insurance float64,
    bonus_commission_voyagefare_regular float64,
    bonus_commission_voyagefare_protected float64,
    bonus_commission_voyagefare_manual float64,
    bonus_commission float64,
    voyagefare_commission float64,
    gsa_commission_voyagefare float64,
    gsa_commission_taxesandfees float64,
    gsa_commission_insurance float64,
    gsa_commission_shorex float64,
    gsa_commission_sailorloot float64,
    protected_commission float64,
    base_protected_commission float64,
    base_commission_other_bookable float64,
    base_commission_voyagefare_regular_usd float64,
    base_commission_voyagefare_protected_usd float64,
    base_commission_voyagefare_manual_usd float64,
    base_commission_voyagefare_usd float64,
    base_commission_taxesandfees_regular_usd float64,
    base_commission_taxesandfees_protected_usd float64,
    base_commission_taxesandfees_manual_usd float64,
    base_commission_taxesandfees_usd float64,
    base_commission_shorex_regular_usd float64,
    base_commission_shorex_protected_usd float64,
    base_commission_shorex_manual_usd float64,
    base_commission_shorex_usd float64,
    base_commission_sailorloot_regular_usd float64,
    base_commission_sailorloot_protected_usd float64,
    base_commission_sailorloot_manual_usd float64,
    base_commission_sailorloot_usd float64,
    base_commission_hotelfare_regular_usd float64,
    base_commission_hotelfare_protected_usd float64,
    base_commission_hotelfare_manual_usd float64,
    base_commission_hotelfare_usd float64,
    base_commission_insurance_regular_usd float64,
    base_commission_insurance_protected_usd float64,
    base_commission_insurance_usd float64,
    bonus_commission_voyagefare_regular_usd float64,
    bonus_commission_voyagefare_protected_usd float64,
    bonus_commission_voyagefare_manual_usd float64,
    bonus_commission_usd float64,
    voyagefare_commission_usd float64,
    gsa_commission_voyagefare_usd float64,
    gsa_commission_taxesandfees_usd float64,
    gsa_commission_insurance_usd float64,
    gsa_commission_shorex_usd float64,
    gsa_commission_sailorloot_usd float64,
    protected_commission_usd float64,
    base_protected_commission_usd float64,
    base_commission_other_bookable_usd float64,
    upd_dt timestamp,
	load_dt timestamp
)

WITH PARTITION COLUMNS(snapshot_date date)
options (
format = 'parquet',
uris = ['gs://vv-dev-nbx-cluster/data/mart/seaware/hvtb_nbx_core_sw_commission_fact/*'],
hive_partition_uri_prefix  = 'gs://vv-dev-nbx-cluster/data/mart/seaware/hvtb_nbx_core_sw_commission_fact')


View in GCP : 

create or replace view seaware.seaware_commission_fact as select * from hive_schema_stg.seaware_commission_fact