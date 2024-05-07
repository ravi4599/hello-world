Hive Table : 
spark.sql("""CREATE EXTERNAL TABLE vv_db.hvtb_nbx_core_sw_group_commission_fact(
     |   group_id int, 
     |   package_id int, 
     |   invoice_item_type_id int, 
     |   ship_id int, 
     |   sail_id int, 
     |   agent_id int, 
     |   agency_id int, 
     |   commission_percentage double, 
     |   commission_code string, 
     |   commission_source string, 
     |   commission_amount double, 
     |   currency string, 
     |   currency_rate double, 
     |   base_commission_voyagefare double, 
     |   base_commission_taxesandfees double, 
     |   base_commission_insurance double, 
     |   base_commission_shorex double, 
     |   base_commission_sailorloot double, 
     |   bonus_commission_voyagefare double, 
     |   bonus_commission_taxesandfees double, 
     |   gsa_commission_voyagefare double, 
     |   gsa_commission_taxesandfees double, 
     |   gsa_commission_insurance double, 
     |   gsa_commission_shorex double, 
     |   gsa_commission_sailorloot double, 
     |   protected_commission double, 
     |   base_commission_voyagefare_usd double, 
     |   base_commission_taxesandfees_usd double, 
     |   base_commission_insurance_usd double, 
     |   base_commission_shorex_usd double, 
     |   base_commission_sailorloot_usd double, 
     |   bonus_commission_voyagefare_usd double, 
     |   bonus_commission_taxesandfees_usd double, 
     |   gsa_commission_voyagefare_usd double, 
     |   gsa_commission_taxesandfees_usd double, 
     |   gsa_commission_insurance_usd double, 
     |   gsa_commission_shorex_usd double, 
     |   gsa_commission_sailorloot_usd double, 
     |   protected_commission_usd double, 
     |   etl_ld_dt timestamp, 
     |   etl_upd_dt timestamp)
     | PARTITIONED BY ( 
     |   snapshot_date date)
     | ROW FORMAT SERDE 
     |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
     | WITH SERDEPROPERTIES ( 
     |   'path'='gs://vv-dev-nbx-cluster/data/mart/seaware/hvtb_nbx_core_sw_group_commission_fact', 
     |   'timestamp.formats'='yyyy-MM-dd HH:mm:ss') 
     | STORED AS INPUTFORMAT 
     |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
     | OUTPUTFORMAT 
     |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
     | LOCATION
     |   'gs://vv-dev-nbx-cluster/data/mart/seaware/hvtb_nbx_core_sw_group_commission_fact' """).show()
	 
	 
Spark Submit : 
spark-submit --name GroupCommissionFact --class com.virginvoyages.GroupCommissionFact --jars /home/ext_sarang_laxman_capgemini_com/jars/virginvoyages_shore_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar --properties-file /home/ext_sarang_laxman_capgemini_com/GroupCommission/SrcGroupCommissionFact_TgtGroupCommissionFact --master yarn --deploy-mode cluster --num-executors 2 --driver-memory 2g --executor-memory 2g --executor-cores 2 /home/ext_sarang_laxman_capgemini_com/GroupCommission/vv-nbx-datamart-group-commission-fact-0.0.1-SNAPSHOT-jar-with-dependencies.jar	 


MetaData: 
INSERT INTO shipdw.HBTB_INGESTION_METADATA (BATCH_INSTANCE_ID,BATCH_ID,PARENTBATCH,SRCCONFIGID,TGTCONFIGID,BATCHSTARTTIME,BATCHENDTIME,BATCH_EXECUTION_STARTTIME,BATCH_EXECUTION_ENDTIME,STATUS,ENVIRONMENT,VOYAGEID,TYPE)
VALUES('SrcGroupCommissionFact-TgtGroupCommissionFact','SrcGroupCommissionFact-TgtGroupCommissionFactl','null','SrcGroupCommissionFact','TgtGroupCommissionFact','1800-01-01 00:00:00','1800-01-01 00:00:00','1800-01-01 00:00:00','1800-01-01 00:00:00','Successful','DEV','SC2002269NCR','Fact');