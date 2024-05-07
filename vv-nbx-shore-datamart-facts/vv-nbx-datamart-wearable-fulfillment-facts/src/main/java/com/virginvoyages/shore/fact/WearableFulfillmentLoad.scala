package com.virginvoyages.shore.fact


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import java.net.UnknownHostException
import java.sql.SQLException
import scala.util.parsing.json._
//import scalaj.http.Http
import org.apache.spark.sql.DataFrame
//import scalaj.http.HttpOptions
import scala.util.Try
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import com.virginvoyages.metadataframework.ManageMetadata

object WearableFulfillmentLoad {

	val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]) {

		def getSparkSession() =
			{
					val spark = SparkSession
							.builder()
							.enableHiveSupport()
							.getOrCreate()

							spark

			}

		val spark = getSparkSession()
				val sc = spark.sparkContext
				val sqlContext = new org.apache.spark.sql.SQLContext(sc)

				val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
				sqlContext.setConf("hive.exec.dynamic.partition", "true")
				sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

				/*************************************calling metadata framework*********************************************/

				val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
				metadata.productIterator.foreach(println)
				val batch_id1 = metadata._1
				val batch_instance_id1 = metadata._2
				val batch_start_tme = metadata._3
				val batch_end_tme = metadata._4
				val part_read_start = metadata._5
				val part_read_end = metadata._6
				val start_execution_time = metadata._7
				val part_write_date = metadata._8

				try {

					import spark.sqlContext.implicits._


//										val resDF=spark.sql("""Select rd.res_id,sail.SAIL_ID,gm.guest_id,rd.src_res_id,cast(gm.client_id as integer) from (Select * from vv_db.hvtb_nbx_core_sw_reservation_dim res_dim where TO_DATE(res_dim.rec_end_dttm) = '9999-12-31') rd   JOIN vv_db.hvtb_nbx_core_sw_res_guest_rel guest_rel ON rd.res_id = guest_rel.res_id  JOIN vv_db.hvtb_nbx_core_sw_guest_dim gm ON (guest_rel.guest_id=gm.guest_id AND TO_DATE(gm.rec_end_dttm)='9999-12-31')  JOIN vv_db.hvtb_nbx_core_sw_sail_dim sail ON (rd.src_sail_id = sail.src_sail_id AND TO_DATE(sail.rec_end_dttm)='9999-12-31') """)
//										resDF.createOrReplaceTempView("restable")			
//										
//										val wearbleDF=spark.sql("""select res.res_id,res.SAIL_ID,res.guest_id,res.src_res_id,res.client_id,case when flag.sw_generated_by_flg is null then false else flag.sw_generated_by_flg end as sw_generated_by_flg,flag.names as sw_wearable_type from restable res left join (select res_id,client_id,sw_generated_by_flg,names from ( select * ,row_number() over (partition by client_id,res_id order by batchtime desc) rn from shipdw.hvtb_parse_seaware_wearables_flag) ol where ol.rn=1) flag on res.src_res_id=flag.res_id and res.client_id=flag.client_id""")
//										wearbleDF.createOrReplaceTempView("wearabletable")					
//					
//										val swDF=spark.sql("""select wtab.res_id,wtab.SAIL_ID,wtab.guest_id,wtab.src_res_id,wtab.client_id,wtab.sw_generated_by_flg,wtab.sw_wearable_type,case when sw.flag is null then false when sw.flag='Y' then true when sw.flag='N' then false end as sw_listener_success_flg from wearabletable wtab left join (select src_res_id,client_id,flag from ( select * ,row_number() over (partition by src_res_id,client_id order by change_dt desc) rn from shipdw.hvtb_parse_swtovxp_wearable_fulfillment) aj where aj.rn=1) sw on wtab.src_res_id=sw.src_res_id and wtab.client_id=sw.client_id""")
//										swDF.createOrReplaceTempView("swtable")
//					
//										val errDF=spark.sql("""select listener.res_id,listener.SAIL_ID,listener.guest_id,listener.src_res_id,listener.client_id,listener.sw_generated_by_flg,listener.sw_wearable_type,listener.sw_listener_success_flg,case when err.sw_listener_error_flg is null then false else err.sw_listener_error_flg end as sw_listener_error_flg,err.sw_listener_error_details from swtable listener left join (select src_res_id,client_id,sw_listener_error_flg,sw_listener_error_details from ( select * ,row_number() over (partition by src_res_id,client_id order by change_dt desc) rn from shipdw.hvtb_parse_error_reporting_wearable) rt where rt.rn=1) err on listener.src_res_id=err.src_res_id and listener.client_id=err.client_id""")
//										errDF.createOrReplaceTempView("errtable")
//					
//										val finalDF=spark.sql("""select A.res_id,A.SAIL_ID,A.guest_id,A.src_res_id,A.client_id,A.sw_generated_by_flg,A.sw_wearable_type,A.sw_listener_success_flg,A.sw_listener_error_flg,A.sw_listener_error_details,case when entityhash.vxp_to_rrd_success_flg is null then false else entityhash.vxp_to_rrd_success_flg end as vxp_to_rrd_success_flg,entityhash.vxp_to_rrd_status,entityhash.vxp_wearable_color,case when entityhash.vxp_rrd_file_received_flg is null then false else entityhash.vxp_rrd_file_received_flg end as vxp_rrd_file_received_flg,case when entityhash.rrd_to_vxp_ingestion_success_flg is null then false else entityhash.rrd_to_vxp_ingestion_success_flg end as rrd_to_vxp_ingestion_success_flg,entityhash.rrd_to_vxp_ingestion_status,case when entityhash.vxp_wearable_processing_completed_flg is null then false else entityhash.vxp_wearable_processing_completed_flg end as vxp_wearable_processing_completed_flg from errtable A left join (select src_res_id,client_id,vxp_to_rrd_success_flg,vxp_to_rrd_status,vxp_wearable_color,vxp_rrd_file_received_flg,rrd_to_vxp_ingestion_success_flg,rrd_to_vxp_ingestion_status,vxp_wearable_processing_completed_flg from ( select * ,row_number() over (partition by entityhashid order by change_dt desc) rn from shipdw.hvtb_parse_entity_hash_wearable) op where op.rn=1) entityhash on A.src_res_id=entityhash.src_res_id and A.client_id=entityhash.client_id """)

					

					val resDF=spark.sql("""Select res_dim.res_id,sail.SAIL_ID,gm.guest_id,res_dim.src_res_id,cast(gm.client_id as integer) 
							from vv_db.hvtb_nbx_core_sw_reservation_dim res_dim 
							JOIN vv_db.hvtb_nbx_core_sw_res_guest_rel guest_rel ON res_dim.res_id = guest_rel.res_id  
							JOIN vv_db.hvtb_nbx_core_sw_guest_dim gm ON (guest_rel.guest_id=gm.guest_id AND TO_DATE(gm.rec_end_dttm)='9999-12-31')  
							JOIN vv_db.hvtb_nbx_core_sw_sail_dim sail ON (res_dim.src_sail_id = sail.src_sail_id AND TO_DATE(sail.rec_end_dttm)='9999-12-31') 
							where TO_DATE(res_dim.rec_end_dttm) = '9999-12-31' """)   
							
					resDF.createOrReplaceTempView("restable")


					val wearbleDF=spark.sql("""select res_id,client_id,sw_generated_by_flg,names as sw_wearable_type from (select res_id, client_id, sw_generated_by_flg, names, row_number() over (partition by client_id,res_id order by batchtime desc) rn from shipdw.hvtb_parse_seaware_wearables) ol where ol.rn=1""")
					wearbleDF.createOrReplaceTempView("wearabletable")					
 

					val swDF=spark.sql("""select src_res_id,client_id,flag from ( select src_res_id, client_id, flag, row_number() over (partition by src_res_id,client_id order by change_dt desc) rn from shipdw.hvtb_parse_swtovxp_wearable_fulfillment) aj where aj.rn=1""")
					swDF.createOrReplaceTempView("swtable") 

					val errDF=spark.sql("""select src_res_id,client_id,sw_listener_error_flg,sw_listener_error_details from ( select * ,row_number() over (partition by src_res_id,client_id order by change_dt desc) rn from shipdw.hvtb_parse_error_reporting_wearable) rt where rt.rn=1""")
					errDF.createOrReplaceTempView("errtable") 

					//val entityDF=spark.sql("""select seaware_to_vxp_success_flg,seaware_to_vxp_status,vxp_to_rrd_success_flg,vxp_to_rrd_status,vxp_wearable_color,vxp_rrd_file_received_flg,rrd_to_vxp_ingestion_success_flg,rrd_to_vxp_ingestion_status,uniquesourceid from ( select * ,row_number() over (partition by uniquesourceid order by change_dt desc,addeddate desc) rn from shipdw.hvtb_parse_entity_hash_wearable) op where op.rn=1 """)
					
					val entityDF=spark.sql("""select seaware_to_vxp_success_flg,seaware_to_vxp_status,vxp_to_rrd_success_flg,vxp_to_rrd_status,vxp_wearable_color,vxp_rrd_file_received_flg,rrd_to_vxp_ingestion_success_flg,rrd_to_vxp_ingestion_status,uniquesourceid,entitycode from ( select * ,row_number() over (partition by uniquesourceid order by change_dt desc,addeddate desc) rn from shipdw.hvtb_parse_entity_hash_wearable) op where op.rn=1 """)
					entityDF.createOrReplaceTempView("entityhashtable") 
					
					
					//val rddDF=spark.sql("""select reservationId,case when status like '%Order Completed%' then true else false end as rrd_success_flg,status as rrd_processing_status from (select reservationId,status,row_number() over (partition by reservationId order by saildate desc,batchtime desc) rn from shipdw.hvtb_parse_rddfile_fromsftp) rddfile where rddfile.rn=1 """)
					
					val rddDF=spark.sql("""select reservationId,case when status='Order Completed' then true else false end as rrd_success_flg,status as rrd_processing_status from (select reservationId,status,row_number() over (partition by reservationId order by saildate desc,batchtime desc) rn from shipdw.hvtb_parse_rddfile_fromsftp) rddfile where rddfile.rn=1 """)
					rddDF.createOrReplaceTempView("rddfiletable")
					
          //val finalDF=spark.sql("""select tab1.res_id,tab1.SAIL_ID,tab1.guest_id,tab1.src_res_id,tab1.client_id,case when tab2.sw_generated_by_flg is null then false else tab2.sw_generated_by_flg end as sw_generated_by_flg,tab2.sw_wearable_type as sw_wearable_type,case when sw.flag is null then false when sw.flag='Y' then true when sw.flag='N' then false end as sw_listener_success_flg,case when err.sw_listener_error_flg is null then false else err.sw_listener_error_flg end as sw_listener_error_flg,err.sw_listener_error_details as sw_listener_error_details,case when entityhash.seaware_to_vxp_success_flg is null then false else entityhash.seaware_to_vxp_success_flg end as seaware_to_vxp_success_flg,entityhash.seaware_to_vxp_status,case when entityhash.vxp_to_rrd_success_flg is null then false else entityhash.vxp_to_rrd_success_flg end as vxp_to_rrd_success_flg,entityhash.vxp_to_rrd_status,entityhash.vxp_wearable_color,case when entityhash.vxp_rrd_file_received_flg is null then false else entityhash.vxp_rrd_file_received_flg end as vxp_rrd_file_received_flg,case when entityhash.rrd_to_vxp_ingestion_success_flg is null then false else entityhash.rrd_to_vxp_ingestion_success_flg end as rrd_to_vxp_ingestion_success_flg,entityhash.rrd_to_vxp_ingestion_status from restable tab1 left join wearabletable tab2 on tab1.src_res_id=tab2.res_id and tab1.client_id=tab2.client_id left join swtable sw on tab1.src_res_id=sw.src_res_id and tab1.client_id=sw.client_id left join errtable err on tab1.src_res_id=err.src_res_id and tab1.client_id=err.client_id left join entityhashtable entityhash on tab1.src_res_id=entityhash.uniquesourceid  and entityhash.entitycode='W' """)

					//val finalDF=spark.sql("""select tab1.res_id,tab1.SAIL_ID,tab1.guest_id,tab1.src_res_id,tab1.client_id,case when tab2.sw_generated_by_flg is null then false else tab2.sw_generated_by_flg end as sw_generated_by_flg,tab2.sw_wearable_type as sw_wearable_type,case when sw.flag is null then false when sw.flag='Y' then true when sw.flag='N' then false end as sw_listener_success_flg,case when err.sw_listener_error_flg is null then false else err.sw_listener_error_flg end as sw_listener_error_flg,err.sw_listener_error_details as sw_listener_error_details,case when entityhash.seaware_to_vxp_success_flg is null then false else entityhash.seaware_to_vxp_success_flg end as seaware_to_vxp_success_flg,entityhash.seaware_to_vxp_status,case when entityhash.vxp_to_rrd_success_flg is null then false else entityhash.vxp_to_rrd_success_flg end as vxp_to_rrd_success_flg,entityhash.vxp_to_rrd_status,entityhash.vxp_wearable_color,case when entityhash.vxp_rrd_file_received_flg is null then false else entityhash.vxp_rrd_file_received_flg end as vxp_rrd_file_received_flg,case when entityhash.rrd_to_vxp_ingestion_success_flg is null then false else entityhash.rrd_to_vxp_ingestion_success_flg end as rrd_to_vxp_ingestion_success_flg,entityhash.rrd_to_vxp_ingestion_status,case when rddTb.rrd_success_flg is null then false else rddTb.rrd_success_flg end as rrd_success_flg,rddTb.rrd_processing_status as rrd_processing_status from restable tab1 left join wearabletable tab2 on tab1.src_res_id=tab2.res_id and tab1.client_id=tab2.client_id left join swtable sw on tab1.src_res_id=sw.src_res_id and tab1.client_id=sw.client_id left join errtable err on tab1.src_res_id=err.src_res_id and tab1.client_id=err.client_id left join entityhashtable entityhash on tab1.src_res_id=entityhash.uniquesourceid  and entityhash.entitycode='W' left join rddfiletable rddTb on tab1.src_res_id=rddTb.reservationId""")
					val finalDF=spark.sql("""select tab1.res_id,tab1.SAIL_ID,tab1.guest_id,tab1.src_res_id,tab1.client_id,case when tab2.sw_generated_by_flg is null then false else tab2.sw_generated_by_flg end as sw_generated_by_flg,tab2.sw_wearable_type as sw_wearable_type,case when sw.flag is null then false when sw.flag='Y' then true when sw.flag='N' then false end as sw_listener_success_flg,case when err.sw_listener_error_flg is null then false else err.sw_listener_error_flg end as sw_listener_error_flg,err.sw_listener_error_details as sw_listener_error_details,case when entityhash.seaware_to_vxp_success_flg is null then false else entityhash.seaware_to_vxp_success_flg end as seaware_to_vxp_success_flg,entityhash.seaware_to_vxp_status,case when entityhash.vxp_to_rrd_success_flg is null then false else entityhash.vxp_to_rrd_success_flg end as vxp_to_rrd_success_flg,entityhash.vxp_to_rrd_status,entityhash.vxp_wearable_color,case when entityhash.vxp_rrd_file_received_flg is null then false else entityhash.vxp_rrd_file_received_flg end as vxp_rrd_file_received_flg,case when entityhash.rrd_to_vxp_ingestion_success_flg is null then false else entityhash.rrd_to_vxp_ingestion_success_flg end as rrd_to_vxp_ingestion_success_flg,entityhash.rrd_to_vxp_ingestion_status,case when rddTb.rrd_success_flg is null then false else rddTb.rrd_success_flg end as rrd_success_flg,rddTb.rrd_processing_status as rrd_processing_status from restable tab1 left join wearabletable tab2 on tab1.src_res_id=tab2.res_id left join swtable sw on tab1.src_res_id=sw.src_res_id and tab1.client_id=sw.client_id left join errtable err on tab1.src_res_id=err.src_res_id and tab1.client_id=err.client_id left join entityhashtable entityhash on tab1.src_res_id=entityhash.uniquesourceid  and entityhash.entitycode='W' left join rddfiletable rddTb on tab1.src_res_id=rddTb.reservationId""")
					
					
					
					if (!finalDF.head(1).isEmpty) {
						loadDimFact(spark: SparkSession, finalDF)
					}

					log.info("Updating Metadata framework")
					ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)


				} 

		catch {

		case e: SQLException =>
		{
			ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
			log.info("******************in the catch of Wearable Fact Load ******************");
			e.printStackTrace();
			throw new Exception("SQL Exception..please check the stacktrace", e);

		}

		println("#----------------------------Process Has Failed---------------------------#")
		System.exit(1)
		spark.stop()
		}	


	}// end of main

} // end of object
