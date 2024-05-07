package com.virginvoyages.shore.fact

import com.virginvoyages.metadataframework.ManageMetadata
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
import com.virginvoyages.scd.GetXrefData.srcReferenceTypeTotgtReferenceType

object BoardingFactLoad {

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

					val frameworkEnv = spark.sparkContext.getConf.get("spark.frameworkEnv").trim()

							if (frameworkEnv.trim().toUpperCase().equals("SHIP"))
							{

								println("################# On Ship Side ########################")
								val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date >='$part_read_start' and part_date <='$part_read_end'"""

								var sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim() + whereClause)
								if (!sourceDf.head(1).isEmpty) {
									log.info("Loading Boarding fact table")
									loadDimFact(spark: SparkSession, sourceDf)
								}

								log.info("Updating Metadata framework")
								ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

							}



					if (frameworkEnv.trim().toUpperCase().equals("SHORE"))
					{

						println("################ On Shore Side #################################")

//										   val whereClause = s""" where 1=1 """
						val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date >='$part_read_start' and part_date <='$part_read_end'"""

						println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
						log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

						import spark.sqlContext.implicits._
						println("#---------------------------Starting the Execution------------------#")

						val sourceDf = spark.sql(spark.sparkContext.getConf.get("""spark.source.sql""").trim() + whereClause)

						val srcDF=sourceDf.withColumn("sourceID",sourceDf.col("vxp_guest_id"))
						.withColumn("srcReferenceType",lit("VXP - Guest")).withColumn("tgtReferenceType",lit("Client"))

						println("############## src Df ####################")
//						srcDF.show(5,false)
//						println(srcDF.count)
//						srcDF.printSchema()

						val interimDF1 = srcReferenceTypeTotgtReferenceType(spark, srcDF)

						println("################### interim Df #############")


						 val interimDF=interimDF1.withColumn("seaware_client_id",interimDF1.col("targetID").cast(DoubleType))



						interimDF.createOrReplaceTempView("interimView")
						
//						val guestDFTemp=spark.sql("select guest_dim.guest_id,res_id,client_id from vv_db.hvtb_nbx_core_sw_res_guest_rel guest_rel left join vv_db.hvtb_nbx_core_sw_guest_dim guest_dim on (guest_rel.guest_id= guest_dim.guest_id) where rec_end_dttm='9999-12-31 00:00:00' ")
//						guestDFTemp.createOrReplaceTempView("guestView")
//						
//						val guestDf=spark.sql("select voyage_id,voyage_skey,person_skey,vxp_reservationguest_id,vxp_reservation_id,vxp_guest_id,boardingnumber_skey,guest_boarding_slot_number,src_last_modified_dt,embark_datetime,checkedin_boarding_number,onboard_boarding_number,seaware_sail_id,seaware_res_id,NVL(guestView.guest_id,-1) as seaware_guest_id,batchtime,part_date from  interimView v left join guestView on v.seaware_res_id=guestView.res_id and v.seaware_client_id=guestView.client_id")	
						
						

            val guestDFTemp=spark.sql("select guest_dim.guest_id,guest_dim.src_guest_id,res_id,client_id from vv_db.hvtb_nbx_core_sw_res_guest_rel guest_rel left join vv_db.hvtb_nbx_core_sw_guest_dim guest_dim on (guest_rel.guest_id= guest_dim.guest_id) where guest_rel.rec_end_dttm='9999-12-31 00:00:00' ")
						guestDFTemp.createOrReplaceTempView("guestView")
						
            val guestDf=spark.sql("select voyage_id,voyage_skey,person_skey,vxp_reservationguest_id,vxp_reservation_id,vxp_guest_id,boardingnumber_skey,guest_boarding_slot_number,src_last_modified_dt,embark_datetime,checkedin_boarding_number,onboard_boarding_number,seaware_sail_id,seaware_res_id,NVL(guestView.src_guest_id,-1) as seaware_guest_id,batchtime,part_date from  interimView v left join guestView on v.seaware_res_id=guestView.res_id and v.seaware_client_id=guestView.client_id")						
						

						
						println("################ after guest dim join ####################")




						if (!sourceDf.head(1).isEmpty) {
						  
							log.info("Loading Boarding fact table")
							loadDimFact(spark: SparkSession, guestDf)
						}

						log.info("Updating Metadata framework")
						ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

					}

				} 
		catch {

		case e: SQLException =>
		{
			ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
			log.info("******************in the catch of Boarding Fact Load ******************");
			e.printStackTrace();
			throw new Exception("SQL Exception..please check the stacktrace", e);

		}

		println("#----------------------------Process Has Failed---------------------------#")
		System.exit(1)
		spark.stop()
		}
	}
}