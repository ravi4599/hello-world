package com.virginvoyages.dimension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.parsing.json._
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import com.virginvoyages.scd.GetXrefData.srcReferenceTypeTotgtReferenceType
import com.virginvoyages.metadataframework.ManageMetadata

object SeawareGuestRTSDimLoad {
  
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
				/*************************************calling metadata framework*********************************************/

    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_time = metadata._3
    val batch_end_time = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6
    val start_execution_time = metadata._7
    val part_write_date = metadata._8

				try
		{

				
							val whereClause = s""" where batchtime>= '$batch_start_time' and batchtime<='$batch_end_time' and part_date>='$part_read_start' and part_date<='$part_read_end'"""



							println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
							log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

							import spark.sqlContext.implicits._
							var rtsDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim().replace("*whereclause*", whereClause))

							println("############## rts source query Df ####################")
							rtsDF.show(5,false)
							println(rtsDF.count)
							rtsDF.printSchema()

							var srcDF=rtsDF.withColumn("sourceID",rtsDF.col("reservationguestid"))
							.withColumn("srcReferenceType",lit("VXP - ReservationGuest")).withColumn("tgtReferenceType",lit("Seaware - Guest"))

							
							var firstXrefDF = srcReferenceTypeTotgtReferenceType(spark, srcDF)

							println("###################  after first xref call #############")
							
							firstXrefDF=firstXrefDF.withColumn("src_guest_id",firstXrefDF.col("targetID").cast(IntegerType))
							
							firstXrefDF=firstXrefDF.drop("sourceID","srcReferenceType","tgtReferenceType","targetID")


							var secondXrefDF=firstXrefDF.withColumn("sourceID",rtsDF.col("vxp_guest_id"))
							.withColumn("srcReferenceType",lit("VXP - Guest")).withColumn("tgtReferenceType",lit("Client"))

							secondXrefDF = srcReferenceTypeTotgtReferenceType(spark, secondXrefDF)

							println("###################  after second xref call #############")

							secondXrefDF=secondXrefDF.withColumn("client_id",secondXrefDF.col("targetID").cast(DoubleType))

							var finalDF=secondXrefDF.select("vxp_guest_id","vxp_reservation_id","voyagenumber","stateroom","src_res_id","src_guest_id","client_id","precruise_departure_airport_code","precruise_departure_city","precruise_departure_time","precruise_arrival_airport_code","precruise_arrival_city","precruise_flight_isDeleted","rts_transfer_response","arrival_transportation_mode","arrival_booking_source","arrival_airline_code","arrival_flight_number","arrival_transportation_date","arrival_transportation_time","departure_transportation_mode","departure_booking_source","postcruise_departure_airport_code","postcruise_departure_city","postcruise_arrival_airport_code","postcruise_arrival_city","postcruise_arrival_time","postcruise_flight_isdeleted","departure_airline_code","departure_flight_number","departure_transportation_date","departure_transportation_time","parking_at_port_flg","statuscode","paymentstatuscode","guestmoderationstatus","guestoveralldocumentstatus","isguestonboard","isvalidated","isassociatedflightbooking","isitineraryrulesapplied","security_photo_checkin_status","health_questions_checkin_status","emergency_contact_checkin_status","travel_documents_checkin_status","post_voyage_checkin_status","embarkation_slot_checkin_status","voyage_contract_checkin_status","payment_method_checkin_status","pregnency_checkin_status","security_photo_checkin_percentage","emergency_contact_checkin_percentage","travel_documents_checkin_percentage","embarkation_slot_checkin_percentage","payment_method_checkin_percentage","voyage_contract_checkin_percentage","checkin_status_total_percentage","reservation_last_modification_time","voyageid","batchtime","part_date")
							println("================ Final DF ================")
							finalDF.show(2,false)
							finalDF.printSchema()

							     if(!rtsDF.head(1).isEmpty){
							       loadDimFact(spark: SparkSession, finalDF)}
                        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
		}

		catch {

		case e: Exception =>
		{
             ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark)
			log.info("in the catch of updateStatus ******************")
			e.printStackTrace()
			throw new Exception("General Exception..please check the stacktrace")
		}
		println("#----------------------------Process Has Failed---------------------------#")
		System.exit(1)
		spark.stop()

		}





	}
  
}