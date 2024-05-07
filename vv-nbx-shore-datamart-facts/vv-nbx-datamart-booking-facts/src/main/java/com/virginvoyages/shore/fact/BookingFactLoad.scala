package com.virginvoyages.shore.fact
import java.util.Date

import com.virginvoyages.metadataframework.ManageMetadata
import java.net.UnknownHostException
//import scalaj.http.Http

import org.apache.spark.broadcast.Broadcast
import java.text.SimpleDateFormat

//import scalaj.http.HttpOptions
import scala.util.parsing.json._
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact


import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._
//import com.mart.dim.ChangeDataCapture.slowlyChangingDimension
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode

object BookingFactLoad {
  
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
    //spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
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
      
      //val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
      
      
      //val whereClause = s""" where 1=1 """
      val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      import spark.sqlContext.implicits._
      println("#---------------------------Starting the Execution------------------#")

      println("#------------------------------------XREF RETURNED-------------------------------_#")
      //xrefDF.show()
      val crew_sql = spark.sparkContext.getConf.get("spark.crewdf.sql")
      val crew_df =spark.sql(crew_sql)
      crew_df.createOrReplaceTempView("crew_vw")
      //
      val query = spark.sparkContext.getConf.get("spark.source.sql").replace("*whereclause*", whereClause)
      println(query)

      val stageBookingDF = spark.sql(query).as("bdf")
     
//      stageBookingDF.createOrReplaceTempView("booking_vw")
//      val persondfquery = spark.sparkContext.getConf.get("spark.persondf.sql")
//      val personDF = spark.sql(persondfquery)
//      personDF.createOrReplaceTempView("person_vw")
//      val personskeyBookingDFQuery = spark.sparkContext.getConf.get("spark.personskeybooking.sql")
//      val personskeyBookingDF = spark.sql(personskeyBookingDFQuery)
//      personskeyBookingDF.createOrReplaceTempView("personbooking_vw")
//      val bookedbyskeyBookingDFQuery = spark.sparkContext.getConf.get("spark.bookedbyskeybooking.sql")
//      val bookedbyskeyBookingDF = spark.sql(bookedbyskeyBookingDFQuery)
//      bookedbyskeyBookingDF.createOrReplaceTempView("bookedbybooking_vw")
//      val paidbyskeyBookingDFQuery = spark.sparkContext.getConf.get("spark.paidbyskeybooking.sql")
//      val paidbyskeyBookingDF = spark.sql(paidbyskeyBookingDFQuery)
//      paidbyskeyBookingDF.createOrReplaceTempView("paidbybooking_vw")
//      val cancelledbyskeyBookingDFQuery = spark.sparkContext.getConf.get("spark.cancelledbyskeybooking.sql")
//      val cancelledbyskeyBookingDF = spark.sql(cancelledbyskeyBookingDFQuery)
	     //stageBookingDF.dropDuplicates()
       stageBookingDF.persist()
	   
      //val stageBookingVxpGuestDF = stageBookingDF.join(xrefDF,bdf.bookedByPersonId===)
      // val stageBookingVxpGuestDF = stageBookingDF.join(xrefDF, col("PersonId") === col("src_id")
      //, "left").selectExpr("activity_skey", "activity_slot_skey", "booking_detail_skey",  "activityBookingId", "personid", "tgt_id as person_vxp","activityslotcode","activityCode", "bookedByPersonId", "payeePersonId", "bookingAmountPaid", "bookingAmountPending", "refundAmountPaid", "refundAmountPending", "voyage_id",  "number_of_people")
      //.join(xrefDF, col("bookedByPersonId") === col("src_id"),"left").selectExpr("activity_skey", "activity_slot_skey", "booking_detail_skey",  "activityBookingId", "personid", "person_vxp","activityslotcode","activityCode", "bookedByPersonId", "tgt_id as booked_by_person_vxp","payeePersonId", "bookingAmountPaid", "bookingAmountPending", "refundAmountPaid", "refundAmountPending", "voyage_id",  "number_of_people")
      //.join(xrefDF, col("payeePersonId") === col("src_id"),"left").selectExpr("activity_skey", "activity_slot_skey", "booking_detail_skey",  "activityBookingId", "personid", "person_vxp","activityslotcode","activityCode", "bookedByPersonId", "booked_by_person_vxp","payeePersonId","tgt_id as paid_by_person_vxp", "bookingAmountPaid", "bookingAmountPending", "refundAmountPaid", "refundAmountPending", "voyage_id",  "number_of_people")

      log.info("#----------------------------------stageBookingVxpGuestSkeyDF-----------------------------------#")
      

      println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage Booking Fact------------------------------------xxxxxxxxxxxxxxxxxxxxxx")

	  //cancelledbyskeyBookingDF.persist()
      
      //cancelledbyskeyBookingDF.show(5,false)

      //val stageBookingVxpGuestDF = stageBookingDF.join(xrefDF,bdf.bookedByPersonId===)
      // val stageBookingVxpGuestDF = stageBookingDF.join(xrefDF, col("PersonId") === col("src_id")
      //, "left").selectExpr("activity_skey", "activity_slot_skey", "booking_detail_skey",  "activityBookingId", "personid", "tgt_id as person_vxp","activityslotcode","activityCode", "bookedByPersonId", "payeePersonId", "bookingAmountPaid", "bookingAmountPending", "refundAmountPaid", "refundAmountPending", "voyage_id",  "number_of_people")
      //.join(xrefDF, col("bookedByPersonId") === col("src_id"),"left").selectExpr("activity_skey", "activity_slot_skey", "booking_detail_skey",  "activityBookingId", "personid", "person_vxp","activityslotcode","activityCode", "bookedByPersonId", "tgt_id as booked_by_person_vxp","payeePersonId", "bookingAmountPaid", "bookingAmountPending", "refundAmountPaid", "refundAmountPending", "voyage_id",  "number_of_people")
      //.join(xrefDF, col("payeePersonId") === col("src_id"),"left").selectExpr("activity_skey", "activity_slot_skey", "booking_detail_skey",  "activityBookingId", "personid", "person_vxp","activityslotcode","activityCode", "bookedByPersonId", "booked_by_person_vxp","payeePersonId","tgt_id as paid_by_person_vxp", "bookingAmountPaid", "bookingAmountPending", "refundAmountPaid", "refundAmountPending", "voyage_id",  "number_of_people")

      log.info("#----------------------------------stageBookingVxpGuestSkeyDF-----------------------------------#")
      //stageBookingVxpGuestSkeyDF.show(12,false)

      //println( cancelledbyskeyBookingDF.count()+"xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage Booking Fact------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
     
      if (!stageBookingDF.take(1).isEmpty) {

        loadDimFact(spark: SparkSession, stageBookingDF)
      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);

    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        log.info("******************in the catch of Booking Fact Factn Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e);
      }

      case e: Exception =>
        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          log.info("******************in the catch of booking fact Factn Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e);
        }
        println("#----------------------------Process Has Failed---------------------------#")
        log.info("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)

    }

  }
}
